require 'rubygems'
require 'bud'
require './pregel/membership.rb'
require './pregel/master.rb'
require './pregel/graph/graph_loader.rb'
require 'debugger'
# require './lib/delivery/reliable'

# Workers:
# 1. maintains vertex' values
# 2. maintains 2 queues of messages per each vertex - one for this superstep, second for the other.
# 3. signals to master when the worker is done processing its chunk,
#    and all messages are sent (with acknowledgement of receipt)
# messages are sent with the superstep #, that they belong to. 
class PregelWorker
  include Bud
  include MembershipWorker

  def initialize(server, vertex_processor, opts={})
    @server = server
    @graph_loader = AdjacencyListGraphLoader.new
    @pregel_vertex_processor = vertex_processor
    super opts
  end

  bootstrap do
    worker_events <= [ ["graph_loaded", false] ]
  end

  state do
    channel :control_pipe, [:@address, :from, :message]
    channel :message_pipe, [:@address, :from, :message]
    channel :lalp_pipe, [:@address, :from, :message]
    interface output, :worker_output, [:message]
    interface input, :worker_input, [:message]
    table :worker_events, [:name] => [:finished]

    # messages_inbox = [[:vertex_from, :value], [:vertex_from, :value]]
    # message type is a symbol with three values: [:regular, :master, :ghost]
    table :vertices, [:id] => [:type, :value, :total_adjacent_vertices, :vertices_to, :messages_inbox]

    periodic :timestep, 1  #Process a Bloom timestep every milliseconds

    # we store each vertex message in a seperate entry, so there might be multiple messages per vertex from same worker
    table :queue_in_next, [:vertex_id, :vertex_from] => [:message_value]
    table :queue_in_next_lalp, [:vertex_id] => [:message_value]
    table :queue_out, [:adjacent_vertex_worker_id, :vertex_from, :vertex_to] => [:message, :sent, :delivered]
    # special lalp messages which will be sent to each worker and replicated to vertex inboxes
    table :queue_out_lalp, [:vertex_from] => [:message, :sent_count, :sent, :delivered]
    # all vertex messages are packed into 1 network message per Worker:
    # [:worker_id] => [[:vertex_from, :vertex_to, :message], ...]
    # table :queue_out_per_worker_temp, [:worker_id] => [:vertex_messages_queue]
    # table :queue_out_per_worker, [:worker_id] => [:vertex_messages_queue, :sent]
    # table :supersteps, [:id] => [:completed]
    lmax :next_superstep_messages_count
  end

  bloom :load_graph do
    vertices <+ control_pipe.flat_map do |payload|
      if(payload.message.command == "load")
        @graph_loader.file_name = payload.message.params[:filename]
        @graph_loader.worker_id = @worker_id
        @graph_loader.total_workers = workers_count.reveal
        @graph_loader.load_graph
        @pregel_vertex_processor.graph_loader = @graph_loader
        @graph_loader.vertices
      end
    end

    control_pipe <~ (vertices.group([], count()) * worker_events).pairs {|vertices_count, event|
      if(vertices_count[0] > 0 and event.name == "graph_loaded" and event.finished == false)
        event.finished = true
        response_message = Message.new(ip_port, @server, nil, "response", "load", {status: "success"})
        [response_message.to, ip_port, response_message]
      end
    }
  end

  # "queue_in_next" may contain not 1 queue of incoming messages, but 1 queue per each Worker
  # Each worker sends 1 big network_message containing all vertex_messages,
  # but there can be multiple workers.
  # Either aggregate all imcoming vertex_message_queues into 1 queue,
  # or do an outer loop over all queues in "queue_in_next" for each vertex
  bloom :superstep_initialization do
    # table :queue_in_next, [ [:vertex_id, :vertex_from] => [:message_value], ... ]
    vertices <+- (vertices * control_pipe).pairs do |vertex, payload|
      if payload.message.command=="start"
        messages = []
        queue_in_next.each {|message|
          if vertex.id == message.vertex_id
            messages << [message[1], message[2]]
          end
        }
        [vertex.id, vertex.type, vertex.value, vertex.total_adjacent_vertices, vertex.vertices_to, messages]
      end
    end

    worker_input <+ control_pipe do |payload|
      [payload.message] if payload.message.command=="start"
    end

    # Purge the "queue_in_next" in the next Bloom timestep,
    # as we have populated "vertices" message_inbox in the current Bloom timestep
    queue_in_next <- (queue_in_next * control_pipe).pairs do |vertex_messages_queue, payload|
      vertex_messages_queue if payload.message.command=="start"
    end
  end

  bloom :pregel_processing do
    # table :queue_out, [:adjacent_vertex_worker_id, :vertex_from, :vertex_to] => [:message]
    queue_out <+ (vertices * worker_input).pairs.flat_map do |vertex, worker_input_command|
      if(worker_input_command.message.command=="start" and vertex.type == :regular)
        # regular vertices send message for each adjacent edges as usual
        vertex_messages = @pregel_vertex_processor.compute(vertex)  
      end
    end

    queue_out_lalp <+ (vertices * worker_input).pairs do |vertex, worker_input_command|
      if(worker_input_command.message.command=="start" and vertex.type == :master)
        # master vertices generate single message for each worker
        @pregel_vertex_processor.compute(vertex)
        source_vertex_id = vertex.id
        message_value = vertex.value / vertex.total_adjacent_vertices
        [source_vertex_id, message_value, 0, false, false]
      end
    end

    # delivery of the vertex messages to adjacent vertices for the next Pregel superstep
    # table :queue_out, [:adjacent_vertex_worker_id, :vertex_id, :vertex_from] => [:message, :sent, :delivered]
    message_pipe <~ (queue_out * workers_list)
      .pairs(:adjacent_vertex_worker_id => :id) do |vertex_message, worker|
        if(vertex_message.sent == false)
          vertex_message.sent = true
          message = Message.new(ip_port, worker.worker_addr, nil, 
            "request", "vertex_message", vertex_message.to_a)
          [worker.worker_addr, ip_port, message]
        end
    end

    # delivery of lalp messages to all workers for next Pregel superstep
    lalp_pipe <~ (queue_out_lalp * workers_list).pairs do |vertex_message, worker|
      # puts "Vertex ID" + vertex_message.vertex_from.to_s
      # puts "Worker" + worker.id.to_s
        if(vertex_message.sent == false)
          # increase sent count, mark sent flag if all workers have message
          vertex_message.sent = true if vertex_message.sent_count == workers_count.reveal
          vertex_message.sent_count += 1
          message = Message.new(ip_port, worker.worker_addr, nil, 
            "request", "lalp_message", vertex_message.to_a)
          # puts "Actaul Message: " + message.inspect
          [worker.worker_addr, ip_port, message]
        end
    end

    # remove all outgoing vertex messages from "queue_out" in next timestep
    # They are sent to recipients in the current timestep.
    # queue_out <- (queue_out * queue_out.group([], bool_and(:sent))).lefts
    queue_out <- queue_out
    queue_out_lalp <-queue_out_lalp

    # send back a confirmation to Master that the superstep is complete
    # This Worker->Master {superstep finished} message is sent after all vertex_messages were *sent*.
    # Thus, this doesn't ensure that all vertex_messages are *delivered* before the Master
    # commands to start the next superstep.
    # Nonetheless, Bud framework sends this message only after all vertex_messages are received,
    # and calculates PageRank correctly.
    control_pipe <~ queue_out.group([], bool_and(:sent)) {|vertex_messages_sent|
      response_message = Message.new(ip_port, @server, nil, "response", "start", {status: "success"})
      [response_message.to, ip_port, response_message]
    }

    # Save vertex_messages_queue for the next Pregel superstep
    queue_in_next <= message_pipe { |network_message|
      [network_message.message.params[2], network_message.message.params[1], network_message.message.params[3]]
    }
    # replicate lalp messages for each adjacent 
    queue_in_next <= (lalp_pipe * vertices).pairs.flat_map { |network_message, vertex|
      lalp_message_vertex = network_message.message.params[0]
      if(lalp_message_vertex == vertex.id)
        messages = []
        vertex.vertices_to.each {|neighbour|
          messages << [neighbour, lalp_message_vertex, network_message.message.params[1]]
        }
        messages
      end
    }

    # TODO: How to add queue_out_lalp message count???
    next_superstep_messages_count <= queue_out.group([], count()) {|columns| columns.first }
  end

  bloom :debug_worker do
    stdio <~ control_pipe { |network_message| [network_message.to_s] if network_message.message.command == "start" }
    # stdio <~ queue_in_next  { |vertex_message| [vertex_message.inspect] }
    stdio <~ vertices  { |vertex| [vertex.inspect] }
    # stdio <~ queue_out { |vertex_queue| [vertex_queue.inspect] }
    # stdio <~ queue_out_per_worker { |vertex_messages_per_worker| [vertex_messages_per_worker.inspect] }
    # stdio <~ [["next_superstep_messages_count: "+next_superstep_messages_count.reveal.to_s]]
    # stdio <~ worker_events { |event| [event.inspect] }
  end
end

# bloom :commands_processing do
#   command_input <= control_pipe { |command| command  }

#   sending response to server is delayed by one timestep,
#   to allow for the requested action to be performed (in current timestep) before sending a response
#   worker_output <+ control_pipe do |payload|
#     if ["start"].include? payload.message.command  #["load", "start"]
#       MessagesHandler.process_input_message(payload.message)
#     end
#   end
#   control_pipe <~ worker_output { |record| [record.message.to, ip_port, record.message] }
# end

# class MessagesHandler
#   def self.process_input_message message
#     if(message.command == "load")
#       response = Message.new(message.to, message.from, message.id, "response",message.command)
#       response.params = (File.exist? message.params[:filename]) ? {status: "success"} : {status: "failure: no such file"}
#       return [response]
#     elsif(message.command == "start") #stub to return successful completion of superstep by Worker
#       response = Message.new(message.to, message.from, message.id, "response",message.command)
#       response.params = {status: "success", superstep: message.params[:superstep]}
#       return [response]
#     end
#   end
# end