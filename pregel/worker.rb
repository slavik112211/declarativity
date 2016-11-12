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
    @graph_loader = DistributedGraphLoader.new
    @pregel_vertex_processor = vertex_processor
    super opts
  end

  bootstrap do
    worker_events <= [ ["graph_loaded", false] ]
  end

  state do
    channel :control_pipe, [:@address, :from, :message]
    interface output, :worker_output, [:message]
    interface input, :worker_input, [:message]
    table :worker_events, [:name] => [:finished]

    # messages_inbox = [[:vertex_from, :value], [:vertex_from, :value]]
    table :vertices, [:id] => [:value, :total_adjacent_vertices, :vertices_to, :messages_inbox]

    periodic :timestep, 0.001  #Process a Bloom timestep every milliseconds

    # Each Pregel superstep "queue_in_next" holds upto N amount of network_messages, where N=1 per each Worker
    # Each network message may contain 1 or more vertex_messages (from that Worker)
    table :queue_in_next, [:messages_queue]
    table :queue_out, [:adjacent_vertex_worker_id, :vertex_from, :vertex_to] => [:message]
    # all vertex messages are packed into 1 network message per Worker:
    # [:worker_id] => [[:vertex_from, :vertex_to, :message], ...]
    table :queue_out_per_worker_temp, [:worker_id] => [:vertex_messages_queue]
    table :queue_out_per_worker, [:worker_id] => [:vertex_messages_queue, :sent]
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
        queue_in_next.each {|vertex_messages|
          vertex_messages.messages_queue.each {|message|
            if vertex.id == message[1]  # message[1] == :vertex_to
              #message[0] == :vertex_from; # message[2] == :value
              messages << [message[0], message[2]]
            end
          }
        }
        [vertex.id, vertex.value, vertex.total_adjacent_vertices, vertex.vertices_to, messages]
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
      if(worker_input_command.message.command=="start")
        vertex_messages = @pregel_vertex_processor.compute(vertex)
      end
    end

    # group all vertex_messages into a 1 network message per Worker
    # Didn't work: queue_out.group([:adjacent_vertex_worker_id], accum([:vertex_from, :vertex_to, :message]))
    # accum() aggregator only works for one column: accum(:vertex_to)
    # VertexMessages to worker_id=1: [1, [[2, 1, 0.16666666666666666], [2, 3, 0.16666666666666666]]]
    # 2 vertex messages: 1st - from vertex 2 to vertex 1, 2nd - from vertex 2 to vertex 3
    queue_out_per_worker_temp <= queue_out.reduce({}) do |accumulator, vertex_message|
      accumulator[vertex_message.adjacent_vertex_worker_id] ||= []
      accumulator[vertex_message.adjacent_vertex_worker_id] <<
        [vertex_message.vertex_from, vertex_message.vertex_to, vertex_message.message]
      accumulator
    end

    # set all outgoing vertex_messages_queues to :sent=false
    queue_out_per_worker <= queue_out_per_worker_temp do |vertex_messages|
      [vertex_messages[0], vertex_messages[1], false]
    end

    # delivery of the vertex messages to adjacent vertices for the next Pregel superstep
    # table :queue_out_per_worker, [:worker_id] => [[:vertex_from, :vertex_to, :message], ...]
    control_pipe <~ (queue_out_per_worker * workers_list)
      .pairs(:worker_id => :id) do |vertex_messages, worker|
        if(vertex_messages.sent == false)
          vertex_messages.sent = true
          message = Message.new(ip_port, worker.worker_addr, nil,
            "request", "vertex_messages", vertex_messages.vertex_messages_queue)
          [worker.worker_addr, ip_port, message]
        end
    end

    # remove all outgoing vertex messages from "queue_out" in next timestep
    # They are sent to recipients in the current timestep.
    # queue_out <- (queue_out * queue_out.group([], bool_and(:sent))).lefts
    queue_out <- queue_out
    queue_out_per_worker_temp <- queue_out_per_worker_temp
    queue_out_per_worker <- queue_out_per_worker

    # send back a confirmation to Master that the superstep is complete
    # This Worker->Master {superstep finished} message is sent after all vertex_messages were *sent*.
    # Thus, this doesn't ensure that all vertex_messages are *delivered* before the Master
    # instructs Workers to start the next superstep.
    # Nonetheless, somehow Bud framework sends this {superstep finished} message 
    # only after all vertex_messages are *delivered*, and calculates PageRank correctly.
    # TODO: use reliable delivery protocol with acknowledgement of receipt, before setting :sent to true
    control_pipe <~ queue_out_per_worker.group([], bool_and(:sent)) {|vertex_messages_sent|
      response_message = Message.new(ip_port, @server, nil, "response", "start", {status: "success"})
      [response_message.to, ip_port, response_message]
    }

    # Save vertex_messages_queue for the next Pregel superstep
    queue_in_next <= control_pipe { |network_message|
      if(network_message.message.command == "vertex_messages")
        [network_message.message.params]
      end
    }
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