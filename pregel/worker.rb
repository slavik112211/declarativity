require 'rubygems'
require 'bud'
require 'securerandom'
require './pregel/membership.rb'
require './pregel/master.rb'
require './pregel/graph/graph_loader.rb'
require './lib/delivery/reliable.rb'
require 'debugger'
# require './lib/delivery/reliable'


# SecureRandom.uuid() to generate unique message ids for reliable delivery
# Workers:
# 1. maintains vertex' values
# 2. maintains 2 queues of messages per each vertex - one for this superstep, second for the other.
# 3. signals to master when the worker is done processing its chunk,
#    and all messages are sent (with acknowledgement of receipt)
# messages are sent with the superstep #, that they belong to. 
class PregelWorker
  include Bud
  include MembershipWorker
  include ReliableDelivery

  def initialize(server, vertex_processor, opts={})
    @server = server
    @graph_loader = AdjacencyListGraphLoader.new
    @pregel_vertex_processor = vertex_processor
    @sent = 0
    @received = 0
    @acked = 0
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

    # TODO: 'vertices' table should ALSO include vertices that don't point to any vertices (dead-end vertices)
    # messages_inbox = [[:vertex_from, :value], [:vertex_from, :value]]
    # message type is a symbol with three values: [:regular, :master, :ghost]
    table :vertices, [:id] => [:type, :value, :total_adjacent_vertices, :vertices_to, :messages_inbox]

    periodic :timestep, 0.00001  #Process a Bloom timestep every milliseconds

    # we store each vertex message in a seperate entry, so there might be multiple messages per vertex from same worker
    table :queue_in_next, [:vertex_id, :vertex_from] => [:message_value]
    table :queue_in_next_lalp, [:vertex_id] => [:message_value]
    table :queue_out, [:adjacent_vertex_worker_id, :vertex_from, :vertex_to, :uuid] => [:message, :sent, :delivered]
    # special lalp messages which will be sent to each worker and replicated to vertex inboxes
    table :queue_out_lalp, [:vertex_from] => [:message, :sent_count, :sent, :delivered]
    # all vertex messages are packed into 1 network message per Worker:
    # [:worker_id] => [[:vertex_from, :vertex_to, :message], ...]
    # table :queue_out_per_worker_temp, [:worker_id] => [:vertex_messages_queue]
    # table :queue_out_per_worker, [:worker_id] => [:vertex_messages_queue, :sent]
    # table :supersteps, [:id] => [:completed]
    lmax :next_superstep_regular_messages_count
    lmax :next_superstep_lalp_messages_count
  end

  bloom :load_graph do
    vertices <+ control_pipe.flat_map do |payload|
      if(payload.message.command == "load")
        @graph_loader.file_name = payload.message.params[:filename]
        @graph_loader.worker_id = @worker_id
        @graph_loader.total_workers = workers_count.reveal
        @graph_loader.load_graph
        @pregel_vertex_processor.graph_loader = @graph_loader

        # STUB DATA populating the messages_inbox for first and second vertices for superstep 0
        # @graph_loader.vertices[0] << [[1, 0.1], [3, 0.7]] if @graph_loader.vertices[0][0] == 2
        # @graph_loader.vertices[0] << [[2, 0.5], [3, 0.3]] if @graph_loader.vertices[0][0] == 1
        @graph_loader.vertices
      end
    end

    control_pipe <~ (vertices.group([], count()) * worker_events).pairs {|vertices_count, event|
      if(vertices_count[0] > 0 and event.name == "graph_loaded" and event.finished == false)
        # print out number of vertices
        # puts "# of regular vertices : #{@graph_loader.regular_vertex_count}"
        # puts "# of master vertices  : #{@graph_loader.master_vertex_count}"
        event.finished = true
        response_message = Message.new(ip_port, @server, nil, "response", "load", {status: "success"})
        [response_message.to, ip_port, response_message]
      end
    }
  end

  # Each entry in queue_in_next is a seperate vertex message (lalp messages are already replicated when we initially receive network pack with lalp message)
  # if master send start, put that into worker_input in next timestep so that next superstep starts
  # Purge queue_in_next in next bloom timestep
  bloom :superstep_initialization do
    # table :queue_in_next, [:vertex_id, :vertex_from] => [:message_value]
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
    queue_in_next <- (queue_in_next * control_pipe).pairs do |vertex_message, payload|
      # puts "!!!!!removing message #{vertex_message.inspect}"
      vertex_message if payload.message.command=="start"
    end
  end

  bloom :pregel_processing do
    # regular vertex messages, one for each adjacent vertex
    # table :queue_out, [:adjacent_vertex_worker_id, :vertex_from, :vertex_to] => [:message]
    queue_out <+ (vertices * worker_input).pairs.flat_map do |vertex, worker_input_command|
      if(worker_input_command.message.command=="start" and vertex.type == :regular)
        # regular vertices send message for each adjacent edges as usual
        vertex_messages = @pregel_vertex_processor.compute(vertex)
        # iterate over values produced by vertex processor, add uuid to each one
        vertex_messages_uuid = []  
        vertex_messages.each {|vertex_message|
          vertex_messages_uuid << [vertex_message[0], vertex_message[1], vertex_message[2],
          SecureRandom.uuid, vertex_message[3], vertex_message[4], vertex_message[5]]
        }
        # add messages with uuid
        vertex_messages_uuid
      end
    end

    # special lalp messages are produced by only master vertices, which copies vertex value to every worker
    queue_out_lalp <+ (vertices * worker_input).pairs do |vertex, worker_input_command|
      if(worker_input_command.message.command=="start" and vertex.type == :master)
        # master vertices generate single message for each worker
        @pregel_vertex_processor.compute(vertex)
        source_vertex_id = vertex.id
        message_value = vertex.value / vertex.total_adjacent_vertices
        [source_vertex_id, message_value, 0, false, false]
      end
    end

    # delivery of the regular vertex messages to adjacent vertices for the next Pregel superstep
    # table :queue_out, [:adjacent_vertex_worker_id, :vertex_id, :vertex_from] => [:message, :sent, :delivered]
    pipe_in <+ (queue_out * workers_list)
      .pairs(:adjacent_vertex_worker_id => :id) do |vertex_message, worker|
        # puts "!!!! PRE queue_out => message_pipe #{vertex_message.inspect}"
        if(vertex_message.sent == false)
          vertex_message.sent = true
          # puts "!!!! POST queue_out => pipe_in #{vertex_message.inspect}"
          @sent += 1
          puts "Sent #{@sent}"
          message = Message.new(ip_port, worker.worker_addr, nil, 
            "request", "vertex_message", vertex_message.to_a)
          [worker.worker_addr, ip_port, vertex_message.uuid, message]
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
    queue_out_lalp <- queue_out_lalp

    queue_out <- (queue_out * worker_output).pairs do |vertex_message, worker_output_message|
      # check whether messages is acked message
      if vertex_message.delivered and worker_output_message.message.command == "acked"
        # puts "Remove vertex #{vertex_message.inspect}"
        vertex_message
      end
    end

    # send back a confirmation to Master that the superstep is complete
    # This Worker->Master {superstep finished} message is sent after all vertex_messages were *delivered*.
    # This is ensured by employing ReliableDelivery protocol for these network messages,
    # meaning that .delivered flag is only set when the acknowledgement network message is received for each network_message sent.
    control_pipe <~ queue_out.group([], bool_and(:delivered)) {|vertex_messages_sent|
      if vertex_messages_sent[0] == true #when all .delivered == true
        puts "ALL MESSAGES DELIVERED #{vertex_messages_sent}"
        response_message = Message.new(ip_port, @server, nil, "response", "start", {status: "success"})
        [response_message.to, ip_port, response_message]
      end
    }

    worker_output <= queue_out.group([], bool_and(:delivered)) {|vertex_messages_sent|
      if vertex_messages_sent[0] == true #when all .delivered == true
        # now write ack message on output so that queues can be cleared
        # puts "worker_output <= ACKED"
        acked_message = Message.new(ip_port, @server, nil, "response", "acked", {})
        [acked_message]
      end
    }

    # now control the delivery messages on pipe_sent then delete mark on control pipe accordingly
    queue_out <+- (queue_out * pipe_sent).pairs(:uuid => :ident) {|original_message, delivered_message|
      if original_message.delivered == false
        # puts "ACKED original #{original_message.inspect}"
        # puts "ACKED ack_message #{delivered_message.inspect}"
        @acked += 1
        puts "Acked #{@acked}"
        original_message.delivered = true
        original_message
      end
    }

    # Save vertex_messages_queue for the next Pregel superstep
    queue_in_next <= pipe_out { |network_message|
      # puts "!!!!!! pipe_out =>queue_in_next id=#{network_message.payload.params[2]}, #{network_message.payload.params[1]}"
      @received += 1
      puts "Received #{@received}"
      [network_message.payload.params[2], network_message.payload.params[1], network_message.payload.params[4]]
    }
    # replicate lalp messages for each adjacent (this rule is fired at receiver side, so that single message per vertex is transmitted on wire)
    queue_in_next <= (lalp_pipe * vertices).pairs.flat_map { |network_message, vertex|
      lalp_message_vertex = network_message.message.params[0]
      if(lalp_message_vertex == vertex.id)
        messages = []
        # replication of lalp message to each neighbour at local partition
        vertex.vertices_to.each {|neighbour|
          messages << [neighbour, lalp_message_vertex, network_message.message.params[1]]
        }
        messages
      end
    }

    # TODO: How to add queue_out_lalp message count???
    # next_superstep_regular_messages_count <= queue_out.group([], count()) {|columns| columns.first }
    # next_superstep_lalp_messages_count <= queue_out_lalp.group([], count()) {|columns| columns.first }
  end

  bloom :debug_worker do
    stdio <~ control_pipe { |network_message| [network_message.to_s] if network_message.message.command == "start" }
    # stdio <~ queue_in_next  { |vertex_message| [vertex_message.inspect] }
    # stdio <~ vertices  { |vertex| [vertex.inspect] }
    # stdio <~ pipe_sent { |ack| [ack.inspect] }
    # stdio <~ queue_out { |vertex_queue| [ "QUEUE_OUT" + vertex_queue.inspect] }
    # stdio <~ queue_out_per_worker { |vertex_messages_per_worker| [vertex_messages_per_worker.inspect] }
    # stdio <~ [["next_superstep_regular_messages_count: "+next_superstep_regular_messages_count.reveal.to_s]]
    # stdio <~ worker_events { |event| [event.inspect] }

    # stdio <~ queue_out.group([:delivered], count()) do |grouped_count|
    #   # This grouping can contain 2 groups - number of delivered messages: [true, 3], and
    #   # number of not delivered messages: [false, 1]
    #   # This block is executed twice - once for each case.
    #   ["VERTEX_MESSAGES_DELIVERED #{grouped_count.inspect}"]
    # end


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