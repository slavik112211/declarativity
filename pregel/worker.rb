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

  def initialize(server, opts={})
    @server = server
    @graph_loader = DistributedGraphLoader.new
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

    # TODO: 'vertices' table should ALSO include vertices that don't point to any vertices (dead-end vertices)
    # messages_inbox = [[:vertex_from, :value], [:vertex_from, :value]]
    table :vertices, [:id] => [:value, :total_adjacent_vertices, :vertices_to, :messages_inbox]

    periodic :timestep, 0.001  #Process a Bloom timestep every milliseconds

    table :queue_in_next, [:vertex_id, :vertex_from] => [:message_value]
    table :queue_out, [:adjacent_vertex_worker_id, :vertex_from, :vertex_to] => [:message, :sent, :delivered]
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
        @total_vertices = @graph_loader.vertices_all.size

        # STUB DATA populating the messages_inbox for first and second vertices for superstep 0
        # @graph_loader.vertices[0] << [[1, 0.1], [3, 0.7]] if @graph_loader.vertices[0][0] == 2
        # @graph_loader.vertices[0] << [[2, 0.5], [3, 0.3]] if @graph_loader.vertices[0][0] == 1
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
        [vertex.id, vertex.value, vertex.total_adjacent_vertices, vertex.vertices_to, messages]
      end
    end

    worker_input <+ control_pipe do |payload|
      [payload.message] if payload.message.command=="start"
    end

    # Purge the "queue_in_next" in the next Bloom timestep,
    # as we have populated "vertices" message_inbox in the current Bloom timestep
    queue_in_next <- (queue_in_next * control_pipe).pairs do |vertex_message, payload|
      vertex_message if payload.message.command=="start"
    end
  end

  bloom :pregel_processing do
    # table :queue_out, [:adjacent_vertex_worker_id, :vertex_from, :vertex_to] => [:message]
    # This rule should also apply when queue_in has no messages
    queue_out <+ worker_input.flat_map do |start_command|
      messages = []
      vertices.each {|vertex|
        if(!vertex.messages_inbox.nil? and !vertex.messages_inbox.empty?)
          new_vertex_value=0
          vertex.messages_inbox.each {|message|
            new_vertex_value+=message[1]
          }
          vertex.value = 0.15/@total_vertices + 0.85*new_vertex_value
          # not considering the random jump factor, for simplicity when testing
          # vertex.value = new_vertex_value
        end

        vertex.vertices_to.each { |adjacent_vertex|
          adjacent_vertex_worker_id = @graph_loader.graph_partition_for_vertex(adjacent_vertex)
          messages << [adjacent_vertex_worker_id, vertex.id, adjacent_vertex, 
            vertex.value.to_f / vertex.total_adjacent_vertices, false, false]
        }
      }
      messages
    end

    # delivery of the vertex messages to adjacent vertices for the next Pregel superstep
    # table :queue_out, [:adjacent_vertex_worker_id, :vertex_id, :vertex_from] => [:message, :sent, :delivered]
    control_pipe <~ (queue_out * workers_list)
      .pairs(:adjacent_vertex_worker_id => :id) do |vertex_message, worker|
        if(vertex_message.sent == false)
          vertex_message.sent = true
          message = Message.new(ip_port, worker.worker_addr, nil, 
            "request", "vertex_message", vertex_message.to_a)
          [worker.worker_addr, ip_port, message]
        end
    end

    # remove all outgoing vertex messages from "queue_out" in next timestep
    # They are sent to recipients in the current timestep.
    queue_out <- (queue_out * queue_out.group([], bool_and(:sent))).lefts

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

    # accumulate vertex messages for the next Pregel superstep
    queue_in_next <= control_pipe { |vertex_message|
      if(vertex_message.message.command == "vertex_message")
        [vertex_message.message.params[2], vertex_message.message.params[1], vertex_message.message.params[3]]
      end
    }
    next_superstep_messages_count <= queue_in_next.group([], count()) {|columns| columns.first }
  end

  bloom :debug_worker do
    # stdio <~ control_pipe { |network_message| [network_message.to_s] if network_message.message.command == "start" }
    # stdio <~ queue_in_next  { |vertex_message| [vertex_message.inspect] }
    stdio <~ vertices  { |vertex| [vertex.inspect] }
    # stdio <~ queue_out { |vertex_queue| [vertex_queue.inspect] }
    stdio <~ [["next_superstep_messages_count: "+next_superstep_messages_count.reveal.to_s]]
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