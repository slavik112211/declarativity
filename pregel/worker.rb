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
# 3. signals to master when the worker is done processing it's chunk, 
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

  state do
    channel :control_pipe, [:@address, :from, :message]
    interface output, :control_pipe_output, [:message]
    interface input, :start_processing_command, [:message]
    # TODO: 'vertices' table should ALSO include vertices that don't point to any vertices (dead-end vertices)
    # messages_inbox = [[:vertex_from, :value], [:vertex_from, :value]]
    table :vertices, [:id] => [:value, :total_adjacent_vertices, :vertices_to, :messages_inbox]

    periodic :timestep, 0.1  #Process a Bloom timestep every 5 seconds

    table :queue_in_next, [:vertex_id, :vertex_from] => [:message_value]
    table :queue_out, [:adjacent_vertex_worker_id, :vertex_from, :vertex_to] => [:message]
    # table :supersteps, [:id] => [:completed]
    lmax :next_superstep_messages_count
  end

  bloom :commands_processing do
    # command_input <= control_pipe { |command| command  }

    #sending response to server is delayed by one timestep,
    #to allow for the requested action to be performed (in current timestep) before sending a response
    control_pipe_output <+ control_pipe do |payload|
      if ["load", "start"].include? payload.message.command
        MessagesHandler.process_input_message(payload.message)
      end
    end
    control_pipe <~ control_pipe_output { |record| [record.message.to, ip_port, record.message] }
  end

  bloom :load_graph do
    vertices <= control_pipe.flat_map do |payload|
      if(payload.message.command == "load")
        @graph_loader.file_name = payload.message.params[:filename]
        @graph_loader.worker_id = @worker_id
        @graph_loader.total_workers = workers_count.reveal
        @graph_loader.load_graph
        @total_vertices = @graph_loader.vertices_all.size

        # STUB DATA populating the messages_inbox for first and second vertices for superstep 0
        @graph_loader.vertices[0] << [[1, 0.1], [3, 0.7]] if @graph_loader.vertices[0][0] == 2
        @graph_loader.vertices[0] << [[2, 0.5], [3, 0.3]] if @graph_loader.vertices[0][0] == 1
        @graph_loader.vertices
      end
    end
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

    start_processing_command <+ control_pipe do |payload|
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
    queue_out <+ start_processing_command.flat_map do |start_command|
      messages = []
      vertices.each {|vertex|
        unless(vertex.messages_inbox.nil? and vertex.messages_inbox.empty?)
          new_vertex_value=0
          vertex.messages_inbox.each {|message|
            new_vertex_value+=message[1]
          }
          vertex.value = 0.15/@total_vertices + 0.85*new_vertex_value
        end

        vertex.vertices_to.each { |adjacent_vertex|
          adjacent_vertex_worker_id = @graph_loader.graph_partition_for_vertex(adjacent_vertex)
          messages << [adjacent_vertex_worker_id, vertex.id, adjacent_vertex, vertex.value.to_f / vertex.total_adjacent_vertices]
        }
      }
      messages
    end

    # delivery of the vertex messages to adjacent vertices for the next Pregel superstep
    # table :queue_out, [:adjacent_vertex_worker_id, :vertex_id, :vertex_from] => [:message]
    control_pipe <~ (queue_out * workers_list)
      .pairs(:adjacent_vertex_worker_id => :id) do |vertex_message, worker|
        # if(!@superstep_vertex_messages_sent)
        #   @superstep_vertex_messages_sent=true
          message = Message.new(ip_port, worker.worker_addr, nil, 
            "request", "vertex_message", vertex_message.to_a)
          [worker.worker_addr, ip_port, message]
        # end
    end

    # remove all outgoing vertex messages from "queue_out" in next timestep
    # They are sent to recipients in the current timestep.
    # This ensures that the rule control_pipe <~ (queue_out * workers_list) won't 
    # send more message duplicates in the next Bloom timestep
    queue_out <- queue_out

    # accumulate vertex messages for the next Pregel superstep
    queue_in_next <= control_pipe { |vertex_message|
      if(vertex_message.message.command == "vertex_message")
        [vertex_message.message.params[2], vertex_message.message.params[1], vertex_message.message.params[3]]
      end
    }
    next_superstep_messages_count <= queue_in_next.group([], count()) {|columns| columns.first }
  end

  bloom :debug_worker do
    # stdio <~ control_pipe { |command| [command.to_s] }
    stdio <~ queue_in_next  { |vertex_message| [vertex_message.inspect] }
    stdio <~ vertices  { |vertex| [vertex.inspect] }
    # stdio <~ queue_out { |vertex_queue| [vertex_queue.inspect] }
    stdio <~ [["next_superstep_messages_count: "+next_superstep_messages_count.reveal.to_s]]
  end
end

class MessagesHandler
  def self.process_input_message message
    if(message.command == "load")
      response = Message.new(message.to, message.from, message.id, "response",message.command)
      response.params = (File.exist? message.params[:filename]) ? {status: "success"} : {status: "failure: no such file"}
      return [response]
    #TODO: ensure that "success" message is only sent back to Master when all the vertex messages were delivered
    elsif(message.command == "start") #stub to return successful completion of superstep by Worker
      response = Message.new(message.to, message.from, message.id, "response",message.command)
      response.params = {status: "success", superstep: message.params[:superstep]}
      return [response]
    end
  end
end