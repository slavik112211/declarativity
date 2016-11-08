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
    super opts
  end

  state do
    channel :control_pipe, [:@address, :from, :message]
    interface output, :control_pipe_output, [:message]
    interface input, :control_pipe_input, [:message]
    table :vertices, [:id] => [:value, :total_adjacent_vertices, :vertices_to]

    periodic :timestep, 5  #Process a Bloom timestep every 5 seconds

    # table :vertex_iteration, [:iteration, :vertex_id] => [:messages_sent, :ack_received]
    table :queue_in,  [:vertex_id] => [:messages] #[[:vertex_from, :value], [:vertex_from, :value]]
    table :queue_out, [:vertex_id, :vertex_from] => [:message]
  end

  bootstrap do
    queue_in <= [ [1, [[2, 0.5], [3, 0.3]]  ]  , [2, [[1, 0.1], [3, 0.7]]] ]
  end

  bloom :commands_processing do
    # command_input <= control_pipe { |command| command  }

    #sending response to server is delayed by one timestep,
    #to allow for the requested action to be performed (in current timestep) before sending a response
    control_pipe_output <+ control_pipe do |payload|
      ControlMessagesHandler.process_input_message(payload.message)
    end
    control_pipe <~ control_pipe_output { |record| [record.message.to, ip_port, record.message] }

    control_pipe_input <= control_pipe do |payload|
      [payload.message] if payload.message.command=="start"
    end
  end

  bloom :load_graph do
    vertices <= control_pipe.flat_map do |payload|
      if(payload.message.command == "load")
        graph_loader = DistributedGraphLoader.new(
          payload.message.params[:filename], @worker_id, workers_count.reveal)
        graph_loader.load_graph
        graph_loader.vertices
      end
    end
  end

  bloom :pregel_processing do
    # [:vertex_id, :vertex_from] => [:message]
    queue_out <= (vertices * queue_in * control_pipe_input)
      .pairs(vertices.id => queue_in.vertex_id) do |vertex, queue_in, command|
        #  queue_in[0].value+queue_in[1].value
        messages = []
        new_vertex_value = 0
        queue_in.messages.each {|message|
          new_vertex_value+=message[1]
        }

        vertex.vertices_to.each { |adjacent_vertex|
          messages << [adjacent_vertex, vertex.id, new_vertex_value]
        }
        vertex.value = new_vertex_value
        messages
    end


    # vertex_iteration <+ control_pipe { |payload|
    #   [0, false, false] if(payload.message.command=="start" and vertex_iteration.empty?)
    # }
  end

  bloom :debug_worker do
    stdio <~ control_pipe { |command| [command.to_s] }
    stdio <~ queue_in { |vertex_queue| [vertex_queue.to_s] }
  end
end

class ControlMessagesHandler
  def self.process_input_message message
    if(message.command == "load")
      response = ControlMessage.new(message.to, message.from, message.id, "response",message.command)
      response.params = (File.exist? message.params[:filename]) ? {status: "success"} : {status: "failure: no such file"}
      return [response]
    elsif(message.command == "start") #stub to return successful completion of superstep by Worker
      response = ControlMessage.new(message.to, message.from, message.id, "response",message.command)
      response.params = {status: "success", superstep: message.params[:superstep]}
      sleep 10
      return [response]
    end
  end
end