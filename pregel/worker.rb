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
    table :vertices, [:vertex_id] => [:value, :total_adjacent_vertices, :vertices_to]
    periodic :timestep, 5  #Process a Bloom timestep every 5 seconds
  end

  bloom :commands_processing do
    # command_input <= control_pipe { |command| command  }

    #sending response to server is delayed by one timestep,
    #to allow for the requested action to be performed (in current timestep) before sending a response
    control_pipe_output <+ control_pipe do |payload|
      ControlMessagesHandler.process_input_message(payload.message)
    end
    control_pipe <~ control_pipe_output { |record| [record.message.to, ip_port, record.message] }
  end

  bloom :load_graph do
    vertices <= control_pipe.flat_map do |payload|
      if(payload.message.command="load")
        graph_loader = DistributedGraphLoader.new(
          payload.message.params[:filename], @worker_id, workers_count.reveal)
        graph_loader.load_graph
        graph_loader.vertices
      end
    end
  end

  bloom :debug_worker do
    stdio <~ control_pipe { |command| [command.to_s] }
  end
end

class ControlMessagesHandler
  def self.process_input_message message
    if(message.command == "load")
      response = ControlMessage.new(message.to, message.from, message.id, "response",message.command)
      response.params = (File.exist? message.params[:filename]) ? {status: "success"} : {status: "failure: no such file"}
      return [response]
    end
  end
end