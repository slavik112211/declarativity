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
    interface input, :control_pipe_input, [:message]
    # 'vertices' table should ALSO include vertices that don't point to any vertices (dead-end vertices)
    table :vertices, [:id] => [:value, :total_adjacent_vertices, :vertices_to]

    periodic :timestep, 5  #Process a Bloom timestep every 5 seconds

    # table :vertex_iteration, [:iteration, :vertex_id] => [:messages_sent, :ack_received]
    table :queue_in,  [:vertex_id] => [:messages] #[[:vertex_from, :value], [:vertex_from, :value]]
    table :queue_in_buffer, [:vertex_id, :vertex_from] => [:message]

    table :queue_out_temp, [:messages]
    table :queue_out, [:adjacent_vertex_worker_id, :vertex_from, :vertex_to] => [:message]
    # table :supersteps, [:id] => [:completed]
  end

  bootstrap do
    queue_in <= [ [1, [[2, 0.5], [3, 0.3]]  ]  , [2, [[1, 0.1], [3, 0.7]] ] ]
  end

  bloom :commands_processing do
    # command_input <= control_pipe { |command| command  }

    #sending response to server is delayed by one timestep,
    #to allow for the requested action to be performed (in current timestep) before sending a response
    control_pipe_output <+ control_pipe do |payload|
      MessagesHandler.process_input_message(payload.message)
    end
    control_pipe <~ control_pipe_output { |record| [record.message.to, ip_port, record.message] }

    control_pipe_input <+ control_pipe do |payload|
      [payload.message] if payload.message.command=="start"
    end
  end

  bloom :load_graph do
    vertices <= control_pipe.flat_map do |payload|
      if(payload.message.command == "load")
        @graph_loader.file_name = payload.message.params[:filename]
        @graph_loader.worker_id = @worker_id
        @graph_loader.total_workers = workers_count.reveal
        # graph_loader = DistributedGraphLoader.new(
        #   payload.message.params[:filename], @worker_id, workers_count.reveal)
        @graph_loader.load_graph
        @total_vertices = @graph_loader.vertices_all.size
        @graph_loader.vertices
      end
    end
  end

  bloom :pregel_processing do
    # table :queue_out, [:adjacent_vertex_worker_id, :vertex_from, :vertex_to] => [:message]
    # This rule should also apply when queue_in has no messages.

    queue_out_temp <+ (vertices * queue_in * control_pipe_input)    # don't put <+ here. the puts "whateva" won't work
      .combos() do |vertex, queue_in, command|
        puts "whateva #{vertex.inspect}"
        puts "whateva #{queue_in.inspect}"
        puts "whateva #{command.inspect}"
        puts "ze end."
        #vertices.id => queue_in.vertex_id
        unless(control_pipe_input.empty?)
          # debugger
          messages = []
          unless(queue_in.messages.nil?)  # no incoming messages
            new_vertex_value=0
            queue_in.messages.each {|message|
              new_vertex_value+=message[1]
            }
            vertex.value = 0.15/@total_vertices + 0.85*new_vertex_value
          end

          vertex.vertices_to.each { |adjacent_vertex|
            adjacent_vertex_worker_id = @graph_loader.graph_partition_for_vertex(adjacent_vertex)
            messages << [adjacent_vertex_worker_id, vertex.id, adjacent_vertex, vertex.value.to_f / vertex.total_adjacent_vertices]
          }
          [messages]
        end
      end

    # table :queue_out_temp,  [:vertex_id] => [:messages]
    # table :queue_out, [:adjacent_vertex_worker_id, :vertex_from, :vertex_to] => [:message]
    # queue_out <+ queue_out_temp.flat_map {|message|
    #   # debugger
    #   message[0]
    # }



    # delivery of the vertex messages to adjacent vertices for the next Pregel superstep
    # table :queue_out, [:adjacent_vertex_worker_id, :vertex_id, :vertex_from] => [:message]
    # control_pipe <~ (queue_out * workers_list)
    #   .pairs(:adjacent_vertex_worker_id => :id) do |vertex_message, worker|
    #     message = Message.new(ip_port, worker.worker_addr, nil, 
    #       "request", "vertex_message", vertex_message)
    #     [worker.worker_addr, ip_port, message]
    # end
  end

  # accumulate messages from nodes into queue_in buffer
  bloom :queue_in_buffer_accumulate do
    queue_in_buffer <= control_pipe { |vertex_message|
      if(vertex_message.message.type == "vertex_message")
        [vertex_message.message.params[1], vertex_message.message.params[2], vertex_message.message]
      end
    }
  end

  bloom :debug_worker do
    stdio <~ control_pipe { |command| [command.to_s] }
    stdio <~ queue_in  { |vertex_queue| [vertex_queue.inspect] }
    stdio <~ queue_out_temp { |vertex_queue| [vertex_queue.inspect] }
  end
end

class MessagesHandler
  def self.process_input_message message
    if(message.command == "load")
      response = Message.new(message.to, message.from, message.id, "response",message.command)
      response.params = (File.exist? message.params[:filename]) ? {status: "success"} : {status: "failure: no such file"}
      return [response]
    elsif(message.command == "start") #stub to return successful completion of superstep by Worker
      # response = Message.new(message.to, message.from, message.id, "response",message.command)
      # response.params = {status: "success", superstep: message.params[:superstep]}
      # sleep 10
      # return [response]
    end
  end
end