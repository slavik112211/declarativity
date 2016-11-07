require 'rubygems'
require 'bud'
# require 'debugger'
require './pregel/membership.rb'
#require './lib/delivery/reliable'
#require './lib/delivery/multicast'

# 1. maintains a list of workers
# 2. pre-computation - pushes workers to read their chunk of graph.
# 3. starts computation - and starts the superstep loop.
#      1. waits until EACH worker signals the completion of the stage
#      2. each worker only signals to master when it's processing is finished,
#         and all messages sent by worker are delivered (with acknowledgements of receipt)
# 4. runs for 30 iterations (or until convergence), where vertex values do not change anymore
#    in consequent iterations.
#
# 1. command to read in the graph - divides the graph vertices by the amount of workers
#    and assigns the chunks of graph per each workers.
# master -> every worker (readin file "graph.txt", totalWorkers)
class PregelMaster
  include Bud
  include MembershipMaster

  def initialize(opts={})
    @request_count = -1
    super opts
  end

  state do
    channel :control_pipe, [:@address, :from, :message]
    interface input, :console_input, [:command, :params]
    lbool :graph_loaded
  end

  bloom :messaging do
    console_input <= stdio { |input|
      if ["load","start"].any? { |command| input.line.include? command }
        command = input.line.split(' ')
        command = [command[0], {:filename=>command[1]}] if command[0] == "load"
      end
    }

    #send commands to all workers
    control_pipe <~ (workers_list * console_input).combos do |worker, command|
      message = ControlMessage.new(ip_port, worker.worker_addr,
        @request_count+=1, 'request', command.command, command.params)
      [worker.worker_addr, ip_port, message]
    end

    # update workers list on job-completion messages
    workers_list <+- (workers_list * control_pipe)
      .pairs(workers_list.worker_addr => control_pipe.from) do |worker, command|
        # if control_pipe record wouldn't be joined with 'workers_list' collection,
        # a relevant worker could be retrieved like this:
        # worker = workers_list.instance_variable_get(:@storage)[[command.message.from]]
        [worker.worker_addr, worker.id, true] if command.message.params[:status]=="success"
    end
  end

  bloom :pregel_processing do
    graph_loaded <= workers_list
      .group([], bool_and(:graph_loaded)) {|columns| columns.first }

    stdio <~ stdio { |input|
      if ["start"].any? { |command| input.line.include? command } and !graph_loaded.reveal
        [["Cannot start processing - graph not loaded."]]
      end
    }
  end

  bloom :debug_master do
    stdio <~ [["loaded: "+graph_loaded.reveal.to_s]]
    stdio <~ console_input { |command| [command.to_s] }
    stdio <~ control_pipe  { |command| [command.message.inspect] }
  end
end

#Control message payload:
#  'id': to match requests and responses
#  'type': either 'request' from master to worker, or 'response'
#  'command': either 'load' or 'start'...
#  'params': additional info (like filename in 'load', or 'error/success' in response)
class ControlMessage
  attr_accessor :from, :to, :id, :type, :command, :params
  def initialize(from, to, id=nil, type=nil, command=nil, params=nil)
    @from    = from
    @to      = to
    @id      = id
    @type    = type
    @command = command
    @params  = params
  end
end