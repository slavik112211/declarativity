require 'rubygems'
require 'bud'
# require 'debugger'
require './pregel/membership.rb'
#require './lib/delivery/reliable'
#require './lib/delivery/multicast'

# 1. maintains a list of workers
# 2. pre-computation - pushes workers to read their chunk of graph.
#    Command to read in the graph - divides the graph vertices by the amount of workers
#    and assigns the chunks of graph per each workers.
# 3. starts computation - and starts the superstep loop.
#      1. waits until EACH worker signals the completion of the stage
#      2. each worker only signals to master when its processing is finished,
#         and all messages sent by worker are delivered (with acknowledgements of receipt)
# 4. runs for 30 iterations (or until convergence), where vertex values do not change anymore
#    in consequent iterations.
class PregelMaster
  include Bud
  include MembershipMaster
  MAX_SUPERSTEPS = 25

  def initialize(opts={})
    @request_count = -1
    super opts
  end

  state do
    channel :control_pipe, [:@address, :from, :message]
    interface input, :multicast, [:command, :params]
    lbool :graph_loaded
    lbool :computation_completed

    table :supersteps, [:id] => [:request_sent, :completed]
    lmax :supersteps_count
    lmax :supersteps_completed_count
    interface input, :start_superstep, [:iteration]
    periodic :timestep, 1 #Process a Bloom timestep every 1 second
  end

  bloom :messaging do
    multicast <= stdio { |input|
      if ["load","start"].any? { |command| input.line.include? command }
        command = input.line.split(' ')
        if(command[0] == "load")
          command = [command[0], {:filename=>command[1]}]
        # elsif(command[0] == "start")
        end 
      end
    }

    #send commands to all workers
    control_pipe <~ (workers_list * multicast).combos do |worker, command|
      message = Message.new(ip_port, worker.worker_addr,
        @request_count+=1, 'request', command.command, command.params)
      [worker.worker_addr, ip_port, message]
    end

    # update workers list on job-completion messages
    workers_list <+- (workers_list * control_pipe)
      .pairs(workers_list.worker_addr => control_pipe.from) do |worker, command|
        # if control_pipe record wouldn't be joined with 'workers_list' collection,
        # a relevant worker could be retrieved like this:
        # worker = workers_list.instance_variable_get(:@storage)[[command.message.from]]
        if(command.message.command == "load" and command.message.params[:status]=="success")
          [worker.worker_addr, worker.id, true, worker.superstep_completed]
        elsif(command.message.command == "start" and command.message.params[:status]=="success")
          #worker completed the current superstep
          [worker.worker_addr, worker.id, worker.graph_loaded, true]
        end
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

    #start iterating
    start_superstep <= stdio { |input|
      [0] if(input.line=="start" and graph_loaded.reveal and supersteps.empty?)
    }

    start_superstep <+ workers_list.group([], bool_and(:superstep_completed)) {|columns|
      if columns.first == true and supersteps_count.reveal < MAX_SUPERSTEPS
        # workers_list.each{|worker| worker.superstep_completed=false } # puts columns
         #Insert the next superstep_id as current 'supersteps_count'
         #That's because superstep_id start from 0, and 'supersteps_count' is always ahead +1
        [supersteps_count.reveal]
      end
    }

    supersteps <+- (supersteps.argmax([], :id) *
      workers_list.group([], bool_and(:superstep_completed)) ).combos {|latest_superstep, columns|
      if columns.first == true
        [latest_superstep.id, latest_superstep.request_sent, true] #completed=true
      end
    }

    workers_list <+- (workers_list * workers_list.group([], bool_and(:superstep_completed)))
      .combos {|worker, columns|
      if columns.first == true
        [worker.worker_addr, worker.id, worker.graph_loaded, false]
      end
    }

    #iterating
    supersteps <= start_superstep {|start_superstep_command|
      [start_superstep_command.iteration, false, false]
    }

    # for the latest superstep tuple in "supersteps", send a request to Workers
    # to start the superstep, if the request wasn't sent yet
    multicast <= supersteps.argmax([], :id) {|superstep|
      if(!superstep.request_sent and !superstep.completed)
        superstep.request_sent=true
        ["start", {:superstep=>superstep.id}]
      end
    }

    supersteps_count <= supersteps.group([], count()) {|columns| columns.first }
    supersteps_completed_count <= supersteps.group([:completed], count()) do |grouped_count|
      # This grouping can contain 2 groups - number of completed supersteps: [true, 3], and
      # number of not completed supersteps: [false, 1]
      # This block is executed twice - once for each case.
      # We're ignoring the [false, 1] and only process the [true, 3] case
      grouped_count[1] if(grouped_count[0]==true)
    end
    computation_completed <= supersteps_completed_count.gt_eq(MAX_SUPERSTEPS)
  end

  bloom :debug_master do
    stdio <~ [["loaded: "+graph_loaded.reveal.to_s]]
    # stdio <~ multicast { |command| [command.to_s] }
    stdio <~ [["supersteps_count: "+supersteps_count.reveal.to_s]]
    stdio <~ [["supersteps_completed_count: "+supersteps_completed_count.reveal.to_s]]
    stdio <~ [["Computation completed: "+computation_completed.reveal.to_s]]
    # stdio <~ start_superstep { |command| [command.to_s] }
    stdio <~ supersteps { |superstep| [superstep.to_s] }
    # stdio <~ control_pipe  { |command| [command.message.inspect] }
  end
end

#Control message payload:
#  'id': to match requests and responses
#  'type': either 'request' from master to worker, or 'response'
#  'command': either 'load' or 'start'...
#  'params': additional info (like filename in 'load', or 'error/success' in response)
class Message
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