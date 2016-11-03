require 'rubygems'
require 'bud'
require './lib/delivery/reliable'
require './lib/delivery/multicast'

module ConnectionProtocol
  state do
    channel :connect, [:@address, :worker_addr] => [:id]
  end
end

module PregelMasterProtocol
  DEFAULT_ADDRESS = "127.0.0.1:1234"

  state do
    interface input, :command_input, [:type] => [:time]
  end
end

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
  include PregelMasterProtocol
  include MulticastProtocol
  include ConnectionProtocol

  state do
    table :workers_list, [:key] => [:value]
  end

  bloom :workers_connect do
    workers_list <= connect{|c| [c.worker_addr, c.id]}
  end

  bloom :command_input do
    command_input <= stdio { |input| [input.line, Time.new] if ["init","load","start"].include? input.line }
  end

  bloom :debug do
    stdio <~ command_input { |command| [command.to_s] }
    stdio <~ workers_list  { |list| [list.to_s] }
  end
end
