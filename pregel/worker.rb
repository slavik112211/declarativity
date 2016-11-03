require 'rubygems'
require 'bud'
require './pregel/master'
require './lib/delivery/reliable'

module PregelWorkerProtocol
  DEFAULT_ADDRESS = "127.0.0.1:1235"
end

# Workers:
# 1. maintains vertex' values
# 2. maintains 2 queues of messages per each vertex - one for this superstep, second for the other.
# 3. signals to master when the worker is done processing it's chunk, 
#    and all messages are sent (with acknowledgement of receipt)
# messages are sent with the superstep #, that they belong to. 
class PregelWorker
  include Bud
  include PregelWorkerProtocol
  include ConnectionProtocol

  def initialize(server, opts={})
    @server = server
    super opts
  end

  bootstrap do
    connect <~ [[@server, ip_port]]
  end

  state do
    table :workers_list, [:worker_addr] => [:id]
  end

  bloom :connect_response do
    workers_list <= connect{|worker| [worker.worker_addr, worker.id] }
  end

  bloom :debug do
    stdio <~ workers_list.inspected
    stdio <~ workers_list.group([], count())
  end
end
