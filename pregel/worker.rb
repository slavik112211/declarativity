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

  def initialize(worker_id, server, opts={})
    @worker_id = worker_id
    @server = server
    super opts
  end

  bootstrap do
    connect <~ [[@server, ip_port, @worker_id]]
    # workers_list <+ [[ip_port, @nick]]
  end

  state do
    table :workers_list, [:key] => [:value]
  end
end
