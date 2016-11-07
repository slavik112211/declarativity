require 'rubygems'
require 'bud'

# MembershipMaster accepts connect requests from Workers,
# updates a list of workers, and shares the updated list with all Workers.
# The updated list is sent back to workers only when it is changed.
module Membership
  include Bud

  state do
    channel :connect, [:@address, :worker_addr] => [:id]
    table :workers_list, [:worker_addr] => [:id, :graph_loaded, :superstep_completed]
    lmax  :workers_count # lattice - monotonically increasing sequence: 0,1,2,3 ...
  end

  bloom :lattices do
    workers_count <= workers_list.group([], count()) {|columns| columns.first }
  end

  bloom :debug_membership do
    stdio <~ workers_list.inspected
    # stdio <~ workers_list.group([], count())
    stdio <~ [["workers_count: "+workers_count.reveal.to_s]]
  end
end

module MembershipMaster
  include Membership

  DEFAULT_ADDRESS = "127.0.0.1:1234"

  def initialize(opts={})
    @workers_count = -1
    super opts
  end

  bloom :workers_connect do
    workers_list <= connect{|request| [request.worker_addr, @workers_count+=1, false, false]}

    #only send updated workers_list to all workers if it was updated
    #in current Bloom timetick - when "connect" channel has incoming connection requests
    connect <~ (workers_list * workers_list).combos do |l1, l2|
      [l1.worker_addr, l2.worker_addr, l2.id] unless connect.empty?
    end
  end

end

module MembershipWorker
  include Membership

  DEFAULT_ADDRESS = "127.0.0.1:1235"

  bootstrap do
    connect <~ [[@server, ip_port]]
  end

  bloom :connect_response do
    workers_list <= connect{ |worker|
      @worker_id = worker.id if worker.worker_addr == ip_port
      [worker.worker_addr, worker.id, false]
    }
  end
end