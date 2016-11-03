ENV["BUD_DEBUG"]="1"

require "./pregel/master.rb"
require "./pregel/worker.rb"

worker_id   = (ARGV.length > 0) ? ARGV[0] : "worker1"
master_addr = (ARGV.length > 1) ? ARGV[1] : PregelMasterProtocol::DEFAULT_ADDRESS
worker_addr = (ARGV.length > 2) ? ARGV[2] : PregelWorkerProtocol::DEFAULT_ADDRESS 
worker_ip, worker_port = worker_addr.split(":")

program = PregelWorker.new(worker_id, master_addr, :ip => worker_ip, :port => worker_port)
program.run_fg
