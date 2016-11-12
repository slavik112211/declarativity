# ENV["BUD_DEBUG"]="1"

require "./pregel/worker.rb"
require "./pregel/membership.rb"
require "./pregel/page_rank_vertex_processor.rb"

master_addr = (ARGV.length > 0) ? ARGV[0] : MembershipMaster::DEFAULT_ADDRESS
worker_addr = (ARGV.length > 1) ? ARGV[1] : MembershipWorker::DEFAULT_ADDRESS
worker_ip, worker_port = worker_addr.split(":")
pregel_vertex_processor = PageRankVertexProcessor.new

program = PregelWorker.new(master_addr, pregel_vertex_processor, :ip => worker_ip, :port => worker_port)
program.run_fg

#ruby pregel/start_worker.rb 127.0.0.1:1234 127.0.0.1:1236
# :stdin => $stdin,