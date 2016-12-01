# ENV["BUD_DEBUG"]="1"

require "./pregel/master.rb"
require "./pregel/membership.rb"

master_addr = (ARGV.length > 0) ? ARGV[0] : MembershipMaster::DEFAULT_ADDRESS
master_ip, master_port = master_addr.split(":")

program = PregelMaster.new(:stdin => $stdin, :ip => master_ip, :port => master_port)
program.run_fg
