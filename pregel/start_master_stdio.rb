require "./pregel/master.rb"
require "./pregel/membership.rb"

DEFAULT_ADDRESS = "127.0.0.1:1334"

master_addr       = (ARGV.length > 0) ? ARGV[0] : MembershipMaster::DEFAULT_ADDRESS
master_stdio_addr = (ARGV.length > 1) ? ARGV[1] : DEFAULT_ADDRESS
master_stdio_ip, master_stdio_port = master_stdio_addr.split(":")

program = PregelMasterConsoleInput.new(
  master_addr, :stdin => $stdin, :ip => master_stdio_ip, :port => master_stdio_port)
program.run_fg

#ruby pregel/start_master_stdio.rb 127.0.0.1:1234 127.0.0.1:1334