require './chat.rb'

# A script to start the chat program supplied in bud-sandbox/chat
# on multiple machines.

# 1. Start an initial node:
# ruby single.rb peter 129.97.84.54:1234 129.97.84.54:1234

# 2. Start some other nodes:
# ruby single.rb joe 129.97.84.54:1234 129.97.84.118:1234

# First IP:port signifies one of the machines already in the chat
# Second IP:port signifies current node address.

if(ARGV.length != 3) {
	puts "Provide all required params"
	return
}

username = ARGV[0]
server_node = ARGV[1]
client_ip, client_port = ARGV[2].split(":")

program = SingleChat.new(username, server_node, :stdin => $stdin,
	:ip => client_ip, :port => client_port)
program.run_fg
