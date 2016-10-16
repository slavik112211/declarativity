require 'rubygems'
require 'backports'
require 'bud'
require_relative 'chat_protocol'

class ChatServer
  include Bud
  include ChatProtocol

  state { 
  	table :nodelist
  	periodic :interval, 3
  }

  bloom do
    nodelist <= connect { |c| [c.client, c.nick] }
    mcast <~ (mcast * nodelist).pairs { |m,n| [n.key, m.val] }
  end

  bloom :server_echo do
  	stdio <~ (interval * nodelist).rights{|n| [n] }
  	stdio <~ mcast { |m| [pretty_print(m.val)] }
  end
end

# ruby command-line wrangling
addr = ARGV.first ? ARGV.first : ChatProtocol::DEFAULT_ADDR
ip, port = addr.split(":")
puts "Server address: #{ip}:#{port}"
program = ChatServer.new(:ip => ip, :port => port.to_i)
program.run_fg

#ruby chat_server.rb 129.97.84.118:1234