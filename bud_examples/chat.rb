require 'rubygems'
require 'backports'
require 'bud'
require_relative 'chat_protocol'

class ChatClient
  include Bud
  include ChatProtocol

  def initialize(nick, server, opts={})
    @nick = nick
    @server = server
    super opts
  end

  bootstrap do
    connect <~ [[@server, ip_port, @nick]]
  end

  bloom do
    mcast <~ stdio do |s|
      [@server, [ip_port, @nick, Time.new.strftime("%I:%M.%S"), s.line]]
    end
  end
end


server = (ARGV.length > 1) ? ARGV[1] : ChatProtocol::DEFAULT_ADDR
puts "Server address: #{server}"

client_addr = (ARGV.length > 2) ? ARGV[2] : ChatProtocol::DEFAULT_ADDR
client_ip, client_port = client_addr.split(":")

program = ChatClient.new(ARGV[0], server, :stdin => $stdin, :ip => client_ip, :port => client_port.to_i)
program.run_fg

# ruby chat.rb your_nick 129.97.84.118:1234 129.97.84.54:1234