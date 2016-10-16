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

    stdio <~ mcast { |m| [pretty_print(m.val)] }
  end
end


server = (ARGV.length > 1) ? ARGV[1] : ChatProtocol::DEFAULT_ADDR
puts "Server address: #{server}"

client_addr = (ARGV.length > 2) ? ARGV[2] : ChatProtocol::DEFAULT_ADDR
client_ip, client_port = client_addr.split(":")

program = ChatClient.new(ARGV[0], server, :stdin => $stdin, :ip => client_ip, :port => client_port.to_i)
program.run_fg
