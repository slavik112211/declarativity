#!/usr/bin/env ruby
require "socket"

# taken from https://www.sitepoint.com/ruby-tcp-chat/
# Author: Simon Benitez

class Client
  def initialize( server )
    @server = server
    @request = nil
    @response = nil
    listen
    send
    @request.join
    @response.join
  end

  def listen
    @response = Thread.new do
      loop {
        msg = @server.gets.chomp
        puts "#{msg}"
      }
    end
  end

  def send
    puts "Enter the username:"
    @request = Thread.new do
      loop {
        msg = $stdin.gets.chomp
        @server.puts( msg )
      }
    end
  end
end

server = TCPSocket.open( "192.168.0.15", 1234 )
Client.new( server )
