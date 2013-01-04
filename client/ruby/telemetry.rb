require 'rubygems'
require 'socket'
require 'json'

class Telemetry
  def initialize(port, host = "localhost")
    @socket = TCPSocket.new(host, port)
  end

  def log(type, payload)
    message = "#{type} #{payload.to_json}"
    @socket.puts(message)
  end
end
