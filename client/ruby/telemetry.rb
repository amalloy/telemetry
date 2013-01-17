require 'rubygems'
require 'socket'
require 'json'

class Telemetry
  def initialize(port, host = "localhost")
    @socket = TCPSocket.new(host, port)
  end

  def log(label, data={}, &block)
    data = block.call if block
    @socket.puts("#{label.to_s} #{data.to_json}")
  end
end
