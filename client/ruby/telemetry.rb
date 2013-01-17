require 'rubygems'
require 'socket'
require 'json'

class Telemetry
  class NetworkException < StandardError
    attr_reader :cause
    def initialize(cause)
      @cause = cause
    end
  end

  def initialize(port, host = "localhost")
    @socket = TCPSocket.new(host, port)
  end

  def log(label, data={}, &block)
    data = block.call if block
    @socket.puts("#{label.to_s} #{data.to_json}")
  rescue Errno::EPIPE => e
    raise NetworkException.new(e)
  end
end
