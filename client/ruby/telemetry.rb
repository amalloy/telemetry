require 'rubygems'
require 'socket'
require 'json'

class Telemetry
  class Exception < StandardError
    attr_reader :cause
    def initialize(cause)
      @cause = cause
    end
  end

  class NetworkException  < Exception; end;
  class EncodingException < Exception; end;

  def initialize(host = "localhost", port = 1845)
    @socket = TCPSocket.new(host, port)
    @socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_KEEPALIVE, true)
  rescue Errno::ECONNREFUSED => e
    raise NetworkException.new(e)
  end

  def close
    @socket.close
  end

  def log(label, data={}, &block)
    data = block.call if block
    json = data.to_json
    json.unpack("U*").pack("U*")
    @socket.puts("#{label.to_s} #{json}")
  rescue Errno::EPIPE => e
    raise NetworkException.new(e)
  rescue ArgumentError => e
    raise EncodingException.new(e)
  end
end
