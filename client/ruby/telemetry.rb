require 'rubygems'
require 'socket'
require 'json'

class Telemetry
  class Exception < StandardError
    attr_reader :cause
    def initialize(cause)
      if cause
        @cause = cause
        super("#{cause.class.name}: #{cause.message}")
      end
    end
  end

  class NetworkException  < Exception; end;
  class EncodingException < Exception; end;

  def initialize(host = "localhost", port = 1845)
    @socket = TCPSocket.new(host, port)
    @socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_KEEPALIVE, true)
  rescue ::Exception => e
    raise NetworkException.new(e)
  end

  def close
    @socket.close
  end

  def log(label, data = {}, &block)
    data = block.call if block
    begin
      json = data.to_json
      json.unpack("U*").pack("U*")
    rescue ::Exception => e
      raise EncodingException.new(e)
    end

    begin
      @socket.puts("#{label.to_s} #{json}")
    rescue ::Exception => e
      raise NetworkException.new(e)
    end
  end

  class Null
    def log(*args)
      yield
    end

    def close
      # do nothing
    end
  end
end
