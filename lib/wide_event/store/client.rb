require "net/http"
require "openssl"
require "uri"
require "json"

module WideEvent
  module Store
    # Minimal standard-library HTTPS client for POST /v1/events
    # (store/internal/httpapi/server.go). One instance issues one request at
    # a time; callers (the Sender's worker thread) serialize their own use.
    # Never touches application threads: instantiated and called only from
    # WideEvent::Store::Sender's worker thread.
    class Client
      # Tags a failure as retryable (timeout/network, 429, 507, 5xx) or
      # permanent (other 4xx). Never carries the ingest token or an event
      # body in its message.
      class Error < StandardError
        def initialize(message, retryable:)
          super(message)
          @retryable = retryable
        end

        def retryable?
          @retryable
        end
      end

      Ack = Struct.new(:protocol_version, :batch_id, :duplicate, :warnings, keyword_init: true)

      OPEN_TIMEOUT = 1
      READ_TIMEOUT = 2
      MAX_RESPONSE_BYTES = 64 * 1024
      LOOPBACK_HOSTS = %w[localhost 127.0.0.1 ::1].freeze
      INGEST_PATH = "/v1/events"

      NETWORK_ERRORS = [
        Net::OpenTimeout, Net::ReadTimeout, Errno::ECONNREFUSED, Errno::ECONNRESET,
        Errno::EHOSTUNREACH, Errno::ETIMEDOUT, Errno::EPIPE, SocketError,
        OpenSSL::SSL::SSLError, EOFError, IOError
      ].freeze

      def initialize(url:, ingest_token:, open_timeout: OPEN_TIMEOUT, read_timeout: READ_TIMEOUT)
        @uri = URI.parse(url.to_s)
        raise ArgumentError, "store url must be an absolute http(s) URL" unless @uri.host

        unless https?(@uri) || loopback?(@uri)
          raise ArgumentError, "store url must use HTTPS (plain http is only allowed for loopback testing)"
        end

        @ingest_token = ingest_token.to_s
        raise ArgumentError, "store ingest token is required" if @ingest_token.empty?

        @open_timeout = open_timeout
        @read_timeout = read_timeout
      end

      # Sends one already-gzipped canonical batch body. Returns an Ack on a
      # 200 response; raises Error (tagged retryable?) otherwise.
      def ingest(gzip_body)
        code, body = perform(gzip_body)
        handle_response(code, body)
      end

      private

      # Ruby's Net::HTTP unconditionally finishes reading the response body
      # itself (via HTTPResponse#reading_body's trailing `self.body` call)
      # once the block passed to #request returns *normally* — a plain
      # `break` inside #read_body's block only stops OUR loop; Net::HTTP
      # then tries to read the REST of the body anyway, which would hang
      # forever against a server that never stops sending. Raising instead
      # unwinds past that trailing read entirely; we catch it below.
      BoundedBodyExceeded = Class.new(StandardError) do
        attr_reader :buffer

        def initialize(buffer)
          @buffer = buffer
          super("response body exceeded #{MAX_RESPONSE_BYTES} bytes")
        end
      end

      def perform(gzip_body)
        request = Net::HTTP::Post.new(INGEST_PATH)
        request["Authorization"] = "Bearer #{@ingest_token}"
        request["Content-Type"] = "application/json"
        request["Content-Encoding"] = "gzip"
        request.body = gzip_body

        http = Net::HTTP.new(@uri.host, @uri.port)
        http.use_ssl = https?(@uri)
        http.open_timeout = @open_timeout
        http.read_timeout = @read_timeout

        code = nil
        body = nil
        begin
          http.start do |connection|
            connection.request(request) do |response|
              code = response.code.to_i
              body = read_bounded(response)
            end
          end
        rescue BoundedBodyExceeded => e
          body = e.buffer
        end
        [ code, body ]
      rescue *NETWORK_ERRORS => e
        raise Error.new("store request failed (#{e.class})", retryable: true)
      end

      def read_bounded(response)
        buffer = "".b
        response.read_body do |chunk|
          remaining = MAX_RESPONSE_BYTES - buffer.bytesize
          buffer << (chunk.bytesize > remaining ? chunk.byteslice(0, remaining) : chunk)
          raise BoundedBodyExceeded, buffer if buffer.bytesize >= MAX_RESPONSE_BYTES
        end
        buffer
      end

      def handle_response(code, body)
        case code
        when 200 then parse_ack(body)
        when 429, 507, 500..599 then raise Error.new("store responded with #{code}", retryable: true)
        else raise Error.new("store responded with #{code}", retryable: false)
        end
      end

      def parse_ack(body)
        json = JSON.parse(body)
        Ack.new(
          protocol_version: json["protocol_version"],
          batch_id: json["batch_id"],
          duplicate: json["duplicate"] == true,
          warnings: Array(json["warnings"])
        )
      rescue JSON::ParserError, TypeError
        raise Error.new("store returned an unparsable acknowledgement", retryable: true)
      end

      def https?(uri)
        uri.scheme == "https"
      end

      def loopback?(uri)
        uri.scheme == "http" && LOOPBACK_HOSTS.include?(uri.host)
      end
    end
  end
end
