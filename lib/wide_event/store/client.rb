require "net/http"
require "openssl"
require "uri"
require "json"

module WideEvent
  module Store
    # Minimal standard-library HTTPS client for POST /v1/events
    # (store/internal/httpapi/server.go) and POST /v1/query
    # (store/internal/httpapi/query.go). One instance issues one request at
    # a time. #ingest is called only from WideEvent::Store::Sender's worker
    # thread; #query is called from the wide_events:sql Rails task on
    # whatever thread invokes it (there is no persistent worker for
    # queries).
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
      QUERY_PATH = "/v1/query"
      QUERY_PROTOCOL_VERSION = 1

      # The store caps its own query responses at 10 MiB
      # (store/internal/query/executor.go's defaultMaxResponseBytes); we read
      # one byte past that so a response that actually reaches the cap+1
      # byte is distinguishable from one that legitimately ends at (or
      # under) 10 MiB, instead of silently parsing a truncated body.
      QUERY_RESPONSE_CAP = 10 * 1024 * 1024
      DEFAULT_QUERY_DEADLINE = 30
      QUERY_FORMATS = %w[json csv].freeze
      RETRY_AFTER_DEFAULT = 1
      RETRY_AFTER_CAP = 10

      NETWORK_ERRORS = [
        Net::OpenTimeout, Net::ReadTimeout, Errno::ECONNREFUSED, Errno::ECONNRESET,
        Errno::EHOSTUNREACH, Errno::ETIMEDOUT, Errno::EPIPE, SocketError,
        OpenSSL::SSL::SSLError, EOFError, IOError
      ].freeze

      # Token presence is validated per-operation (in #ingest and #query),
      # not here: a Client built only to query has no ingest token to give
      # it, and vice versa. The constructor still validates the URL, since
      # that's shared by both operations and needed before either can run.
      def initialize(url:, ingest_token: nil, query_token: nil, open_timeout: OPEN_TIMEOUT, read_timeout: READ_TIMEOUT)
        @uri = URI.parse(url.to_s)
        raise ArgumentError, "store url must be an absolute http(s) URL" unless @uri.host

        unless https?(@uri) || loopback?(@uri)
          raise ArgumentError, "store url must use HTTPS (plain http is only allowed for loopback testing)"
        end

        @ingest_token = ingest_token.to_s
        @query_token = query_token.to_s
        @open_timeout = open_timeout
        @read_timeout = read_timeout
      end

      # Sends one already-gzipped canonical batch body. Returns an Ack on a
      # 200 response; raises Error (tagged retryable?) otherwise.
      def ingest(gzip_body)
        raise ArgumentError, "store ingest token is required" if @ingest_token.empty?
        code, body = perform(gzip_body)
        handle_response(code, body)
      end

      # Runs one read-only statement. Retries a busy (429) response, bounded
      # by the server's Retry-After header, until `deadline` monotonic
      # seconds have elapsed overall; policy/limit errors (400/422/503) are
      # never retried here. Returns a QueryResult for format: :json, or the
      # server's CSV text verbatim for format: :csv.
      def query(sql, format: :json, deadline: DEFAULT_QUERY_DEADLINE)
        raise ArgumentError, "store query token is required" if @query_token.empty?
        format = format.to_s
        raise ArgumentError, "query format must be :json or :csv" unless QUERY_FORMATS.include?(format)

        deadline_at = monotonic_now + deadline
        loop do
          code, body, retry_after = perform_query(sql, format)
          if code == 429
            sleep(bounded_retry_wait(retry_after, deadline_at))
            next
          end
          return handle_query_response(code, body, format)
        end
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

        def initialize(buffer, max_bytes)
          @buffer = buffer
          super("response body exceeded #{max_bytes} bytes")
        end
      end

      def perform(gzip_body)
        request = Net::HTTP::Post.new(INGEST_PATH)
        request["Authorization"] = "Bearer #{@ingest_token}"
        request["Content-Type"] = "application/json"
        request["Content-Encoding"] = "gzip"
        request.body = gzip_body

        code, body, = perform_request(request, MAX_RESPONSE_BYTES)
        [ code, body ]
      rescue *NETWORK_ERRORS => e
        raise Error.new("store request failed (#{e.class})", retryable: true)
      end

      def perform_query(sql, format)
        request = Net::HTTP::Post.new(QUERY_PATH)
        request["Authorization"] = "Bearer #{@query_token}"
        request["Content-Type"] = "application/json"
        request.body = JSON.generate(protocol_version: QUERY_PROTOCOL_VERSION, sql: sql, format: format)

        code, body, response = perform_request(request, QUERY_RESPONSE_CAP + 1)
        [ code, body, response && response["Retry-After"] ]
      rescue *NETWORK_ERRORS => e
        raise Error.new("store query request failed (#{e.class})", retryable: true)
      end

      # Shared connect/send/bounded-read path for both endpoints. Returns
      # the response object too (query needs its Retry-After header; ingest
      # ignores it) — the caller must not read the body of that object
      # again, since #read_body has already been drained into `body`.
      def perform_request(request, max_bytes)
        http = Net::HTTP.new(@uri.host, @uri.port)
        http.use_ssl = https?(@uri)
        http.open_timeout = @open_timeout
        http.read_timeout = @read_timeout

        code = nil
        body = nil
        captured_response = nil
        begin
          http.start do |connection|
            connection.request(request) do |response|
              code = response.code.to_i
              captured_response = response
              body = read_bounded(response, max_bytes)
            end
          end
        rescue BoundedBodyExceeded => e
          body = e.buffer
        end
        [ code, body, captured_response ]
      end

      def read_bounded(response, max_bytes)
        buffer = "".b
        response.read_body do |chunk|
          remaining = max_bytes - buffer.bytesize
          buffer << (chunk.bytesize > remaining ? chunk.byteslice(0, remaining) : chunk)
          raise BoundedBodyExceeded.new(buffer, max_bytes) if buffer.bytesize >= max_bytes
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

      def handle_query_response(code, body, format)
        case code
        when 200
          format == "csv" ? validate_csv_size(body) : parse_query_result(body)
        when 400 then raise Error.new("query was rejected (invalid SQL, or not read-only)", retryable: false)
        when 422 then raise Error.new("query exceeded a store limit (rows, time, or response size)", retryable: false)
        when 503 then raise Error.new("store is temporarily unavailable (maintenance)", retryable: true)
        else raise Error.new("store responded with #{code}", retryable: false)
        end
      end

      def validate_csv_size(body)
        raise Error.new("store query response exceeded the #{QUERY_RESPONSE_CAP} byte cap", retryable: false) if body.bytesize > QUERY_RESPONSE_CAP
        body
      end

      def parse_query_result(body)
        raise Error.new("store query response exceeded the #{QUERY_RESPONSE_CAP} byte cap", retryable: false) if body.bytesize > QUERY_RESPONSE_CAP
        json = JSON.parse(body)
        QueryResult.new(
          columns: Array(json["columns"]),
          rows: Array(json["rows"]).map { |row| Array(row) },
          truncated: json["truncated"] == true
        )
      rescue JSON::ParserError, TypeError
        raise Error.new("store returned an unparsable query result", retryable: false)
      end

      # `retry_after` is the raw (possibly nil/blank/malformed) header
      # value; a missing or non-numeric value falls back to
      # RETRY_AFTER_DEFAULT rather than trusting an unbounded string.
      # Whatever it resolves to is clamped to RETRY_AFTER_CAP and to
      # whatever time remains before `deadline_at`.
      def bounded_retry_wait(retry_after, deadline_at)
        remaining = deadline_at - monotonic_now
        raise Error.new("store is busy and the query deadline elapsed", retryable: true) if remaining <= 0
        seconds = Integer(retry_after) rescue nil
        seconds = RETRY_AFTER_DEFAULT if seconds.nil? || seconds.negative?
        [ seconds, RETRY_AFTER_CAP, remaining ].min
      end

      def monotonic_now
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
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
