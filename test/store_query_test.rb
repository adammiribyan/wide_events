require "test_helper"
require "socket"
require "json"
require "stringio"

class StoreQueryTest < ActiveSupport::TestCase
  INGEST_TOKEN = "test-ingest-token"
  QUERY_TOKEN = "test-query-token"

  test "query parses columns, rows, and truncated from a json response" do
    with_fake_server([ [ 200, {}, query_json(rows: [ [ 1 ] ]) ] ]) do |server, _requests|
      result = build_client(server).query("SELECT 1")
      assert_instance_of WideEvent::Store::QueryResult, result
      assert_equal [ "value" ], result.columns
      assert_equal [ [ 1 ] ], result.rows
      refute result.truncated
    end
  end

  test "query sends the query token, not the ingest token, and the request body" do
    with_fake_server([ [ 200, {}, query_json ] ]) do |server, requests|
      build_client(server).query("SELECT 1 AS value")
      request = requests.pop
      assert_equal "Bearer #{QUERY_TOKEN}", request.fetch(:headers).fetch("authorization")
      body = JSON.parse(request.fetch(:body))
      assert_equal 1, body.fetch("protocol_version")
      assert_equal "SELECT 1 AS value", body.fetch("sql")
      assert_equal "json", body.fetch("format")
    end
  end

  test "query retries a busy response within the command deadline" do
    with_fake_server([ [ 429, { "Retry-After" => "0" }, "{}" ], [ 200, {}, query_json(rows: [ [ 1 ] ]) ] ]) do |server, requests|
      result = build_client(server).query("SELECT 1")
      assert_equal [ [ 1 ] ], result.rows
      assert_equal 2, requests.size
    end
  end

  test "query gives up once the overall deadline elapses while busy" do
    with_fake_server([ [ 429, { "Retry-After" => "0" }, "{}" ] ]) do |server, _requests|
      error = assert_raises(WideEvent::Store::Client::Error) { build_client(server).query("SELECT 1", deadline: 0) }
      assert error.retryable?
      assert_match(/busy/, error.message)
    end
  end

  test "query does not retry a result-limit (422) response" do
    with_fake_server([ [ 422, {}, { error: "Unprocessable Entity" }.to_json ] ]) do |server, requests|
      error = assert_raises(WideEvent::Store::Client::Error) { build_client(server).query("SELECT 1") }
      refute error.retryable?
      assert_equal 1, requests.size
    end
  end

  test "query does not retry an invalid/read-write (400) response" do
    with_fake_server([ [ 400, {}, { error: "Bad Request" }.to_json ] ]) do |server, requests|
      error = assert_raises(WideEvent::Store::Client::Error) { build_client(server).query("DELETE FROM wide_events") }
      refute error.retryable?
      assert_equal 1, requests.size
    end
  end

  test "query surfaces a maintenance (503) response as retryable without looping" do
    with_fake_server([ [ 503, {}, { error: "Service Unavailable" }.to_json ] ]) do |server, requests|
      error = assert_raises(WideEvent::Store::Client::Error) { build_client(server).query("SELECT 1") }
      assert error.retryable?
      assert_equal 1, requests.size
    end
  end

  test "query returns CSV verbatim for format: :csv" do
    with_fake_server([ [ 200, {}, "value\n1\n" ] ]) do |server, _requests|
      result = build_client(server).query("SELECT 1 AS value", format: :csv)
      assert_equal "value\n1\n", result
    end
  end

  test "query rejects an oversized response instead of parsing a truncated body" do
    oversized = query_json(rows: Array.new(1) { [ "x" * (WideEvent::Store::Client::QUERY_RESPONSE_CAP + 1_000) ] })
    with_fake_server([ [ 200, {}, oversized ] ]) do |server, _requests|
      error = assert_raises(WideEvent::Store::Client::Error) { build_client(server).query("SELECT 1") }
      refute error.retryable?
      assert_match(/cap/, error.message)
    end
  end

  test "requires a non-blank query token to query" do
    with_fake_server([ [ 200, {}, query_json ] ]) do |server, _requests|
      client = WideEvent::Store::Client.new(url: "http://127.0.0.1:#{server.port}", ingest_token: INGEST_TOKEN)
      assert_raises(ArgumentError) { client.query("SELECT 1") }
    end
  end

  test "queries with no ingest token at all — a query-only client never needs one" do
    with_fake_server([ [ 200, {}, query_json(rows: [ [ 1 ] ]) ] ]) do |server, _requests|
      client = WideEvent::Store::Client.new(url: "http://127.0.0.1:#{server.port}", query_token: QUERY_TOKEN)
      assert_equal [ [ 1 ] ], client.query("SELECT 1").rows
    end
  end

  test "constructing a client without an ingest token does not raise (validated lazily by #ingest, not here)" do
    assert WideEvent::Store::Client.new(url: "https://example.com", query_token: QUERY_TOKEN)
    assert WideEvent::Store::Client.new(url: "https://example.com")
  end

  test "never includes the query token or sql text in a raised error message" do
    with_fake_server([ [ 400, {}, { error: "Bad Request" }.to_json ] ]) do |server, _requests|
      secret_sql = "SELECT super_secret_column FROM wide_events"
      error = assert_raises(WideEvent::Store::Client::Error) { build_client(server).query(secret_sql) }
      refute_includes error.message, secret_sql
      refute_includes error.message, QUERY_TOKEN
    end
  end

  # --- Formatter ---

  test "Formatter.table renders NULL for nil values, sizes columns, and emits no ANSI" do
    result = WideEvent::Store::QueryResult.new(columns: [ "id", "name" ], rows: [ [ 1, "alice" ], [ 2, nil ] ], truncated: false)
    io = StringIO.new
    WideEvent::Store::Formatter.table(result, io: io)
    output = io.string
    assert_includes output, "NULL"
    assert_includes output, "alice"
    refute_match(/\e\[/, output, "table output must never contain ANSI escape sequences")
  end

  test "Formatter.table notes truncation" do
    result = WideEvent::Store::QueryResult.new(columns: [ "id" ], rows: [ [ 1 ] ], truncated: true)
    io = StringIO.new
    WideEvent::Store::Formatter.table(result, io: io)
    assert_match(/truncated/, io.string)
  end

  test "Formatter.json emits columns, rows, and truncated" do
    result = WideEvent::Store::QueryResult.new(columns: [ "id" ], rows: [ [ 1 ], [ nil ] ], truncated: true)
    io = StringIO.new
    WideEvent::Store::Formatter.json(result, io: io)
    parsed = JSON.parse(io.string)
    assert_equal [ "id" ], parsed.fetch("columns")
    assert_equal [ [ 1 ], [ nil ] ], parsed.fetch("rows")
    assert_equal true, parsed.fetch("truncated")
  end

  test "Formatter.csv renders a header row and null as an empty field" do
    result = WideEvent::Store::QueryResult.new(columns: [ "count" ], rows: [ [ 3 ] ], truncated: false)
    io = StringIO.new
    WideEvent::Store::Formatter.csv(result, io: io)
    assert_equal "count\n3\n", io.string
  end

  test "Formatter.csv renders null as an empty field, not the text NULL" do
    result = WideEvent::Store::QueryResult.new(columns: [ "name" ], rows: [ [ nil ] ], truncated: false)
    io = StringIO.new
    WideEvent::Store::Formatter.csv(result, io: io)
    assert_equal "name\n\n", io.string
  end

  # --- QueryResult ---

  test "QueryResult is immutable" do
    result = WideEvent::Store::QueryResult.new(columns: [ "id" ], rows: [ [ 1 ] ], truncated: false)
    assert_raises(FrozenError) { result.rows << [ 2 ] }
    assert_raises(FrozenError) { result.columns << "extra" }
  end

  private

  def build_client(server, read_timeout: 2, open_timeout: 1)
    WideEvent::Store::Client.new(
      url: "http://127.0.0.1:#{server.port}",
      ingest_token: INGEST_TOKEN,
      query_token: QUERY_TOKEN,
      open_timeout: open_timeout,
      read_timeout: read_timeout
    )
  end

  def query_json(columns: [ "value" ], rows: [ [ 1 ] ], truncated: false)
    { columns: columns, rows: rows, truncated: truncated }.to_json
  end

  def with_fake_server(responses)
    server = FakeQueryServer.new(responses)
    yield server, server.requests
  ensure
    server&.close
  end

  # A minimal raw-socket HTTP responder that serves a fixed sequence of
  # responses (repeating the last one if more requests arrive than were
  # queued), so a single test can exercise a busy-then-success retry.
  class FakeQueryServer
    attr_reader :port, :requests

    def initialize(responses)
      @tcp = TCPServer.new("127.0.0.1", 0)
      @port = @tcp.local_address.ip_port
      @responses = responses
      @index = 0
      @mutex = Mutex.new
      @requests = Queue.new
      @handlers = []
      @thread = Thread.new { serve }
    end

    def close
      @tcp.close
      @thread.kill
      @handlers.each(&:kill)
    end

    private

    def serve
      loop do
        socket = @tcp.accept
        @handlers << Thread.new { handle(socket) }
      end
    rescue IOError, Errno::EBADF
      nil
    end

    def handle(socket)
      headers, body = read_request(socket)
      @requests << { headers: headers, body: body } if headers
      status, extra_headers, response_body = next_response
      write_response(socket, status: status, headers: extra_headers, body: response_body)
    rescue IOError, Errno::EPIPE, Errno::ECONNRESET
      nil
    ensure
      socket.close rescue nil
    end

    def next_response
      @mutex.synchronize do
        response = @responses[@index]
        @index += 1 if @index < @responses.length - 1
        response
      end
    end

    def read_request(socket)
      request_line = socket.gets
      return nil if request_line.nil?
      headers = {}
      while (line = socket.gets) && !line.strip.empty?
        key, value = line.split(":", 2)
        headers[key.strip.downcase] = value.strip if key && value
      end
      length = headers["content-length"].to_i
      body = length > 0 ? socket.read(length) : ""
      [ headers, body ]
    end

    def write_response(socket, status:, headers:, body:)
      header_lines = { "Content-Type" => "application/json", "Content-Length" => body.bytesize.to_s, "Connection" => "close" }
      header_lines.merge!(headers || {})
      socket.write("HTTP/1.1 #{status} Status\r\n")
      header_lines.each { |key, value| socket.write("#{key}: #{value}\r\n") }
      socket.write("\r\n")
      socket.write(body)
    end
  end
end
