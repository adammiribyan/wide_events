require "test_helper"
require "socket"
require "zlib"
require "json"

class StoreClientTest < ActiveSupport::TestCase
  TOKEN = "test-ingest-token"

  test "rejects a non-HTTPS, non-loopback url" do
    error = assert_raises(ArgumentError) do
      WideEvent::Store::Client.new(url: "http://example.com", ingest_token: TOKEN)
    end
    assert_match(/https/i, error.message)
  end

  test "accepts an https url without connecting" do
    assert WideEvent::Store::Client.new(url: "https://example.com", ingest_token: TOKEN)
  end

  test "accepts plain http for loopback hosts" do
    assert WideEvent::Store::Client.new(url: "http://127.0.0.1:9999", ingest_token: TOKEN)
    assert WideEvent::Store::Client.new(url: "http://localhost:9999", ingest_token: TOKEN)
  end

  test "requires a non-blank ingest token to ingest (not at construction — a query-only client has none)" do
    assert WideEvent::Store::Client.new(url: "https://example.com", ingest_token: "")
    assert WideEvent::Store::Client.new(url: "https://example.com", ingest_token: nil)

    client = WideEvent::Store::Client.new(url: "https://example.com", ingest_token: "")
    assert_raises(ArgumentError) { client.ingest(gzip("{}")) }

    client = WideEvent::Store::Client.new(url: "https://example.com", ingest_token: nil)
    assert_raises(ArgumentError) { client.ingest(gzip("{}")) }
  end

  test "sends the bearer token, content-type, and content-encoding, and never leaks the token in an error" do
    with_fake_server do |server, requests|
      server.respond(status: 200, body: ack_json)
      client = build_client(server)
      ack = client.ingest(gzip("{}"))
      assert_equal "018f6f50-2f9d-7fb0-a258-6d9f95f20c01", ack.batch_id
      assert_equal false, ack.duplicate
      assert_equal [], ack.warnings

      request = requests.pop
      assert_equal "Bearer #{TOKEN}", request.fetch("authorization")
      assert_equal "application/json", request.fetch("content-type")
      assert_equal "gzip", request.fetch("content-encoding")
    end
  end

  test "parses duplicate and warnings from the acknowledgement" do
    with_fake_server do |server, _requests|
      server.respond(status: 200, body: ack_json(duplicate: true, warnings: [ "client service/environment differs" ]))
      client = build_client(server)
      ack = client.ingest(gzip("{}"))
      assert_equal true, ack.duplicate
      assert_equal [ "client service/environment differs" ], ack.warnings
    end
  end

  test "classifies 429 as retryable" do
    assert_retryable(429)
  end

  test "classifies 507 as retryable" do
    assert_retryable(507)
  end

  test "classifies 503 as retryable" do
    assert_retryable(503)
  end

  test "classifies 500 as retryable" do
    assert_retryable(500)
  end

  test "classifies 400 as permanent" do
    assert_permanent(400)
  end

  test "classifies 426 as permanent" do
    assert_permanent(426)
  end

  test "classifies 401 as permanent" do
    assert_permanent(401)
  end

  test "classifies connection refused as a retryable network error" do
    server = TCPServer.new("127.0.0.1", 0)
    port = server.local_address.ip_port
    server.close # nothing listening now
    client = WideEvent::Store::Client.new(url: "http://127.0.0.1:#{port}", ingest_token: TOKEN)

    error = assert_raises(WideEvent::Store::Client::Error) { client.ingest(gzip("{}")) }
    assert error.retryable?
    refute_includes error.message, TOKEN
  end

  test "classifies a read timeout as a retryable network error" do
    with_fake_server do |server, _requests|
      server.hang
      client = build_client(server, read_timeout: 0.2)
      error = assert_raises(WideEvent::Store::Client::Error) { client.ingest(gzip("{}")) }
      assert error.retryable?
    end
  end

  test "bounds response body reading instead of consuming an unbounded stream" do
    with_fake_server do |server, _requests|
      server.stream_forever
      client = build_client(server, read_timeout: 5)
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      error = assert_raises(WideEvent::Store::Client::Error) { client.ingest(gzip("{}")) }
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      assert error.retryable? # unparsable/garbage body past the 200 status
      assert_operator elapsed, :<, 2.0, "client should stop reading well before the 5s read timeout"
    end
  end

  test "never includes the event body or token in a raised error message" do
    with_fake_server do |server, _requests|
      server.respond(status: 400, body: { error: "bad request" }.to_json)
      client = build_client(server)
      secret_event = "super-secret-attribute-value"
      error = assert_raises(WideEvent::Store::Client::Error) { client.ingest(gzip(secret_event)) }
      refute_includes error.message, secret_event
      refute_includes error.message, TOKEN
    end
  end

  private

  def assert_retryable(status)
    with_fake_server do |server, _requests|
      server.respond(status: status, body: "{}")
      client = build_client(server)
      error = assert_raises(WideEvent::Store::Client::Error) { client.ingest(gzip("{}")) }
      assert error.retryable?, "expected #{status} to be retryable"
    end
  end

  def assert_permanent(status)
    with_fake_server do |server, _requests|
      server.respond(status: status, body: "{}")
      client = build_client(server)
      error = assert_raises(WideEvent::Store::Client::Error) { client.ingest(gzip("{}")) }
      refute error.retryable?, "expected #{status} to be permanent"
    end
  end

  def build_client(server, read_timeout: 2, open_timeout: 1)
    WideEvent::Store::Client.new(
      url: "http://127.0.0.1:#{server.port}",
      ingest_token: TOKEN,
      open_timeout: open_timeout,
      read_timeout: read_timeout
    )
  end

  def gzip(body)
    Zlib.gzip(body)
  end

  def ack_json(duplicate: false, warnings: [])
    {
      protocol_version: 1,
      batch_id: "018f6f50-2f9d-7fb0-a258-6d9f95f20c01",
      duplicate: duplicate,
      warnings: warnings
    }.to_json
  end

  # Minimal raw-socket HTTP responder: gives full control over status,
  # body, hangs, and unbounded streaming without pulling in a server gem.
  class FakeServer
    attr_reader :port

    def initialize
      @tcp = TCPServer.new("127.0.0.1", 0)
      @port = @tcp.local_address.ip_port
      @requests = Queue.new
      @mode = nil
      @handlers = []
      @thread = Thread.new { serve }
    end

    def requests
      @requests
    end

    def respond(status:, body:)
      @mode = [ :respond, status, body ]
    end

    def hang
      @mode = [ :hang ]
    end

    def stream_forever
      @mode = [ :stream_forever ]
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
      headers = read_request(socket)
      @requests << headers if headers
      case @mode
      in [ :respond, status, body ]
        write_response(socket, status: status, body: body)
      in [ :hang ]
        sleep 5
      in [ :stream_forever ]
        socket.write("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n")
        loop { socket.write("x" * 4096) }
      else
        write_response(socket, status: 200, body: "{}")
      end
    rescue IOError, Errno::EPIPE, Errno::ECONNRESET
      nil
    ensure
      socket.close rescue nil
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
      socket.read(length) if length > 0
      headers
    end

    def write_response(socket, status:, body:)
      socket.write("HTTP/1.1 #{status} Status\r\nContent-Type: application/json\r\nContent-Length: #{body.bytesize}\r\nConnection: close\r\n\r\n#{body}")
    end
  end

  def with_fake_server
    server = FakeServer.new
    yield server, server.requests
  ensure
    server&.close
  end
end
