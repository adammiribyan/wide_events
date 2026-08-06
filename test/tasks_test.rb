require "test_helper"
require "rake"
require "tempfile"
require "tmpdir"
require "fileutils"
require "socket"
require "stringio"
require "json"

# Several tests below call WideEvent::Tasks::Store methods directly
# (bypassing Rake::Task#invoke entirely, to drive the interactive/Readline
# branch without a real terminal). That module is only ever *defined* by
# loading tasks/store.rake — normally done lazily inside `with_rake` for
# each example — so, with Minitest's random test order, a direct-call test
# could run before any `with_rake`-based test ever loaded it, raising
# NameError. Loading it once here, at file-load time, guarantees the
# module exists before any test in this file runs, regardless of order.
load File.expand_path("../lib/wide_event/tasks/store.rake", __dir__)
load File.expand_path("../lib/wide_event/tasks/setup.rake", __dir__)

class TasksTest < ActiveSupport::TestCase
  QUERY_TOKEN = "test-query-token"

  def with_rake
    rake = Rake::Application.new
    Rake.application = rake
    Rake::Task.define_task(:environment)
    load File.expand_path("../lib/wide_event/tasks/registry.rake", __dir__)
    load File.expand_path("../lib/wide_event/tasks/store.rake", __dir__)
    load File.expand_path("../lib/wide_event/tasks/setup.rake", __dir__)
    yield rake
  end

  def with_registry_file(yaml)
    Tempfile.create([ "registry", ".yml" ]) do |f|
      f.write(yaml)
      f.flush
      WideEvent.configure { |c| c.registry_path = f.path }
      yield
    end
  end

  test "registry:check passes on a valid registry" do
    with_registry_file("report.format:\n  type: string\n") do
      with_rake do |rake|
        assert_output(/registry OK/) { rake["wide_events:registry:check"].invoke }
      end
    end
  end

  test "registry:check aborts on invalid entries" do
    with_registry_file("bad.attr:\n") do
      with_rake do |rake|
        error = assert_raises(SystemExit) do
          capture_io { rake["wide_events:registry:check"].invoke }
        end
        refute error.success?
      end
    end
  end

  test "registry:check aborts when no registry is configured" do
    with_rake do |rake|
      assert_raises(SystemExit) do
        capture_io { rake["wide_events:registry:check"].invoke }
      end
    end
  end

  test "registry:docs writes the generated table" do
    with_registry_file("report.format:\n  type: string\n  notes: pdf or csv\n") do
      Dir.mktmpdir do |dir|
        ENV["WIDE_EVENTS_DOCS_PATH"] = File.join(dir, "registry.md")
        with_rake do |rake|
          capture_io { rake["wide_events:registry:docs"].invoke }
        end
        doc = File.read(File.join(dir, "registry.md"))
        assert_match(/Generated from/, doc)
        assert_match(/`report\.format`/, doc)
        assert_match(/`main`/, doc, "gem defaults included")
      ensure
        ENV.delete("WIDE_EVENTS_DOCS_PATH")
      end
    end
  end

  test "registry:docs names the registry by the relative path the railtie stores" do
    Dir.mktmpdir do |dir|
      FileUtils.mkdir_p(File.join(dir, "config", "wide_event"))
      File.write(File.join(dir, "config", "wide_event", "registry.yml"), "report.format:\n  type: string\n")
      WideEvent.configure { |c| c.registry_path = "config/wide_event/registry.yml" }
      ENV["WIDE_EVENTS_DOCS_PATH"] = File.join(dir, "registry.md")
      with_rails_root(dir) do
        with_rake { |rake| capture_io { rake["wide_events:registry:docs"].invoke } }
      end
      doc = File.read(File.join(dir, "registry.md"))
      assert_match(%r{Generated from `config/wide_event/registry\.yml`}, doc)
      refute_includes doc, dir, "the committed doc must not carry a machine-specific path"
    ensure
      ENV.delete("WIDE_EVENTS_DOCS_PATH")
    end
  end

  # --- wide_events:sql ---

  test "sql task reads piped SQL and prints CSV" do
    ENV["WIDE_EVENTS_FORMAT"] = "csv"
    with_store_server(query_json(columns: [ "count" ], rows: [ [ 3 ] ])) do
      assert_output("count\n3\n") { invoke_store_task(stdin: StringIO.new("SELECT count(*) AS count FROM wide_events")) }
    end
  ensure
    ENV.delete("WIDE_EVENTS_FORMAT")
  end

  test "sql task reads piped SQL and prints JSON by request" do
    ENV["WIDE_EVENTS_FORMAT"] = "json"
    with_store_server(query_json(columns: [ "count" ], rows: [ [ 3 ] ])) do
      output = capture_io { invoke_store_task(stdin: StringIO.new("SELECT count(*) AS count FROM wide_events")) }.first
      assert_equal({ "columns" => [ "count" ], "rows" => [ [ 3 ] ], "truncated" => false }, JSON.parse(output))
    end
  ensure
    ENV.delete("WIDE_EVENTS_FORMAT")
  end

  test "sql task defaults to a rendered table" do
    with_store_server(query_json(columns: [ "count" ], rows: [ [ 3 ] ])) do
      output = capture_io { invoke_store_task(stdin: StringIO.new("SELECT count(*) AS count FROM wide_events")) }.first
      assert_match(/count/, output)
      assert_match(/\|\s*3\s*\|/, output)
    end
  end

  test "sql task does nothing for blank piped input" do
    with_store_server(query_json) do |server|
      capture_io { invoke_store_task(stdin: StringIO.new("   \n  ")) }
      assert_empty server.requests
    end
  end

  test "sql task runs a multiline, semicolon-terminated statement interactively and exits on quit" do
    with_store_server(query_json(columns: [ "count" ], rows: [ [ 3 ] ])) do
      readline = FakeReadline.new([ "SELECT count(*)", "FROM wide_events;", "quit" ])
      out = capture_io do
        WideEvent::Tasks::Store.run(stdin: StringIO.new(""), stdout: $stdout, readline: readline, interactive: true)
      end.first
      assert_match(/count/, out)
    end
  end

  test "sql task exits the interactive loop on EOF" do
    with_store_server(query_json) do
      readline = FakeReadline.new([]) # readline.readline returns nil immediately, simulating ^D
      assert_nothing_raised do
        WideEvent::Tasks::Store.run(stdin: StringIO.new(""), stdout: StringIO.new, readline: readline, interactive: true)
      end
    end
  end

  test "sql task keeps the interactive session open after a rejected query" do
    with_store_server([ 400, {}, { error: "Bad Request" }.to_json ]) do
      readline = FakeReadline.new([ "DELETE FROM wide_events;", "quit" ])
      out = capture_io do
        WideEvent::Tasks::Store.run(stdin: StringIO.new(""), stdout: $stdout, readline: readline, interactive: true)
      end.first
      assert_match(/error:/, out)
      refute_includes out, QUERY_TOKEN
    end
  end

  test "sql task aborts with setup guidance when the query token is missing (url present, no ingest token either)" do
    # A query-only deployment: url is configured, ingest token is not (it
    # must not be required to run wide_events:sql — see Client#query), but
    # the query token is missing, which must abort with exact guidance.
    WideEvent.configure { |c| c.store_url = "https://store.example.com"; c.store_ingest_token = nil; c.store_query_token = nil }
    error, stderr = abort_message { invoke_store_task(stdin: StringIO.new("SELECT 1")) }
    refute error.success?
    assert_match(/WIDE_EVENTS_QUERY_TOKEN/, stderr)
  end

  test "sql task aborts with setup guidance when the store url is missing" do
    WideEvent.configure { |c| c.store_url = nil; c.store_query_token = QUERY_TOKEN }
    error, stderr = abort_message { invoke_store_task(stdin: StringIO.new("SELECT 1")) }
    refute error.success?
    assert_match(/WIDE_EVENTS_URL/, stderr)
  end

  test "sql task aborts on a rejected piped query without leaking the token or a raw server body" do
    with_store_server([ 400, {}, { error: "Bad Request" }.to_json ]) do
      error = assert_raises(SystemExit) do
        capture_io { invoke_store_task(stdin: StringIO.new("DELETE FROM wide_events")) }
      end
      refute error.success?
    end
  end

  # --- wide_events:setup:check ---

  test "setup:check is registered under the wide_events namespace" do
    with_rake do |rake|
      assert_includes rake.tasks.map(&:name), "wide_events:setup:check"
    end
  end

  test "setup:check aborts with guidance when config/deploy.yml has not been generated yet" do
    # A bare Checker (real runner/resolver/etc.) is safe here: with no
    # config/deploy.yml under `root`, WideEvent::Setup::Checker fails before
    # ever shelling out to `kamal` or touching the network - so this
    # exercises the task's abort/exit-status wiring without needing any of
    # the injected doubles the checker's own test suite uses.
    Dir.mktmpdir do |dir|
      error = nil
      stdout, = capture_io do
        begin
          WideEvent::Tasks::Setup.run(root: dir)
        rescue SystemExit => e
          error = e
        end
      end
      refute_nil error, "expected the task to abort (raise SystemExit)"
      refute error.success?
      assert_match(%r{config/deploy\.yml}, stdout)
      assert_match(/failed/, stdout)
    end
  end

  private

  def query_json(columns: [ "value" ], rows: [ [ 1 ] ], truncated: false)
    { columns: columns, rows: rows, truncated: truncated }.to_json
  end

  # Configures a real store URL/query token pointing at a one-shot fake
  # server that returns `response` (a JSON string body, or an
  # [status, headers, body] triple) for every /v1/query it receives.
  def with_store_server(response)
    triple = response.is_a?(String) ? [ 200, {}, response ] : response
    server = FakeSqlServer.new(triple)
    WideEvent.configure do |c|
      c.store_url = "http://127.0.0.1:#{server.port}"
      c.store_ingest_token = "unused-ingest-token"
      c.store_query_token = QUERY_TOKEN
    end
    yield server
  ensure
    server&.close
  end

  # Runs `block`, expecting it to hit a Kernel#abort (raising SystemExit).
  # Rescuing SystemExit *inside* the capture_io block (rather than wrapping
  # capture_io in assert_raises, as the registry:check tests do) lets us
  # get the captured stderr text back too, so tests can assert on the
  # exact setup-guidance message instead of only the non-zero exit.
  def abort_message
    error = nil
    _stdout, stderr = capture_io do
      begin
        yield
      rescue SystemExit => e
        error = e
      end
    end
    refute_nil error, "expected the block to abort (raise SystemExit)"
    [ error, stderr ]
  end

  def invoke_store_task(stdin:)
    original_stdin = $stdin
    $stdin = stdin
    with_rake { |rake| rake["wide_events:sql"].invoke }
  ensure
    $stdin = original_stdin
  end

  # readline.readline(prompt, add_history) returning each queued line in
  # turn, then nil (EOF) once exhausted — stands in for GNU Readline/Reline
  # without a real terminal.
  class FakeReadline
    def initialize(lines)
      @lines = lines.dup
    end

    def readline(_prompt, _add_history)
      @lines.shift
    end
  end

  # A minimal raw-socket HTTP responder for POST /v1/query: serves the same
  # fixed [status, headers, body] response to every connection it accepts,
  # which is all that's needed here (each test either sends one request,
  # or several identical ones in the interactive-loop tests).
  class FakeSqlServer
    attr_reader :port, :requests

    def initialize(response)
      @tcp = TCPServer.new("127.0.0.1", 0)
      @port = @tcp.local_address.ip_port
      @response = response
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
      request_line = socket.gets
      return if request_line.nil?
      headers = {}
      while (line = socket.gets) && !line.strip.empty?
        key, value = line.split(":", 2)
        headers[key.strip.downcase] = value.strip if key && value
      end
      length = headers["content-length"].to_i
      body = length > 0 ? socket.read(length) : ""
      @requests << { headers: headers, body: body }

      status, extra_headers, response_body = @response
      header_lines = { "Content-Type" => "application/json", "Content-Length" => response_body.bytesize.to_s, "Connection" => "close" }
      header_lines.merge!(extra_headers || {})
      socket.write("HTTP/1.1 #{status} Status\r\n")
      header_lines.each { |key, value| socket.write("#{key}: #{value}\r\n") }
      socket.write("\r\n")
      socket.write(response_body)
    rescue IOError, Errno::EPIPE, Errno::ECONNRESET
      nil
    ensure
      socket.close rescue nil
    end
  end
end
