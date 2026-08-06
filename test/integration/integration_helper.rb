ENV["TZ"] = "UTC"

require "fileutils"
require "json"
require "net/http"
require "securerandom"
require "socket"
require "tmpdir"

require "active_support"
require "active_support/test_case"
require "active_support/core_ext"
require "active_support/testing/autorun"

require "wide_events"

# Cross-runtime integration harness: a real `store/cmd/wide-events` process,
# spoken to by the real Ruby client/sender/sink over loopback HTTP. Nothing
# here stubs either side of the protocol - every assertion in
# test/integration is made against bytes that crossed a socket.
#
# These files deliberately sit outside the `rake test` glob (see the
# Rakefile): they compile Go and spawn processes, so they run from their own
# command in CONTRIBUTING.md and their own CI job.
module StoreIntegration
  ROOT = File.expand_path("../..", __dir__)

  # The binary is built with the `testprobe` tag so the disk probe is
  # deterministic (a fixed 50 GiB volume, 20 GiB free) instead of inheriting
  # whatever the host filesystem happens to report - on a nearly-full
  # developer machine the real probe would park the store below its write
  # floor and every ingest would answer 507. The same seam is what lets
  # StoreProcess#initialize drive that 507 path on purpose via
  # WIDE_EVENTS_TESTPROBE_FREE_BYTES. Everything else in the binary - HTTP,
  # protocol, DuckDB, admin socket, shutdown - is the shipped code.
  BUILD_TAGS = "testprobe".freeze
  WRITE_FLOOR_BYTES = 5 * 1024 * 1024 * 1024 # 10% of the probe's 50 GiB volume

  INGEST_TOKEN = "a" * 64
  QUERY_TOKEN = "b" * 64

  class << self
    # Builds the store binary once per test process into a scratch directory
    # that is removed at exit.
    def binary
      @binary ||= build_binary
    end

    def workspace
      @workspace ||= begin
        # Kept short: the admin socket lives under here and AF_UNIX paths are
        # capped near 104 bytes on macOS.
        dir = Dir.mktmpdir("we-int-", "/tmp")
        at_exit { FileUtils.remove_entry(dir) rescue nil }
        dir
      end
    end

    def unused_port
      server = TCPServer.new("127.0.0.1", 0)
      port = server.addr[1]
      server.close
      port
    end

    private

    def build_binary
      path = File.join(workspace, "wide-events")
      output = IO.popen(
        { "CGO_ENABLED" => "1" },
        [ "go", "build", "-tags", BUILD_TAGS, "-ldflags", "-X main.version=0.0.0-integration",
          "-o", path, "./cmd/wide-events" ],
        chdir: File.join(ROOT, "store"),
        err: [ :child, :out ], &:read
      )
      raise "failed to build the store binary:\n#{output}" unless $?.success?
      path
    end
  end

  # One `wide-events` server process against one DuckDB directory. Stoppable
  # and restartable against the same database, which is what the reopen and
  # outage tests need.
  class StoreProcess
    attr_reader :port, :database, :admin_socket, :directory

    def initialize(directory: nil, service: "checkout", environment: "test", env: {})
      @directory = directory || Dir.mktmpdir("db-", StoreIntegration.workspace)
      @database = File.join(@directory, "wide-events.duckdb")
      @admin_socket = File.join(@directory, "admin.sock")
      @service = service
      @environment = environment
      @extra_env = env
      @pid = nil
      @log = File.join(@directory, "server.log")
      # Fixed for the lifetime of the object: a restart has to come back on
      # the same address, or a sender that survived the outage would be
      # retrying against a port nothing listens on.
      @port = StoreIntegration.unused_port
    end

    def start
      raise "store process is already running" if @pid

      log = File.open(@log, "a")
      @pid = Process.spawn(process_env, StoreIntegration.binary, out: log, err: log)
      log.close
      wait_for_up
      self
    end

    def stop
      return self unless @pid

      Process.kill("TERM", @pid)
      deadline = monotonic + 15
      until monotonic > deadline
        break if Process.waitpid(@pid, Process::WNOHANG)
        sleep 0.02
      end
      if process_alive?
        Process.kill("KILL", @pid)
        Process.waitpid(@pid)
      end
      @pid = nil
      self
    rescue Errno::ESRCH, Errno::ECHILD
      @pid = nil
      self
    end

    def restart(env: nil)
      stop
      @extra_env = env if env
      start
    end

    def url
      "http://127.0.0.1:#{@port}"
    end

    def logs
      File.exist?(@log) ? File.read(@log) : ""
    end

    def client(url: self.url)
      WideEvent::Store::Client.new(
        url: url,
        ingest_token: StoreIntegration::INGEST_TOKEN,
        query_token: StoreIntegration::QUERY_TOKEN
      )
    end

    # Runs one read-only statement through the real POST /v1/query route and
    # returns the parsed WideEvent::Store::QueryResult.
    def query(sql)
      client.query(sql)
    end

    def one_value(sql)
      result = query(sql)
      raise "expected exactly one row from #{sql.inspect}, got #{result.rows.length}" unless result.rows.length == 1
      result.rows.first.first
    end

    # Runs the shipped `wide-events admin backup` CLI over the private Unix
    # socket, exactly as the Kamal `wide-events-backup` alias does.
    def admin(*args)
      output = IO.popen(
        { "WIDE_EVENTS_ADMIN_SOCKET" => @admin_socket },
        [ StoreIntegration.binary, "admin", *args ],
        err: [ :child, :out ], &:read
      )
      [ output, $?.exitstatus ]
    end

    def up?
      response = Net::HTTP.start("127.0.0.1", @port, open_timeout: 0.5, read_timeout: 0.5) do |http|
        http.get("/up")
      end
      response.code == "200"
    rescue StandardError
      false
    end

    private

    def process_env
      {
        "WIDE_EVENTS_LISTEN" => "127.0.0.1:#{@port}",
        "WIDE_EVENTS_DATABASE" => @database,
        "WIDE_EVENTS_SERVICE" => @service,
        "WIDE_EVENTS_ENVIRONMENT" => @environment,
        "WIDE_EVENTS_INGEST_TOKEN" => StoreIntegration::INGEST_TOKEN,
        "WIDE_EVENTS_QUERY_TOKEN" => StoreIntegration::QUERY_TOKEN,
        "WIDE_EVENTS_ADMIN_SOCKET" => @admin_socket
      }.merge(@extra_env)
    end

    def wait_for_up
      deadline = monotonic + 60
      while monotonic < deadline
        raise "store process exited during startup:\n#{logs}" unless process_alive?
        return true if up?
        sleep 0.05
      end
      raise "store did not answer GET /up within 60s:\n#{logs}"
    end

    def process_alive?
      return false unless @pid
      Process.waitpid(@pid, Process::WNOHANG).nil?
    rescue Errno::ECHILD
      false
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end

  # A loopback TCP relay in front of the store that can drop the response of
  # the first N connections *after* the store has already committed the
  # batch. That is the real "the ack never came back" failure the sender's
  # idempotent retry exists for - no client stubbing involved.
  class LossyProxy
    def initialize(target_port:, drop_responses: 1)
      @target_port = target_port
      @remaining_drops = drop_responses
      @mutex = Mutex.new
      @server = TCPServer.new("127.0.0.1", 0)
      @threads = []
      @dropped = 0
    end

    def port
      @server.addr[1]
    end

    def url
      "http://127.0.0.1:#{port}"
    end

    def dropped
      @mutex.synchronize { @dropped }
    end

    def start
      @acceptor = Thread.new do
        loop do
          client = @server.accept
          @threads << Thread.new { relay(client) }
        end
      rescue IOError, Errno::EBADF
        nil
      end
      self
    end

    def stop
      @acceptor&.kill
      @server.close unless @server.closed?
      @threads.each { |thread| thread.join(2) || thread.kill }
      self
    end

    private

    def claim_drop
      @mutex.synchronize do
        next false unless @remaining_drops.positive?
        @remaining_drops -= 1
        @dropped += 1
        true
      end
    end

    def relay(client)
      upstream = TCPSocket.new("127.0.0.1", @target_port)
      drop = claim_drop
      loop do
        ready, = IO.select([ client, upstream ], nil, nil, 15)
        break unless ready

        ready.each do |io|
          chunk = io.readpartial(65_536)
          if io.equal?(client)
            upstream.write(chunk)
          elsif drop
            # The store only writes a response after its commit transaction
            # returns, so seeing any byte here means the batch is durable.
            # Hanging up now is exactly a lost acknowledgement.
            return
          else
            client.write(chunk)
          end
        end
      end
    rescue EOFError, Errno::ECONNRESET, Errno::EPIPE, IOError
      nil
    ensure
      client.close rescue nil
      upstream&.close rescue nil
    end
  end
end

class StoreIntegrationTest < ActiveSupport::TestCase
  # Real processes, real sockets, real DuckDB: keep the per-test budget well
  # above anything the machine should need, since CI and a busy laptop both
  # run these alongside other work.
  SHUTDOWN_TIMEOUT = 30

  def setup
    WideEvent.reset_configuration!
    @servers = []
    @senders = []
    @proxies = []
  end

  def teardown
    @senders.each { |sender| sender.shutdown(timeout: 1) rescue nil }
    @proxies.each { |proxy| proxy.stop rescue nil }
    @servers.each { |server| server.stop rescue nil }
    WideEvent.reset_configuration!
  end

  def start_store(**options)
    server = StoreIntegration::StoreProcess.new(**options)
    @servers << server
    server.start
  end

  def track_store(server)
    @servers << server
    server
  end

  def start_proxy(**options)
    proxy = StoreIntegration::LossyProxy.new(**options)
    @proxies << proxy
    proxy.start
  end

  # One independent "host": its own HTTP client, its own bounded sender, and
  # its own Sinks::Store whose envelope reports `source` as the given name.
  def build_host(name, url:, service: "checkout", environment: "test", **sender_options)
    client = WideEvent::Store::Client.new(
      url: url,
      ingest_token: StoreIntegration::INGEST_TOKEN,
      query_token: StoreIntegration::QUERY_TOKEN
    )
    sender = WideEvent::Store::Sender.new(
      client: client, service: service, environment: environment, **sender_options
    )
    @senders << sender
    sink = WideEvent::Sinks::Store.new(sender: sender, env: { "HOSTNAME" => name })
    [ sender, sink ]
  end

  def request_event(controller:, status: 200, duration_ms: 12.5)
    {
      "http.route.controller" => controller,
      "http.response.status_code" => status,
      "http.request.id" => SecureRandom.uuid,
      "duration_ms" => duration_ms,
      "main" => true
    }
  end

  def job_event(job_class:, queue: "default", duration_ms: 33.25)
    {
      "job.class" => job_class,
      "job.queue" => queue,
      "duration_ms" => duration_ms,
      "main" => true
    }
  end

  def wait_until(timeout: 15, message: "condition was never met")
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      return true if yield
      sleep 0.05
    end
    flunk(message)
  end
end
