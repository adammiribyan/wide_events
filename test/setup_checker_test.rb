require "test_helper"
require "tmpdir"
require "fileutils"
require "yaml"
require "json"
require "zlib"
require "wide_event/setup/checker"
require "wide_event/setup/command_runner"
require "wide_event/store/query_result"

class SetupCheckerTest < ActiveSupport::TestCase
  DEFAULT_HOST = "203.0.113.20"
  DEFAULT_HOSTNAME = "telemetry.example.com"
  DEFAULT_SERVICE = "my_app"
  FIXED_ID = "11111111-1111-1111-1111-111111111111"

  teardown do
    Array(@tmp_roots).each { |dir| FileUtils.remove_entry(dir) }
  end

  # ---- The two tests specified verbatim by the plan ------------------------

  test "DNS wait stops at 120 seconds and prints the resume command" do
    checker = build_checker(resolver: always_missing, clock: fake_clock, sleeper: advancing_sleeper)
    result = checker.run
    refute result.success?
    assert_includes result.message, "bin/rails wide_events:setup:check"
    assert_equal 120, fake_clock.elapsed
  end

  test "routing health with write_ready false is an actionable failure" do
    checker = build_checker(health: { "protocol_version" => 1, "write_ready" => false, "degradation_reason" => "disk_space_low" })
    refute checker.run.success?
  end

  # ---- DNS: two distinct failure modes --------------------------------------

  test "a genuine DNS-propagation timeout is needs_dns, not failed, once the store host itself resolves" do
    checker = build_checker(resolver: always_missing, clock: fake_clock, sleeper: advancing_sleeper)
    result = checker.run
    assert_equal "needs_dns", result.state
  end

  test "fails with a config error (not needs_dns) when the store host itself cannot be resolved" do
    # A mistyped SSH address/hostname in config/deploy.yml never starts
    # resolving no matter how long the caller waits, so this must not be
    # confused with (or advise retrying like) a genuine DNS-propagation
    # timeout: it is a config error, so it must be "failed", must not
    # mention resuming, and must name the offending host.
    root = make_root(host: "typo-host.example.invalid")
    checker = build_checker(root: root, resolver: ->(_hostname) { [] })
    result = checker.run

    assert_equal "failed", result.state
    assert_equal false, result.checks[:dns]
    assert_match(/typo-host\.example\.invalid/, result.message)
    assert_match(/config\/deploy\.yml/, result.message)
    refute_match(/setup:check/, result.message)
  end

  # ---- failed: Kamal version/config/directory preflights -------------------

  test "fails and prints an upgrade instruction when the installed kamal is older than the minimum version" do
    checker = build_checker(runner: FakeRunner.new(kamal_version: "2.9.0"))
    result = checker.run
    assert_equal "failed", result.state
    assert_equal false, result.checks[:kamal_version]
    assert_match(/2\.9\.0/, result.message)
    assert_match(/2\.10\.0/, result.message)
  end

  test "fails and preserves kamal config's actionable stderr" do
    checker = build_checker(runner: FakeRunner.new(kamal_config_ok: false, kamal_config_stderr: "config/deploy.yml: bad indentation\n"))
    result = checker.run
    assert_equal "failed", result.state
    assert_match(/bad indentation/, result.message)
  end

  test "fails with the exact one-time administrator command when the host directory cannot be created" do
    checker = build_checker(runner: FakeRunner.new(host_dir_ok: false, host_dir_stderr: "install: Permission denied\n"))
    result = checker.run
    assert_equal "failed", result.state
    assert_includes result.message,
      "kamal server exec --hosts=#{DEFAULT_HOST} -- install -d -m 0750 -o 1000 -g 1000 /var/lib/#{DEFAULT_SERVICE}-wide-events"
  end

  test "fails when config/deploy.yml has no wide_events accessory yet" do
    root = Dir.mktmpdir
    (@tmp_roots ||= []) << root
    FileUtils.mkdir_p(File.join(root, "config"))
    File.write(File.join(root, "config/deploy.yml"), "service: my_app\n")

    checker = build_checker(root: root)
    result = checker.run
    assert_equal "failed", result.state
    assert_equal false, result.checks[:deploy_config]
    assert_match(/wide_events:store/, result.message)
  end

  test "fails when config/deploy.yml is missing entirely" do
    root = Dir.mktmpdir
    (@tmp_roots ||= []) << root

    checker = build_checker(root: root)
    result = checker.run
    assert_equal "failed", result.state
    assert_match(/config\/deploy\.yml/, result.message)
  end

  test "with no injected dependencies at all, still fails safely on a missing config/deploy.yml" do
    # Exercises the real (non-test-double) runner/resolver/http_get/
    # client_factory defaults just enough to prove they construct without
    # error; the missing config/deploy.yml short-circuits before any of
    # them would actually shell out, resolve DNS, or touch the network.
    root = Dir.mktmpdir
    (@tmp_roots ||= []) << root

    result = WideEvent::Setup::Checker.new(root: root).run
    assert_equal "failed", result.state
    assert_match(/config\/deploy\.yml/, result.message)
  end

  # ---- ready_to_deploy: DNS good, store not answering yet -------------------

  test "is ready_to_deploy and suggests kamal setup when the endpoint refuses connections" do
    checker = build_checker(http_get: ->(_uri) { { error: :connection_refused } })
    result = checker.run
    assert_equal "ready_to_deploy", result.state
    refute result.success?
    assert_match(/kamal setup/, result.message)
  end

  test "is ready_to_deploy when the endpoint returns 404 (Kamal Proxy with nothing behind it)" do
    checker = build_checker(http_get: ->(_uri) { { status: 404, body: "" } })
    result = checker.run
    assert_equal "ready_to_deploy", result.state
  end

  # ---- failed: bad protocol version ----------------------------------------

  test "fails when the store speaks an unsupported protocol version" do
    checker = build_checker(health: { "protocol_version" => 2, "write_ready" => true, "degradation_reason" => nil })
    result = checker.run
    assert_equal "failed", result.state
    assert_match(/protocol/, result.message)
  end

  # ---- healthy: full synthetic round trip -----------------------------------

  test "is healthy end-to-end: sends a synthetic event, queries it back, and prints route stats" do
    client = FakeStoreClient.new
    checker = build_checker(client: client)
    result = checker.run

    assert_equal "healthy", result.state
    assert result.success?
    assert_equal 1, client.ingested_batches.size
    assert_equal FIXED_ID, client.ingested_batches.first.dig("events", 0, "id")
    assert_includes result.message, FIXED_ID
    assert_includes result.message, "route count"
  end

  test "reports failure when the synthetic event cannot be read back by id" do
    checker = build_checker(client: FakeStoreClient.new(missing_row: true))
    result = checker.run
    assert_equal "failed", result.state
    assert_match(/could not read it back/, result.message)
  end

  test "reports failure when ingest is rejected" do
    checker = build_checker(client: FakeStoreClient.new(raise_on_ingest: WideEvent::Store::Client::Error.new("store responded with 401", retryable: false)))
    result = checker.run
    assert_equal "failed", result.state
    assert_match(/synthetic round trip failed/, result.message)
  end

  test "fails when the ingest/query token files are missing" do
    root = make_root
    File.delete(File.join(root, ".kamal/wide-events-ingest-token"))

    checker = build_checker(root: root, client: FakeStoreClient.new)
    result = checker.run
    assert_equal "failed", result.state
    assert_match(/wide-events-ingest-token/, result.message)
  end

  # ---- degraded/checks hash shape -------------------------------------------

  test "every check up to the failing one is recorded true in checks" do
    checker = build_checker(health: { "protocol_version" => 1, "write_ready" => false, "degradation_reason" => "disk_space_low" })
    result = checker.run
    assert_equal true, result.checks[:deploy_config]
    assert_equal true, result.checks[:kamal_version]
    assert_equal true, result.checks[:kamal_config]
    assert_equal true, result.checks[:host_directory]
    assert_equal true, result.checks[:dns]
    assert_equal false, result.checks[:endpoint]
  end

  private

  # ---- Root fixture ----------------------------------------------------------

  def make_root(host: DEFAULT_HOST, hostname: DEFAULT_HOSTNAME, service: DEFAULT_SERVICE)
    dir = Dir.mktmpdir
    (@tmp_roots ||= []) << dir
    FileUtils.mkdir_p(File.join(dir, "config"))
    File.write(File.join(dir, "config/deploy.yml"), deploy_yml(host: host, hostname: hostname, service: service))
    FileUtils.mkdir_p(File.join(dir, ".kamal"))
    File.write(File.join(dir, ".kamal/wide-events-ingest-token"), "ingest-token-value\n")
    File.write(File.join(dir, ".kamal/wide-events-query-token"), "query-token-value\n")
    dir
  end

  def deploy_yml(host:, hostname:, service:)
    <<~YAML
      service: #{service}
      image: #{service}

      accessories:
        wide_events:
          image: ghcr.io/adammiribyan/wide-events-store:0.1.0
          host: #{host}
          proxy:
            host: #{hostname}
            ssl: true
            app_port: 7421
            healthcheck:
              path: /up
              interval: 3
              timeout: 3
          env:
            clear:
              WIDE_EVENTS_LISTEN: 0.0.0.0:7421
              WIDE_EVENTS_DATABASE: /var/lib/wide-events/events.duckdb
              WIDE_EVENTS_SERVICE: #{service}
              WIDE_EVENTS_ENVIRONMENT: production
              WIDE_EVENTS_RETENTION_DAYS: 30
            secret:
              - WIDE_EVENTS_INGEST_TOKEN
              - WIDE_EVENTS_QUERY_TOKEN
          directories:
            - local: /var/lib/#{service}-wide-events
              remote: /var/lib/wide-events
              mode: "0750"
              owner: "1000:1000"
          options:
            cpus: 2
            memory: 2g
    YAML
  end

  # ---- Injected-dependency defaults ------------------------------------------

  # `always_missing`/`fake_clock`/`advancing_sleeper` are called as bare
  # method references from test bodies (per the plan's own example test
  # text), so `fake_clock` must be memoized: `advancing_sleeper` needs to
  # mutate the exact same instance the test later reads `.elapsed` from.
  def always_missing
    ->(_hostname) { [] }
  end

  def fake_clock
    @fake_clock ||= FakeClock.new
  end

  def advancing_sleeper
    AdvancingSleeper.new(fake_clock)
  end

  def default_resolver
    ->(hostname) { hostname == DEFAULT_HOSTNAME ? [ DEFAULT_HOST ] : [] }
  end

  def default_health
    { "protocol_version" => 1, "write_ready" => true, "degradation_reason" => nil }
  end

  def health_http_get(health)
    ->(_uri) { { status: 200, body: JSON.generate(health) } }
  end

  def build_checker(root: make_root, runner: FakeRunner.new, clock: -> { 0 }, sleeper: ->(_seconds) { },
                     resolver: default_resolver, health: default_health, http_get: nil, client: nil)
    http_get ||= health_http_get(health)
    client_double = client || FakeStoreClient.new

    WideEvent::Setup::Checker.new(
      root: root,
      runner: runner,
      clock: clock,
      sleeper: sleeper,
      resolver: resolver,
      http_get: http_get,
      client_factory: ->(**_kwargs) { client_double },
      uuid: -> { FIXED_ID },
      wall_clock: -> { Time.utc(2026, 1, 1, 12, 0, 0) }
    )
  end

  # A monotonic clock double: starts at 0, and only ever advances when
  # `advancing_sleeper` tells it to - so a real `sleep` never happens and the
  # test can assert on total elapsed time deterministically.
  class FakeClock
    attr_reader :elapsed

    def initialize
      @elapsed = 0
    end

    def call
      @elapsed
    end

    def advance(seconds)
      @elapsed += seconds
    end
  end

  class AdvancingSleeper
    def initialize(clock)
      @clock = clock
    end

    def call(seconds)
      @clock.advance(seconds)
    end
  end

  # Responds to `kamal version`/`kamal config`/`kamal server exec ...` with
  # canned Results, keyed on the first two argv words - never runs a real
  # subprocess.
  class FakeRunner
    def initialize(kamal_version: "2.12.0", kamal_config_ok: true, kamal_config_stderr: "",
                   host_dir_ok: true, host_dir_stderr: "install: Permission denied\n")
      @kamal_version = kamal_version
      @kamal_config_ok = kamal_config_ok
      @kamal_config_stderr = kamal_config_stderr
      @host_dir_ok = host_dir_ok
      @host_dir_stderr = host_dir_stderr
    end

    def call(argv)
      case argv.first(2)
      when %w[kamal version]
        ok("#{@kamal_version}\n")
      when %w[kamal config]
        @kamal_config_ok ? ok("") : fail_with(@kamal_config_stderr)
      else
        @host_dir_ok ? ok("") : fail_with(@host_dir_stderr)
      end
    end

    private

    def ok(stdout)
      WideEvent::Setup::CommandRunner::Result.new(stdout: stdout, stderr: "", success: true)
    end

    def fail_with(stderr)
      WideEvent::Setup::CommandRunner::Result.new(stdout: "", stderr: stderr, success: false)
    end
  end

  # Stands in for WideEvent::Store::Client: #ingest decodes the gzipped
  # batch it was actually given (so tests can assert on its contents)
  # instead of trusting the caller built it correctly, and #query answers
  # either the row-by-id lookup or the route-stats query based on the SQL
  # text, without ever opening a socket.
  class FakeStoreClient
    attr_reader :ingested_batches

    def initialize(raise_on_ingest: nil, raise_on_query: nil, missing_row: false)
      @ingested_batches = []
      @raise_on_ingest = raise_on_ingest
      @raise_on_query = raise_on_query
      @missing_row = missing_row
    end

    def ingest(gzip_body)
      raise @raise_on_ingest if @raise_on_ingest
      json = JSON.parse(Zlib.gunzip(gzip_body))
      @ingested_batches << json
      WideEvent::Store::Client::Ack.new(protocol_version: 1, batch_id: json["batch_id"], duplicate: false, warnings: [])
    end

    def query(sql, format: :json, deadline: 30)
      raise @raise_on_query if @raise_on_query

      if sql.include?("GROUP BY route")
        return WideEvent::Store::QueryResult.new(columns: %w[route requests p95_ms], rows: [ [ "wide_events:setup:check", 1, 0.0 ] ], truncated: false)
      end

      return WideEvent::Store::QueryResult.new(columns: [], rows: [], truncated: false) if @missing_row

      event = @ingested_batches.last&.dig("events", 0) || {}
      WideEvent::Store::QueryResult.new(
        columns: %w[id occurred_at route duration_ms],
        rows: [ [ event["id"], event["occurred_at"], event.dig("attributes", "http.route.controller"), event["duration_ms"] ] ],
        truncated: false
      )
    end
  end
end
