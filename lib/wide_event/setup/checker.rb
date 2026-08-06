require "yaml"
require "json"
require "zlib"
require "ipaddr"
require "resolv"
require "net/http"
require "openssl"
require "uri"
require "securerandom"
require "stringio"
require "wide_event/setup/command_runner"
require "wide_event/kamal/deploy_editor"
require "wide_event/kamal/secrets_editor"
require "wide_event/store/client"
require "wide_event/store/envelope"
require "wide_event/store/query_result"
require "wide_event/store/formatter"

module WideEvent
  module Setup
    # The single entry point behind `bin/rails wide_events:setup:check`.
    # Re-derives everything it needs from the app's own generated files
    # (config/deploy.yml, .kamal/wide-events-*-token) so the same command is
    # correct with no flags whether it is run before the first `kamal
    # setup`, while waiting on DNS, or against an already-deployed store.
    #
    # Every check that would otherwise touch the outside world - running
    # `kamal`, sleeping, resolving DNS, or calling the store over HTTP - goes
    # through an injected seam (runner/sleeper/resolver/http_get/
    # client_factory) so tests never shell out, sleep for real, or hit a
    # real network.
    class Checker
      MIN_KAMAL_VERSION = WideEvent::Kamal::DeployEditor::MINIMUM_VERSION
      DNS_TIMEOUT_SECONDS = 120
      DNS_POLL_INTERVAL_SECONDS = 3
      RESUME_COMMAND = "bin/rails wide_events:setup:check"
      DEPLOY_YML_RELATIVE_PATH = "config/deploy.yml"
      OPEN_TIMEOUT = 3
      READ_TIMEOUT = 5
      QUERY_DEADLINE = 15

      NETWORK_ERRORS = [
        Net::OpenTimeout, Net::ReadTimeout, Errno::EHOSTUNREACH, Errno::ETIMEDOUT, Errno::ECONNRESET,
        SocketError, OpenSSL::SSL::SSLError, EOFError, IOError
      ].freeze

      ROUTE_STATS_SQL = <<~SQL.strip
        SELECT route, count(*) AS requests, quantile_cont(duration_ms, 0.95) AS p95_ms
        FROM wide_events
        WHERE occurred_at > current_timestamp - INTERVAL 1 DAY
        GROUP BY route
        ORDER BY requests DESC
        LIMIT 20
      SQL

      Result = Struct.new(:state, :checks, :message, keyword_init: true) do
        def success?
          state == "healthy"
        end
      end

      def initialize(root:, runner: CommandRunner.new, clock: -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) },
                     sleeper: ->(seconds) { sleep(seconds) }, resolver: method(:default_resolve),
                     http_get: method(:default_http_get), client_factory: method(:default_client_factory),
                     uuid: -> { SecureRandom.uuid }, wall_clock: -> { Time.now })
        @root = root.to_s
        @runner = runner
        @clock = clock
        @sleeper = sleeper
        @resolver = resolver
        @http_get = http_get
        @client_factory = client_factory
        @uuid = uuid
        @wall_clock = wall_clock
      end

      def run
        checks = {}

        config = load_deploy_config
        unless config[:ok]
          checks[:deploy_config] = false
          return build_result("failed", checks, config[:error])
        end
        checks[:deploy_config] = true

        version = check_kamal_version
        checks[:kamal_version] = version[:ok]
        return build_result("failed", checks, version[:message]) unless version[:ok]

        kamal_config = check_kamal_config
        checks[:kamal_config] = kamal_config[:ok]
        return build_result("failed", checks, kamal_config[:message]) unless kamal_config[:ok]

        directory = check_host_directory(config)
        checks[:host_directory] = directory[:ok]
        return build_result("failed", checks, directory[:message]) unless directory[:ok]

        dns = check_dns(config)
        checks[:dns] = dns[:ok]
        return build_result(dns[:state], checks, dns[:message]) unless dns[:ok]

        endpoint = check_endpoint(config)
        checks[:endpoint] = endpoint[:state] == :healthy
        case endpoint[:state]
        when :ready_to_deploy then return build_result("ready_to_deploy", checks, endpoint[:message])
        when :degraded then return build_result("degraded", checks, endpoint[:message])
        when :failed then return build_result("failed", checks, endpoint[:message])
        end

        synthetic = run_synthetic_round_trip(config)
        checks[:synthetic] = synthetic[:ok]
        return build_result("failed", checks, synthetic[:message]) unless synthetic[:ok]

        build_result("healthy", checks, synthetic[:message])
      end

      private

      def build_result(state, checks, message)
        Result.new(state: state, checks: checks, message: message)
      end

      # ---- config/deploy.yml -----------------------------------------------

      def load_deploy_config
        path = File.join(@root, DEPLOY_YML_RELATIVE_PATH)
        unless File.exist?(path)
          return not_generated_yet("config/deploy.yml was not found")
        end

        parsed = YAML.safe_load(File.read(path), permitted_classes: [], aliases: false)
        accessory = parsed.is_a?(Hash) ? parsed.dig("accessories", "wide_events") : nil
        return not_generated_yet("config/deploy.yml has no wide_events accessory") unless accessory.is_a?(Hash)

        host = accessory["host"]
        hostname = accessory.dig("proxy", "host")
        service = parsed["service"]
        environment = accessory.dig("env", "clear", "WIDE_EVENTS_ENVIRONMENT") || "production"

        if blank?(host) || blank?(hostname) || blank?(service)
          return not_generated_yet("config/deploy.yml's wide_events accessory is missing host, hostname, or service")
        end

        { ok: true, host: host, hostname: hostname, service: service, environment: environment }
      rescue Psych::Exception => e
        { ok: false, error: "config/deploy.yml could not be parsed (#{e.message})" }
      end

      def not_generated_yet(reason)
        { ok: false, error: "#{reason} - run `bin/rails generate wide_events:store` first" }
      end

      def blank?(value)
        value.to_s.strip.empty?
      end

      # ---- Kamal version/config/directory preflights -----------------------

      def check_kamal_version
        result = @runner.call(%w[kamal version])
        unless result.success?
          return { ok: false, message: "could not run `kamal version` (#{result.stderr.to_s.strip}); " \
            "install Kamal #{MIN_KAMAL_VERSION} or newer" }
        end

        version_text = result.stdout.to_s[/\d+\.\d+\.\d+/]
        if version_text.nil?
          return { ok: false, message: "could not parse a version from `kamal version` output " \
            "(#{result.stdout.to_s.strip.inspect})" }
        end

        if Gem::Version.new(version_text) < Gem::Version.new(MIN_KAMAL_VERSION)
          return { ok: false, message: "Kamal #{version_text} is installed; wide_events requires " \
            "#{MIN_KAMAL_VERSION} or newer - upgrade with `gem install kamal -v '>= #{MIN_KAMAL_VERSION}'`" }
        end

        { ok: true }
      end

      def check_kamal_config
        result = @runner.call(%w[kamal config])
        return { ok: true } if result.success?
        { ok: false, message: result.stderr.to_s.strip }
      end

      # Because setting the accessory's data-directory ownership requires
      # root on the store host, this preflights directory creation before
      # `kamal setup` ever boots the accessory - a deploy user who cannot
      # create/chown it stops here with the exact one-time command an
      # administrator needs to run instead of booting a store that will
      # later fail to persist data.
      def check_host_directory(config)
        directory = "/var/lib/#{config[:service]}-wide-events"
        argv = [ "kamal", "server", "exec", "--hosts=#{config[:host]}", "--",
                 "install", "-d", "-m", "0750", "-o", "1000", "-g", "1000", directory ]
        result = @runner.call(argv)
        return { ok: true } if result.success?

        {
          ok: false,
          message: "the deploy user cannot create #{directory} as uid/gid 1000:1000 " \
            "(#{result.stderr.to_s.strip}). Ask an administrator to run this once:\n\n    #{argv.join(" ")}\n"
        }
      end

      # ---- DNS ---------------------------------------------------------------

      # Two different failure modes here, deliberately kept distinct:
      #
      # - The *store host* (the SSH address recorded by the store generator)
      #   doesn't resolve at all. That is a config error - a mistyped
      #   address never starts resolving no matter how long you wait - so it
      #   is "failed", not resumable, and points at the host setting.
      # - The *telemetry hostname* hasn't propagated to point at the store
      #   host yet. That is expected and resumable, so a timeout here (after
      #   polling every DNS_POLL_INTERVAL_SECONDS for up to
      #   DNS_TIMEOUT_SECONDS monotonic seconds) is "needs_dns" with the
      #   record still needed and the exact resume command. Generated files
      #   are never touched in either case.
      def check_dns(config)
        target_addresses = resolve_target_addresses(config[:host])
        if target_addresses.empty?
          return { ok: false, state: "failed",
                    message: "could not resolve the store host #{config[:host]} to an address - " \
                      "check the host set on the wide_events accessory in config/deploy.yml " \
                      "(re-run `bin/rails generate wide_events:store --host=...` if it's wrong)" }
        end

        started = @clock.call
        loop do
          resolved = Array(@resolver.call(config[:hostname]))
          return { ok: true } if (resolved & target_addresses).any?

          elapsed = @clock.call - started
          if elapsed >= DNS_TIMEOUT_SECONDS
            return { ok: false, state: "needs_dns", message: dns_timeout_message(config, target_addresses) }
          end

          @sleeper.call(DNS_POLL_INTERVAL_SECONDS)
        end
      end

      def dns_timeout_message(config, target_addresses)
        target = target_addresses.first
        record_type = target.include?(":") ? "AAAA" : "A"
        "DNS has not resolved after #{DNS_TIMEOUT_SECONDS} seconds. Create a #{record_type} record: " \
          "#{config[:hostname]} -> #{target}. Once it propagates, run `#{RESUME_COMMAND}` to resume - " \
          "generated files are unchanged and nothing else needs to be redone."
      end

      def resolve_target_addresses(host)
        return [ host ] if ip_address?(host)
        Array(@resolver.call(host))
      end

      def ip_address?(value)
        IPAddr.new(value)
        true
      rescue IPAddr::Error
        false
      end

      def default_resolve(hostname)
        Resolv::DNS.open { |dns| dns.getaddresses(hostname).map(&:to_s) }
      rescue Resolv::ResolvError, Resolv::ResolvTimeout
        []
      end

      # ---- Deployed endpoint (/up) --------------------------------------------

      def check_endpoint(config)
        uri = URI("https://#{config[:hostname]}/up")
        response = @http_get.call(uri)

        if response[:error] == :connection_refused || response[:status] == 404
          return { state: :ready_to_deploy,
                    message: "#{config[:hostname]} is not serving the store yet - run `kamal setup`, then " \
                      "`#{RESUME_COMMAND}` again to verify it." }
        end

        if response[:error]
          return { state: :failed,
                    message: "could not reach https://#{config[:hostname]}/up (#{response[:message]})" }
        end

        unless response[:status] == 200
          return { state: :failed, message: "https://#{config[:hostname]}/up returned HTTP #{response[:status]}" }
        end

        body = parse_json(response[:body])
        unless body.is_a?(Hash)
          return { state: :failed, message: "https://#{config[:hostname]}/up returned an unparsable response" }
        end

        protocol_version = body["protocol_version"]
        unless protocol_version == 1
          return { state: :failed, message: "the store speaks protocol #{protocol_version.inspect}; " \
            "wide_events expects 1 - upgrade the gem or the pinned store image" }
        end

        unless body["write_ready"] == true
          reason = body["degradation_reason"] || "unknown reason"
          return { state: :degraded,
                    message: "#{config[:hostname]} is routable but not accepting writes (#{reason}) - " \
                      "check `wide_events_store_status` on the store (`kamal telemetry`)" }
        end

        { state: :healthy }
      end

      def parse_json(body)
        JSON.parse(body.to_s)
      rescue JSON::ParserError, TypeError
        nil
      end

      def default_http_get(uri)
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = uri.scheme == "https"
        http.open_timeout = OPEN_TIMEOUT
        http.read_timeout = READ_TIMEOUT
        response = http.get(uri.request_uri)
        { status: response.code.to_i, body: response.body }
      rescue Errno::ECONNREFUSED
        { error: :connection_refused }
      rescue *NETWORK_ERRORS => e
        { error: :network, message: e.message }
      end

      # ---- Synthetic round trip -----------------------------------------------

      def run_synthetic_round_trip(config)
        ingest_token = read_token(WideEvent::Kamal::SecretsEditor::INGEST_TOKEN_PATH)
        query_token = read_token(WideEvent::Kamal::SecretsEditor::QUERY_TOKEN_PATH)
        unless ingest_token && query_token
          return { ok: false, message: "missing #{WideEvent::Kamal::SecretsEditor::INGEST_TOKEN_PATH} or " \
            "#{WideEvent::Kamal::SecretsEditor::QUERY_TOKEN_PATH} - run `bin/rails generate wide_events:store` again" }
        end

        client = @client_factory.call(url: "https://#{config[:hostname]}", ingest_token: ingest_token, query_token: query_token)

        id = @uuid.call
        event_json = WideEvent::Store::Envelope.build(
          { "duration_ms" => 0.0, "http.route.controller" => "wide_events:setup:check" },
          clock: @wall_clock, uuid: -> { id }, env: {}
        )
        client.ingest(Zlib.gzip(build_batch(event_json, config)))

        row_result = client.query("SELECT id, occurred_at, route, duration_ms FROM wide_events WHERE id = '#{id}'",
          format: :json, deadline: QUERY_DEADLINE)
        if row_result.rows.empty?
          return { ok: false, message: "sent a synthetic event (#{id}) but could not read it back by id" }
        end

        message = render_synthetic_result(client, row_result)
        { ok: true, message: message }
      rescue WideEvent::Store::Client::Error => e
        { ok: false, message: "synthetic round trip failed: #{e.message}" }
      end

      def build_batch(event_json, config)
        envelope = {
          "protocol_version" => 1,
          "batch_id" => @uuid.call,
          "service" => config[:service],
          "environment" => config[:environment],
          "sent_at" => @wall_clock.call.utc.iso8601(3),
          "dropped_events_since_last_batch" => 0,
          "events" => [ JSON.parse(event_json) ]
        }
        JSON.generate(envelope)
      end

      def render_synthetic_result(client, row_result)
        io = StringIO.new
        io.puts "store is healthy; captured synthetic event:"
        WideEvent::Store::Formatter.table(row_result, io: io)
        io.puts
        io.puts "route count / p95 latency (last 24h):"
        begin
          stats = client.query(ROUTE_STATS_SQL, format: :json, deadline: QUERY_DEADLINE)
          WideEvent::Store::Formatter.table(stats, io: io)
        rescue WideEvent::Store::Client::Error => e
          io.puts "(could not run the route-count/p95 query: #{e.message})"
        end
        io.string
      end

      def read_token(relative_path)
        path = File.join(@root, relative_path)
        return nil unless File.exist?(path)
        content = File.read(path).strip
        content.empty? ? nil : content
      end

      def default_client_factory(url:, ingest_token:, query_token:)
        WideEvent::Store::Client.new(url: url, ingest_token: ingest_token, query_token: query_token)
      end
    end
  end
end
