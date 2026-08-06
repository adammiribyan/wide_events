# Wide Events

[![Gem Version](https://img.shields.io/gem/v/wide_events)](https://rubygems.org/gems/wide_events)
[![CI](https://github.com/adammiribyan/wide_events/actions/workflows/ci.yml/badge.svg)](https://github.com/adammiribyan/wide_events/actions/workflows/ci.yml)

One wide telemetry event per Rails request or job execution, in a database you own.

Wide Events collects everything your app knows about each unit of work (route, user, account, build SHA, query counts, cache hits, feature flags, phase timings, errors) into one flat, high-cardinality event. Every request becomes one row, and every question becomes one query against storage you run.

Three places to put those rows, in order of how much you have to operate:

- **The bundled store.** A DuckDB service the gem ships as a Kamal accessory: one generator, one `kamal setup`, and `kamal telemetry` gives you a SQL prompt against production. No collector, no OTel SDK.
- **Any OTLP backend.** The event goes onto the OpenTelemetry root span you already export. ClickHouse with HyperDX is a proven pairing (both open source).
- **Your log store.** The log sink emits one JSON event per request.

That format matters more now that agents build with you. Telemetry stops being something humans only glance at and becomes something software queries repeatedly: an agent can instrument a feature, deploy it, and verify it in production. One row per request is a dense way to feed production behavior back into a context window, and owning the storage avoids metered per-event and per-query observability fees.

## Install

```ruby
# Gemfile
gem "wide_events"
gem "useragent"  # optional: parses user_agent.browser/os/platform from the raw UA string
```

```bash
bin/rails generate wide_events:install   # initializer + attribute registry + AGENTS.md section + test helper wiring
bin/rails generate wide_events:skills    # agent skills into .claude/skills/
```

## See an event in 60 seconds

Wide events are off unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set (the development log tells you so at boot). To see one immediately, no tracing required, enable the log sink in `config/initializers/wide_events.rb`:

```ruby
WideEvent.configure do |config|
  config.enabled = true
  config.sink = :log
end
```

Hit any route, then:

```bash
grep '"main":true' log/development.log
```

One JSON object per request: route, status, timings, query counts, everything.

## Production in ten minutes: the bundled store

The gem ships its own telemetry database: a single Go process around DuckDB, deployed as a Kamal accessory on a VM you own. No collector, no OTel SDK, no vendor account. One `bin/rails generate` writes the accessory, `kamal setup` deploys it, and `kamal telemetry` opens a SQL prompt against production.

```bash
bin/rails generate wide_events:store --host=203.0.113.20 --hostname=telemetry.example.com --retention-days=30
```

`--host` is the SSH address of the VM that will run the store. `--hostname` is the public DNS name the app and your SQL prompt reach it through; point an A record at the host now, so propagation happens while you finish the rest. The generator edits `config/deploy.yml` and writes two 64-hex tokens to `.kamal/wide-events-ingest-token` and `.kamal/wide-events-query-token` (mode 0600, gitignored, wired into `.kamal/secrets`). Every edit is marked and idempotent, so re-running it is safe. If your `config/deploy.yml` is shaped in a way the editor won't touch, it refuses and prints the complete block to merge by hand.

Here's the accessory it adds:

```yaml
accessories:
  wide_events:
    image: .2.0
    host: 203.0.113.20

    proxy:
      host: telemetry.example.com
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
        WIDE_EVENTS_SERVICE: myapp
        WIDE_EVENTS_ENVIRONMENT: production
        WIDE_EVENTS_RETENTION_DAYS: 30
      secret:
        - WIDE_EVENTS_INGEST_TOKEN
        - WIDE_EVENTS_QUERY_TOKEN

    directories:
      - local: /var/lib/myapp-wide-events
        remote: /var/lib/wide-events
        mode: "0750"
        owner: "1000:1000"

    options:
      cpus: 2
      memory: 2g
```

The structured `directories` entry is why the generator also raises `minimum_version` to `2.10.0`: older Kamal releases only accept a `local:remote` string and would mount the DuckDB volume without the mode and owner the container's uid/gid 1000 needs. On the app side it sets `WIDE_EVENTS_URL`, `WIDE_EVENTS_SERVICE`, and `WIDE_EVENTS_ENVIRONMENT` in `env.clear` and binds both tokens as secrets. Setting `WIDE_EVENTS_URL` is all it takes to switch the sink: the gem enables itself and sends to the store instead of an OTel span.

Then, in order:

```bash
bin/rails wide_events:setup:check   # Kamal version, config, host directory, DNS, endpoint
kamal setup
bin/rails wide_events:setup:check   # sends one synthetic event and queries it back
```

`wide_events:setup:check` is the same command at every stage. It reports `failed`, `needs_dns`, `ready_to_deploy`, `degraded`, or `healthy`, and `needs_dns` is expected while an A record propagates: nothing is lost, run it again later. On a healthy store it writes one synthetic event, reads it back by id, and prints a route/p95 query, so you know ingest token, query token, TLS, and DuckDB all work before real traffic arrives.

Working with an agent? `bin/rails generate wide_events:skills` installs the `setting-up-wide-events-store` skill, which walks an agent through this whole flow, including what to say about DNS waits and backup limitations.

## Querying the store

```bash
kamal telemetry                                     # interactive SQL prompt
echo "SELECT count(*) FROM wide_events" | kamal telemetry
```

Output is a text table by default. `bin/rails wide_events:sql` reads `WIDE_EVENTS_FORMAT` (`table`, `json`, or `csv`) from the environment it runs in, so set it in the app container, not in your local shell.

`route`, `job_class`, `status`, `error`, `request_id`, `duration_ms`, `kind`, `deployment`, `source`, `occurred_at`, and `received_at` are real columns. Everything else the app set lives in an `attributes` VARIANT column, read as `attributes['key.name']` and cast to the type you want:

```sql
SELECT route,
       count(*) AS requests,
       quantile_cont(duration_ms, 0.5) AS p50_ms,
       quantile_cont(duration_ms, 0.95) AS p95_ms
FROM wide_events
WHERE occurred_at > current_timestamp - INTERVAL 1 DAY
GROUP BY route
ORDER BY p95_ms DESC
LIMIT 15
```

```sql
-- Exceptions that escaped without a named handled failure
SELECT route, attributes['exception.type']::VARCHAR AS exception_type, count(*) AS occurrences
FROM wide_events
WHERE error
  AND coalesce(attributes['exception.slug']::VARCHAR, '') = ''
  AND occurred_at > current_timestamp - INTERVAL 1 DAY
GROUP BY route, exception_type
ORDER BY occurrences DESC
```

`SELECT * FROM wide_events_store_status` is the one-row operational view: rows in the last hour and day, oldest and newest event, database and free-disk bytes, queued ingest bytes, rejected batches, reported dropped events, write readiness and why, last checkpoint/retention/backup times and errors, and the store, protocol, and DuckDB versions. Read it first whenever a query returns nothing.

Queries are read-only and bounded: two run at a time with four more allowed to wait, 15 seconds of execution, 2 seconds of queueing, 10,000 rows, and 10 MiB of response. A statement that isn't a single `SELECT` is rejected by DuckDB's own parser before it runs.

## Two tokens, two jobs

The ingest token writes and the query token reads; the store refuses to start if they're equal. Sharing SQL access with a teammate or an agent means handing over `.kamal/wide-events-query-token` only, and never the ability to forge events. Both are 64 hex characters, generated once by the generator and reused on every later run.

## Sizing, retention, and what happens when the disk fills

One event is roughly 1 KB before compression. At 30-day retention, 100 requests/second is about 250 GB of raw rows before DuckDB's columnar compression, which typically lands 5-10x smaller; 10 requests/second fits comfortably in tens of gigabytes. Start with a 100 GB volume and 2 GB of RAM, watch `database_file_bytes` in the status view for a week, and resize from evidence.

Retention runs once a day and deletes events older than `WIDE_EVENTS_RETENTION_DAYS`. The store reserves the greater of 2 GiB or 10% of the volume as a write floor. Below it, ingest answers `507` and the sender holds its batches, but `/up` still returns 200 and queries keep working, so you can diagnose a full disk from the store itself rather than by SSH.

Loss is bounded and reported, never silent. Each app process queues up to 1,000 events or 8 MiB; past that, the oldest are dropped and the count travels with the next batch into `wide_events_loss_reports`, so a gap in the data is visible as a number. Batches are idempotent by `batch_id`, so a response lost after the store committed is retried and deduplicated instead of double-counted.

## Backups, upgrades, and rollback

A verified local backup runs daily and keeps the three newest copies in `backups/` next to the database. Take one on demand with `kamal wide-events-backup`, which prints the path it wrote.

Backups are local only. They protect against a bad upgrade or a mistaken `DELETE`, not against losing the VM or its volume; there's no off-host copy in v1. If that matters for your deployment, snapshot the host volume on your provider's schedule. Restoring is manual: stop the accessory, replace `/var/lib/wide-events/events.duckdb` with the backup file, and start it again. Nothing but the store process may open the database; the admin socket exists so backups happen through the running owner instead of a second process touching the file.

Upgrading is a gem bump. Each release publishes `ghcr.io/adammiribyan/wide-events-store:<version>` for linux/amd64 and linux/arm64. Re-run `bin/rails generate wide_events:store` with the same options to rewrite the pinned image, then `kamal accessory reboot wide_events` to pull it and restart. There is no `latest` tag on purpose, so an already-deployed accessory can't change under you on its next boot. Migrations are forward-only and run at startup: a store that finds a schema newer than it understands refuses to open rather than corrupting it. To roll back, pin the previous version by hand, and take a backup first if the newer release applied a migration.

## Production setup with OpenTelemetry

Prefer an existing OTel pipeline, or already run ClickHouse? Skip the store. The `:otel` sink writes the event onto the current OTel root span, which is the span OTel's Rack instrumentation opens at the top of the middleware stack. That means production needs an OpenTelemetry SDK configured; the gem detects it and wires itself after your initializers run. Minimal setup:

```ruby
# Gemfile
gem "opentelemetry-sdk"
gem "opentelemetry-exporter-otlp"
gem "opentelemetry-instrumentation-rails"
gem "opentelemetry-instrumentation-rack"
gem "opentelemetry-instrumentation-pg"        # enables stats.postgres_query_*
gem "opentelemetry-instrumentation-net_http"  # enables stats.http_call_*
```

```ruby
# config/initializers/opentelemetry.rb
return if ENV["OTEL_EXPORTER_OTLP_ENDPOINT"].to_s.empty?

require "opentelemetry/sdk"
require "opentelemetry/exporter/otlp"
require "opentelemetry/instrumentation/rails"
require "opentelemetry/instrumentation/rack"
require "opentelemetry/instrumentation/pg"
require "opentelemetry/instrumentation/net/http"

OpenTelemetry::SDK.configure(&:use_all)
```

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318   # your collector or backend
OTEL_SERVICE_NAME=myapp
OTEL_RESOURCE_ATTRIBUTES=deployment.environment=production
```

With the endpoint set, wide events enable themselves: the railtie inserts the middleware directly below `ActionDispatch::Executor` (the executor clears the per-request store, so placement matters), instruments `ActiveJob::Base`, and registers the span counter and notification subscribers via `WideEvent.install!` after your initializers run. Without Rails, call `WideEvent.install!` yourself after configuring the SDK.

## What you get per event

- `http.request.id`, `http.response.status_code`, `http.route.controller`, request body size, parsed `user_agent.*`
- `db.duration_ms` and `view.duration_ms` (Rails' own measurements)
- `stats.postgres_query_count` / `_duration_ms` and `stats.http_call_count` / `_duration_ms`, rolled up from OTel child spans (scope map is configurable)
- `cache.<prefix>` hit/miss booleans, capped per event
- `job.class`, `job.queue`, `job.queue_latency_ms`, `job.executions`, `job.scheduled` (Solid Queue recurring detection built in, detector pluggable)
- `error`, `exception.type`, `exception.message`, `uptime_sec`, `main: true`

A request and the jobs it enqueues are separate units of work. Synchronous enqueue work contributes to the request's total duration; each job execution emits its own event carrying `job.queue_latency_ms` (time spent waiting in the queue), so queue pressure is visible per job rather than folded into request latency.

## Adding your own attributes

```ruby
WideEvent.set("report.id" => report.id, "report.format" => "pdf")
WideEvent.phase("pdf_render") { render_pdf }         # -> pdf_render.duration_ms
```

Attribute writes are safe no-ops outside a unit of work; `phase` still executes and returns the wrapped block. Telemetry failures never raise into app code, so a telemetry bug cannot fail a request or job.

Errors come in two shapes, and the difference is the point:

```ruby
# Handled: you rescued it, you name it
rescue Vendor::ApiError => e
  WideEvent.error!(slug: "err-vendor-sync-failed", exception: e, expected: true)

# Unhandled: any exception that escapes gets error: true with NO slug, automatically
```

Filtering for `error = true`, a missing `exception.slug`, and a present `exception.type` isolates exception paths that escaped without a named handled failure. Treat the results as a prioritized investigation queue; add a rescue and `error!` only when handling the exception is intentional.

## The registry is a schema, not a wiki page

Every attribute is declared in `config/wide_event/registry.yml` (the gem's own attributes are pre-registered; globs like `feature_flag.*` cover dynamic families):

```yaml
report.format:
  type: string
  set_by: ReportsController#create
  pii: none
  notes: pdf or csv
```

The workflow, end to end:

1. Set the attribute in code, declare it in `registry.yml`, same change.
2. Assert it in a test with `assert_wide_event`.
3. Strict mode (`config.strict = Rails.env.test?`, the generated default) records any undeclared attribute the suite sees; `assert_registered_wide_event_attributes` fails on them.
4. CI runs `bin/rails wide_events:registry:check` to validate the file.
5. `bin/rails wide_events:registry:docs` generates `docs/wide-events-registry.md` from it, so documentation can't drift from reality.

```yaml
# .github/workflows/ci.yml (host app)
- run: bin/rails wide_events:registry:check
- run: bin/rails test
```

## Testing

The install generator wires this into `test/test_helper.rb` (or tells you how):

```ruby
require "wide_event/test_helper"

class ActiveSupport::TestCase
  include WideEvent::TestHelper
  teardown { assert_registered_wide_event_attributes }
end
```

```ruby
test "report rendering is instrumented" do
  assert_wide_event("pdf_render.duration_ms", "report.format" => "pdf") do
    Report.new(format: "pdf").render
  end
end

test "job emits one wide event" do
  events = capture_wide_events { ExportJob.perform_now }
  assert_equal 1, events.length
end
```

The teardown pattern makes the registry check work with parallel test workers: violation tracking is per process, and checking (then resetting) after every test means the test that set an undeclared attribute is the one that fails, in whichever worker it ran.

## When events are emitted

| Environment | Typical setup | Result |
|---|---|---|
| Production / staging | `WIDE_EVENTS_URL` set (the store generator sets it) | Enabled automatically, `:store` sink, batched over HTTPS |
| Production / staging | `OTEL_EXPORTER_OTLP_ENDPOINT` set | Enabled automatically, `:otel` sink, events on root spans |
| Development | `config.enabled = true`, `config.sink = :log` | One JSON line per request in the log |
| Test | Enabled via the test helper's capture, or strict mode for registry tracking | `capture_wide_events` collects them |

`WIDE_EVENTS_URL` wins over `OTEL_EXPORTER_OTLP_ENDPOINT` when both are set.

`WideEvent.install!` runs once at boot, only if enabled at that point. Flipping `config.enabled` at runtime gates the middleware and job hook (they check per request), but the span counter and subscribers only register at boot, so treat enablement as a boot-time decision.

## Built for the agent loop

`bin/rails generate wide_events:skills` installs three agent skills:

- **instrumenting-wide-events**: the write path. Naming conventions, when to use `set` vs `phase` vs `error!`, the registry workflow, PII rules, test assertions.
- **debugging-with-wide-events**: the read path. Symptom-to-query workflow against the DuckDB store, ClickHouse/HyperDX, or JSON logs, plus the standing queries worth running.
- **setting-up-wide-events-store**: the deploy path. Generator, DNS, `kamal setup`, and the checker states, end to end.

Skills land in `.claude/skills/`, which Claude Code and Claude-compatible agents read. Other agents (Cursor, etc.) read the pointer the install generator appends to `AGENTS.md`; the SKILL.md files are plain markdown, so copy or symlink them wherever your tooling looks (`.cursor/rules/`, docs, a system prompt).

## Configuration

```ruby
WideEvent.configure do |config|
  config.enabled = ENV["WIDE_EVENTS_URL"].present? || ENV["OTEL_EXPORTER_OTLP_ENDPOINT"].present?  # default
  config.sink = :store              # :store, :otel, :log, or any object responding to flush(attrs)
  config.store_url = ENV["WIDE_EVENTS_URL"]                    # HTTPS, except an explicit loopback URL
  config.store_service = ENV["WIDE_EVENTS_SERVICE"]
  config.store_environment = ENV["WIDE_EVENTS_ENVIRONMENT"]
  config.store_ingest_token = ENV["WIDE_EVENTS_INGEST_TOKEN"]
  config.store_query_token = ENV["WIDE_EVENTS_QUERY_TOKEN"]
  config.logger = nil               # :log sink target; defaults to Rails.logger
  config.strict = Rails.env.test?   # track attributes against the registry
  config.max_cache_attrs = 10
  config.span_scopes = {            # OTel instrumentation scope -> stats.* name
    "OpenTelemetry::Instrumentation::PG" => "postgres_query",
    "OpenTelemetry::Instrumentation::Net::HTTP" => "http_call"
  }
  # Optional overrides; defaults detect Solid Queue recurring executions and report via OpenTelemetry.handle_error:
  # config.scheduled_job_detector = ->(job) { MyApp.scheduled_job?(job) }
  # config.error_handler = ->(exception, message) { Rails.error.report(exception, context: { message: message }) }
end
```

Store sink notes: every store setting defaults to its `WIDE_EVENTS_*` environment variable, which the Kamal generator already sets, so most apps configure nothing here. Delivery happens on one background thread per process; application threads only append to a bounded in-memory queue and never block on the network. The sender is fork-aware, so Puma and Sidekiq workers each get their own queue instead of inheriting the parent's.

Log sink notes: events go to `config.logger` (or `Rails.logger`) at info level as single-line JSON. Point it at a dedicated logger (`config.logger = Logger.new("log/wide_events.log")`) to keep them out of your main log, and leave the sink off in the test environment unless you want one JSON line per test request; `capture_wide_events` is the intended test-side tap.

## Querying an OTLP backend: ClickHouse + HyperDX

[docs/clickhouse-hyperdx.md](https://github.com/adammiribyan/wide_events/blob/main/docs/clickhouse-hyperdx.md) has a local quickstart (one container) and a query cookbook. The flavor:

```sql
SELECT SpanAttributes['http.route.controller'] AS controller,
       count() AS requests,
       round(quantile(0.5)(Duration/1e6)) AS p50_ms,
       round(avg(toFloat64OrZero(SpanAttributes['stats.postgres_query_count']))) AS avg_queries
FROM otel_traces
WHERE SpanAttributes['main'] = 'true'
GROUP BY controller ORDER BY p50_ms DESC LIMIT 15
```

## In production

Wide Events was extracted from a multi-tenant Rails app subject to healthcare privacy rules, where it runs in staging and production today: one row per request and job execution, exported over OTLP into self-hosted ClickStack. A request that runs a hybrid search and drafts a reply with an LLM still lands as one event, carrying the account, the feature flags evaluated, search quality (semantic hits kept, top cosine similarity), the model, tokens, latency, and cost, the Postgres query count, and an error slug for anything rescued along the way. [docs/production-example.md](https://github.com/adammiribyan/wide_events/blob/main/docs/production-example.md) shows a full anonymized event, the instrumentation patterns behind it, and two standing queries taken from that deployment.

## Conventions

Flat keys, dot namespaces, snake_case leaves. Durations end in `_duration_ms`, counts in `_count`, booleans read as assertions, timestamps serialize to RFC 3339. Apply your deployment's privacy policy to every identifier: opaque IDs are preferable to names or emails but should still be registered as `pii: opaque_id`. Exclude request params and free text, or flag them as `pii: review` for explicit handling.

## Development

```bash
bin/setup                 # bundle install + appraisal gemfiles
bundle exec rake test     # run the suite
bin/rubocop               # lint (rubocop-rails-omakase)
cd store && go test ./... # the DuckDB store service
```

The CI matrix runs the suite across Ruby 3.2 to 4.0 and Rails 7.1 to main via [Appraisal](https://github.com/thoughtbot/appraisal); run a specific combination locally with `BUNDLE_GEMFILE=gemfiles/rails_7_1.gemfile bundle exec rake test`. [CONTRIBUTING.md](https://github.com/adammiribyan/wide_events/blob/main/CONTRIBUTING.md) covers the store's Go, Docker, cross-runtime integration, and Kamal compatibility commands. Releases go out with `bin/release <version>`, which also publishes the matching store image.

## Requirements

Ruby >= 3.2, Rails >= 7.1 (activesupport and rack are the only hard dependencies; opentelemetry-sdk and useragent are optional and detected at runtime).

The bundled store needs a Linux VM with Docker and Kamal >= 2.10.0. Images are published for linux/amd64 and linux/arm64.

## License

MIT.
