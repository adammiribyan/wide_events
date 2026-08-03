# Wide Events

[![Gem Version](https://img.shields.io/gem/v/wide_events)](https://rubygems.org/gems/wide_events)
[![CI](https://github.com/adammiribyan/wide_events/actions/workflows/ci.yml/badge.svg)](https://github.com/adammiribyan/wide_events/actions/workflows/ci.yml)

One wide telemetry event per Rails request or job execution, in a database you own.

Wide Events collects everything your app knows about each unit of work (route, user, account, build SHA, query counts, cache hits, feature flags, phase timings, errors) into one flat, high-cardinality event on the OpenTelemetry root span you already export. Every request becomes one row. Every question becomes one query against storage you run: ClickHouse with HyperDX on top is a proven pairing (both open source), any OTLP backend works, and a log-line sink emits one JSON event per request if you'd rather not run tracing at all.

That format matters more now that agents build with you. Telemetry stops being something humans glance at and becomes something software queries in a loop: an agent that instruments a feature, deploys it, and verifies it in production will hit your observability stack fifty times before lunch. One row per request is the densest way to feed production behavior back into a context window, and owning the storage means the loop has no price per iteration.

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

## Production setup with OpenTelemetry

The default sink writes the event onto the current OTel root span, which is the span OTel's Rack instrumentation opens at the top of the middleware stack. That means production needs an OpenTelemetry SDK configured; the gem detects it and wires itself after your initializers run. Minimal setup:

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

A request and the jobs it enqueues are separate units of work: the request's wide event records the enqueue inside its timings, and each job execution emits its own event carrying `job.queue_latency_ms` (time spent waiting in the queue), so queue pressure is visible per job, not smeared into request latency.

## Adding your own attributes

```ruby
WideEvent.set("report.id" => report.id, "report.format" => "pdf")
WideEvent.phase("pdf_render") { render_pdf }         # -> pdf_render.duration_ms
```

Every call is a safe no-op outside a unit of work and never raises into app code. A telemetry bug cannot fail a request or a job.

Errors come in two shapes, and the difference is the point:

```ruby
# Handled: you rescued it, you name it
rescue Vendor::ApiError => e
  WideEvent.error!(slug: "err-vendor-sync-failed", exception: e, expected: true)

# Unhandled: any exception that escapes gets error: true with NO slug, automatically
```

That makes `error = true AND exception.slug IS NULL` a standing query: every row is a failure nobody wrote a rescue for, which is a permanent, prioritized to-do list of rescues worth instrumenting.

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
| Production / staging | `OTEL_EXPORTER_OTLP_ENDPOINT` set | Enabled automatically, `:otel` sink, events on root spans |
| Development | `config.enabled = true`, `config.sink = :log` | One JSON line per request in the log |
| Test | Enabled via the test helper's capture, or strict mode for registry tracking | `capture_wide_events` collects them |

`WideEvent.install!` runs once at boot, only if enabled at that point. Flipping `config.enabled` at runtime gates the middleware and job hook (they check per request), but the span counter and subscribers only register at boot, so treat enablement as a boot-time decision.

## Built for the agent loop

`bin/rails generate wide_events:skills` installs two agent skills:

- **instrumenting-wide-events**: the write path. Naming conventions, when to use `set` vs `phase` vs `error!`, the registry workflow, PII rules, test assertions.
- **debugging-with-wide-events**: the read path. Symptom-to-query workflow against ClickHouse/HyperDX or JSON logs, plus the standing queries worth running.

Skills land in `.claude/skills/`, which Claude Code and Claude-compatible agents read. Other agents (Cursor, etc.) read the pointer the install generator appends to `AGENTS.md`; the SKILL.md files are plain markdown, so copy or symlink them wherever your tooling looks (`.cursor/rules/`, docs, a system prompt).

## Configuration

```ruby
WideEvent.configure do |config|
  config.enabled = ENV["OTEL_EXPORTER_OTLP_ENDPOINT"].present?  # default
  config.sink = :otel               # :otel, :log, or any object responding to flush(attrs)
  config.logger = nil               # :log sink target; defaults to Rails.logger
  config.strict = Rails.env.test?   # track attributes against the registry
  config.max_cache_attrs = 10
  config.span_scopes = {            # OTel instrumentation scope -> stats.* name
    "OpenTelemetry::Instrumentation::PG" => "postgres_query",
    "OpenTelemetry::Instrumentation::Net::HTTP" => "http_call"
  }
  config.scheduled_job_detector = ->(job) { ... }  # default detects Solid Queue recurring executions
  config.error_handler = ->(exception, message) { ... }  # default reports via OpenTelemetry.handle_error
end
```

Log sink notes: events go to `config.logger` (or `Rails.logger`) at info level as single-line JSON. Point it at a dedicated logger (`config.logger = Logger.new("log/wide_events.log")`) to keep them out of your main log, and leave the sink off in the test environment unless you want one JSON line per test request; `capture_wide_events` is the intended test-side tap.

## Querying: ClickHouse + HyperDX

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

Flat keys, dot namespaces, snake_case leaves. Durations end in `_duration_ms`, counts in `_count`, booleans read as assertions, timestamps serialize to RFC 3339. Opaque ids are fine; names, emails, and request params are not, and anything that could quote user input is flagged `pii: review` in the registry.

## Development

```bash
bin/setup                 # bundle install + appraisal gemfiles
bundle exec rake test     # run the suite
bin/rubocop               # lint (rubocop-rails-omakase)
```

The CI matrix runs the suite across Ruby 3.2 to 4.0 and Rails 7.1 to main via [Appraisal](https://github.com/thoughtbot/appraisal); run a specific combination locally with `BUNDLE_GEMFILE=gemfiles/rails_7_1.gemfile bundle exec rake test`. Releases go out with `bin/release <version>`.

## Requirements

Ruby >= 3.2, Rails >= 7.1 (activesupport and rack are the only hard dependencies; opentelemetry-sdk and useragent are optional and detected at runtime).

## License

MIT.
