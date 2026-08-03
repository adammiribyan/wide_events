# wide_events

One wide telemetry event per Rails request or job execution, in a database you own.

wide_events collects everything your app knows about each unit of work (route, user, account, build SHA, query counts, cache hits, feature flags, phase timings, errors) into one flat, high-cardinality event on the OpenTelemetry root span you already export. Every request becomes one row. Every question becomes one query against storage you run: ClickHouse with HyperDX on top is a proven pairing (both open source), any OTLP backend works, and a log-line sink emits one JSON event per request if you'd rather not run tracing at all.

That format matters more now that agents build with you. Telemetry stops being something humans glance at and becomes something software queries in a loop: an agent that instruments a feature, deploys it, and verifies it in production will hit your observability stack fifty times before lunch. One row per request is the densest way to feed production behavior back into a context window, and owning the storage means the loop has no price per iteration.

## Install

```ruby
# Gemfile
gem "wide_events"
```

```bash
bin/rails generate wide_events:install   # initializer + attribute registry + AGENTS.md section
bin/rails generate wide_events:skills    # agent skills into .claude/skills/
```

Wide events turn on wherever `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Dev and test pay nothing.

## What you get per event

The gem records the generic attributes on every request and job:

- `http.request.id`, `http.response.status_code`, `http.route.controller`, request body size, parsed `user_agent.*`
- `db.duration_ms` and `view.duration_ms` (Rails' own measurements)
- `stats.postgres_query_count` / `_duration_ms` and `stats.http_call_count` / `_duration_ms`, rolled up from OTel child spans (scope map is configurable)
- `cache.<prefix>` hit/miss booleans, capped per event
- `job.class`, `job.queue`, `job.queue_latency_ms`, `job.executions`, `job.scheduled` (Solid Queue recurring detection built in, detector pluggable)
- `error`, `exception.type`, `exception.message`, `uptime_sec`, `main: true`

Your code adds the parts only it knows:

```ruby
WideEvent.set("report.id" => report.id, "report.format" => "pdf")
WideEvent.phase("pdf_render") { render_pdf }         # -> pdf_render.duration_ms
WideEvent.error!(slug: "err-export-source-missing", exception: e, expected: true)
```

Every call is a safe no-op outside a unit of work and never raises into app code. A telemetry bug cannot fail a request or a job.

"Something is slow" turns into "the reports route is slow for account 4218 on build f3a91c, and it's running 742 queries": one query in HyperDX for you, one SQL call for your agent.

## The registry is a schema, not a wiki page

Every attribute is declared in `config/wide_event/registry.yml`:

```yaml
report.format:
  type: string
  set_by: ReportsController#create
  pii: none
  notes: pdf or csv
```

The gem registers its own attributes by default; globs (`feature_flag.*`) cover dynamic families. With `config.strict = true` in the test environment, the suite records every undeclared attribute it sees, and `assert_registered_wide_event_attributes` fails on them. `rake wide_events:registry:check` validates the file; `rake wide_events:registry:docs` generates the human-readable registry doc from it, so documentation can't drift from reality.

## Testing

```ruby
# test_helper.rb
require "wide_event/test_helper"
class ActiveSupport::TestCase
  include WideEvent::TestHelper
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

## Built for the agent loop

`bin/rails generate wide_events:skills` installs two agent skills:

- **instrumenting-wide-events**: the write path. Naming conventions, when to use `set` vs `phase` vs `error!`, the registry workflow, PII rules, test assertions.
- **debugging-with-wide-events**: the read path. Symptom-to-query workflow against ClickHouse/HyperDX or JSON logs, plus the standing queries worth running: `error = true AND exception.slug IS NULL` is a permanent, queryable to-do list of rescues nobody instrumented.

The install generator also appends a wide-events section to `AGENTS.md`, so every future session knows the instrumentation exists and how to extend it.

## Configuration

```ruby
WideEvent.configure do |config|
  config.enabled = ENV["OTEL_EXPORTER_OTLP_ENDPOINT"].present?  # default
  config.sink = :otel               # :otel, :log, or any object responding to flush(attrs)
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

The railtie inserts the middleware directly below `ActionDispatch::Executor` (the executor clears the per-request store, so placement matters), instruments `ActiveJob::Base`, and wires the span counter and notification subscribers after your initializers run. Without Rails, call `WideEvent.install!` yourself after configuring the OpenTelemetry SDK.

## Conventions

Flat keys, dot namespaces, snake_case leaves. Durations end in `_duration_ms`, counts in `_count`, booleans read as assertions, timestamps serialize to RFC 3339. Opaque ids are fine; names, emails, and request params are not, and anything that could quote user input is flagged `pii: review` in the registry.

Unhandled exceptions get `error: true` with no slug, deliberately: the missing slug marks the rescue you haven't instrumented yet.

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
