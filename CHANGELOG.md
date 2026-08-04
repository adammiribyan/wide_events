# Changelog

## v0.1.4 (2026-08-04)

- The railtie stores `registry_path` relative to the app root
  (`config/wide_event/registry.yml`) and `Configuration` resolves it via
  `Rails.root.join` when opening the registry. The generated doc's header
  previously committed the machine-specific absolute path, which rewrote
  itself on every other checkout; it now carries the relative path.
  Explicitly configured absolute paths keep working unchanged.
- `Registry#to_markdown` collapses whitespace and escapes pipes in every
  cell. A `notes: >` folded scalar keeps a trailing newline, which pushed
  the row's closing pipe onto its own line and split the table.
- The generated doc's title is "Wide events attribute registry" — no
  hyphen; that spelling belongs to file names, not copy.

## v0.1.3 (2026-08-03)

- README links to the docs are absolute GitHub URLs, so they work on
  rubygems.org (relative links 404 there).
- The install generator adds the registry-enforcement teardown to
  `test/test_helper.rb`, and `capture_wide_events` temporarily enables
  instrumentation while restoring the previous enabled state and sink.
- Documentation and bundled agent skills clarify privacy handling, unnamed
  exception queries, request/job timing, and one-generation-per-event examples.

## v0.1.2 (2026-08-03)

- README "In production" section and `docs/production-example.md`: an
  anonymized composite event, instrumentation patterns, and standing
  queries from the production app the gem was extracted from.

## v0.1.1 (2026-08-03)

- The install generator wires `WideEvent::TestHelper` into
  `test/test_helper.rb` (or prints manual instructions when the file
  isn't recognized).
- Development boot logs a one-time notice when wide events are disabled,
  pointing at the `:log` sink quickstart.
- `assert_registered_wide_event_attributes` resets the violation tally
  after each check, so it can run as a global `teardown` and works with
  parallel test workers.
- README rewritten around first-run experience: 60-second `:log` sink
  quickstart, OpenTelemetry setup guide, enablement modes, end-to-end
  registry workflow, log sink operations, request-vs-job event semantics.
- New `docs/clickhouse-hyperdx.md`: local quickstart and query cookbook.

## v0.1.0 (2026-08-03)

First release, extracted from a production Rails app.

- Wide-event accumulator (`set`, `phase`, `error!`, `count`) with a
  never-raise contract and per-request/per-job isolation.
- Rack middleware and ActiveJob instrumentation, wired by a railtie.
- Sinks: OTel root span (default), JSON log lines, in-memory (tests).
- Span counter: child-span rollups into `stats.*` (configurable scope map).
- Notification subscribers: `db.duration_ms`, `view.duration_ms`, capped
  `cache.*` hit/miss booleans.
- Attribute registry (`config/wide_event/registry.yml`): gem defaults,
  glob entries, strict-mode tracking, `rake wide_events:registry:check`
  and `rake wide_events:registry:docs`.
- Test helpers: `assert_wide_event`, `capture_wide_events`,
  `assert_registered_wide_event_attributes`.
- Generators: `wide_events:install` (initializer, registry, AGENTS.md
  section) and `wide_events:skills` (agent skills for writing and
  querying instrumentation).
