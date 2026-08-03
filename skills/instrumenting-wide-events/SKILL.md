---
name: instrumenting-wide-events
description: Use when adding or changing app code that should be observable in production: new features, slow paths, rescue blocks, background jobs. Covers the WideEvent API (set/phase/error!), attribute naming conventions, the registry workflow, and test assertions.
---

# Instrumenting wide events

Every HTTP request and job execution in this app emits one flat telemetry
event: the OTel root span, marked `main = true` (or one JSON log line with
the `:log` sink). The gem records the generic attributes (route, status,
timings, query counts, cache hits). Your job is the attributes only the app
knows: the domain nouns.

## The API

Attribute writes are safe no-ops outside a request/job; `phase` still executes
and returns its block. Telemetry failures never raise into app code.

```ruby
# Flat attributes, merged into the current event
WideEvent.set("report.id" => report.id, "report.format" => "pdf")

# Time a block into <name>.duration_ms (returns the block's value)
WideEvent.phase("pdf_render") { render_pdf }

# Mark a handled failure in a rescue block
rescue Api::ClientError => e
  WideEvent.error!(slug: "err-vendor-user-not-found", exception: e, expected: true)
```

`error!` slugs are static kebab-case string literals, unique across the app,
prefixed `err-`. Never interpolate into a slug. Unhandled exceptions get
`error: true` with NO slug automatically: that absence is the standing
"instrument this rescue" query, so don't fake slugs for paths you didn't
handle.

## Naming conventions

- Flat keys, dot namespaces, snake_case leaves: `report.page_count`, not `reportPageCount` or nested hashes.
- Durations end in `_duration_ms` (float milliseconds). Counts end in `_count`.
- Booleans read as assertions: `report.cached`, not `report.cache_status`.
- Timestamps are RFC 3339 strings (pass a Time; the gem serializes it).
- Sizes and counts over raw content: `note.body_char_count: 2140`, never the note text.

## PII policy

Apply the deployment's privacy policy to every identifier. Opaque user and
account IDs are preferable to names or emails, but still declare them as
`pii: opaque_id`. Exclude free-text user input and request params. If an
attribute could quote user input, mark it `pii: review` in the registry and
expect it to be scrubbed or excluded from forwarding.

## The registry is the contract

Every attribute you set must be declared in `config/wide_event/registry.yml`
in the same change:

```yaml
report.format:
  type: string
  set_by: ReportsController#create
  pii: none
  notes: pdf or csv
```

Keys containing `*` are globs for dynamic families (`feature_flag.*`). With
`config.strict = true` in the test environment, the suite records undeclared
attributes; `assert_registered_wide_event_attributes` fails on them. Run
`rake wide_events:registry:docs` if this app generates the human-readable
registry doc, and never rename a shipped attribute without a reason—renames
orphan historical data.

## Prove it in tests

```ruby
require "wide_event/test_helper"   # include WideEvent::TestHelper

test "report rendering is instrumented" do
  assert_wide_event("pdf_render.duration_ms", "report.format" => "pdf") do
    Report.new(format: "pdf").render
  end
end
```

For code that runs its own unit of work (jobs, full request dispatch), use
`capture_wide_events { perform_work }` and assert on the returned events.

## Checklist for any instrumentation change

1. Attributes follow the naming conventions above.
2. New rescue blocks that swallow errors call `error!` with a unique static slug.
3. Every new attribute has a registry entry in the same change.
4. A test asserts the attribute is set (`assert_wide_event`).
5. Opaque IDs are declared `pii: opaque_id`; anything questionable is flagged `pii: review`.
