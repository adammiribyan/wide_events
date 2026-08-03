## Wide events

Every HTTP request and job execution emits one flat telemetry event (the OTel
root span, marked `main = true`). When you add or change a feature,
instrument it:

- `WideEvent.set("report.id" => report.id)` for domain attributes
- `WideEvent.phase("pdf_render") { ... }` to time a block into `pdf_render.duration_ms`
- `WideEvent.error!(slug: "err-report-source-missing", exception: e, expected: true)` in rescue blocks

Naming: dot namespaces with snake_case leaves, durations end in
`_duration_ms`, counts in `_count`, booleans read as assertions. Every new
attribute must be declared in `config/wide_event/registry.yml` in the same
change; the test suite fails on undeclared attributes. Assert instrumentation
with `assert_wide_event` from `wide_event/test_helper`. See the
`instrumenting-wide-events` skill for full conventions and
`debugging-with-wide-events` for querying production.
