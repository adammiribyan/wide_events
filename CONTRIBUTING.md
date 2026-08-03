# Contributing

Issues and pull requests welcome.

## Setup

```bash
bin/setup                 # bundle install + appraisal gemfiles
bundle exec rake test     # run the suite
bin/rubocop               # lint (rubocop-rails-omakase)
```

Run a specific Rails version with
`BUNDLE_GEMFILE=gemfiles/rails_7_1.gemfile bundle exec rake test`. CI runs the
full Ruby x Rails matrix plus RuboCop; both must pass.

## Ground rules

- Every behavior change ships with tests. The suite is plain Minitest under
  `test/`; documentation-only changes don't need synthetic test coverage.
- Telemetry never raises into app code: any new entry point rescues and
  reports through `WideEvent.handle_error`. Tests must cover the failure
  path, not only the happy path.
- New attributes the gem itself sets are declared in
  `lib/wide_event/registry/defaults.yml` in the same change, following the
  naming conventions in the README (dot namespaces, `_duration_ms`,
  `_count`, booleans as assertions).
- Add a line to the `## Unreleased` section of `CHANGELOG.md` (create the
  section if it's missing).
