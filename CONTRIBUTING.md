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

## The store service

`store/` is a Go service around DuckDB, shipped as the container image the
Kamal generator pins. It needs Go (the toolchain version comes from
`store/go.mod`) and cgo, since DuckDB is a native library.

```bash
cd store
go build ./cmd/wide-events   # binary in store/wide-events (gitignored)
go test ./...
go vet ./...
gofmt -w .                   # CI fails on a dirty diff after this
```

Build the published image from the repository root, not from `store/` (the
Dockerfile's context is the repo root):

```bash
docker build --build-arg VERSION=0.0.0-dev -f store/Dockerfile -t wide-events:dev .
docker buildx build --platform linux/amd64,linux/arm64 -f store/Dockerfile .
```

`store/README.md` documents the runtime environment variables, the HTTP
routes, and the admin socket.

## Cross-runtime integration tests

`test/integration/` drives a real `wide-events` process from the real Ruby
client and sender: cross-host round trips, lost acknowledgements, restarts,
queue overflow during an outage, the disk-degraded `507` path, and the admin
backup CLI. These compile Go and spawn processes, so they stay out of
`rake test` and run from their own command:

```bash
bundle exec ruby -Itest -e 'Dir["test/integration/**/*_test.rb"].sort.each { |f| require File.expand_path(f) }'
```

The harness builds the binary with the `testprobe` build tag so the disk
probe is deterministic (a fixed 50 GiB volume, 20 GiB free) rather than
inheriting the host filesystem's real capacity. Everything else under test is
the shipped code.

## Kamal compatibility

`test/integration/kamal/` is a fixture app whose `config/deploy.yml` is real
generator output, including the structured `directories` entry whose `mode`
and `owner` the store's volume depends on.
`script/test-kamal-config` installs the requested Kamal into an isolated
`GEM_HOME` (nothing touches this repo's bundle), runs `kamal config` against
the fixture, and asserts the resolved configuration:

```bash
script/test-kamal-config 2.10.0 success
script/test-kamal-config 2.12.0 success
script/test-kamal-config 2.9.0 failure    # older Kamal must reject it
```

Change the accessory template and this will tell you the fixture is stale.
The editor rewrites its own marked block on every apply, so regenerate in
place:

```bash
bundle exec ruby -Ilib -rwide_events -r"wide_event/kamal/deploy_editor" -e '
  path = "test/integration/kamal/config/deploy.yml"
  result = WideEvent::Kamal::DeployEditor.new(File.read(path),
    service: "myapp", host: "203.0.113.20", hostname: "telemetry.example.com").apply
  abort result.conflicts.inspect if result.conflicts.any?
  File.write(path, result.content)'
```

## Packaging and release

```bash
script/check-gem-contents      # builds the gem, asserts store files and all skills ship
script/verify-release-version 0.1.4
```

`bin/release <version>` runs both before it pushes anything. Pushing the
`v<version>` tag triggers `.github/workflows/release-store.yml`, which
publishes `ghcr.io/adammiribyan/wide-events-store:<version>` and a matching
`sha-<commit>` tag for linux/amd64 and linux/arm64. There is no `latest`
tag; the generated accessory pins an exact version, so the tag, the gem's
`WideEvent::VERSION`, and that pin all have to agree.

Agreement is not existence. Between the tag push and `gem push`,
`bin/release` blocks on `script/await-store-image <version>`, which watches
the triggered workflow run and then confirms the manifest is readable. If
the image never appears, the gem is not published and the script prints the
exact commands to resume once the workflow is fixed. Run
`script/await-store-image <version>` by hand to resume, or to check an
existing release.

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
- Changes to the store's HTTP protocol, its schema, or the Ruby sender ship
  with a `test/integration/` case. Unit tests on either side can both pass
  while the two runtimes disagree on the wire.
