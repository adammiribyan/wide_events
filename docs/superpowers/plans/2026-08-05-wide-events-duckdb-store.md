# Wide Events DuckDB Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a version-pinned DuckDB companion service plus a bounded Rails sender, SQL client, and Kamal setup workflow so several Rails hosts can capture and query one telemetry dataset.

**Architecture:** One Go process owns the DuckDB file, accepts authenticated batched events, and exposes bounded read-only SQL through Kamal Proxy. Ruby application processes only enqueue sanitized events into finite per-process memory; a sender thread batches them over HTTPS. Rails generators and tasks produce the pinned Kamal accessory, durable secrets, DNS/setup checks, query command, and bundled operating skill.

**Tech Stack:** Ruby 3.2+, Rails 7.1+, Minitest, Go 1.24.0, `github.com/duckdb/duckdb-go/v2` v2.10505.0 (DuckDB 1.5.5), standard-library HTTP/JSON/gzip, Docker Buildx, Kamal 2.10.0+.

## Global Constraints

- Protocol version is integer `1`; gem and store semantic versions are independent.
- Runtime Ruby classes use `WideEvent`; only Rails generators use `WideEvents::Generators`.
- DuckDB is opened by exactly one service process. Helpers communicate with it over `/run/wide-events/admin.sock` and never open the file.
- The application request/job thread performs no network I/O and telemetry never raises into application work.
- Client queue: at most 1,000 events or 8 MiB; event: at most 64 KiB canonical JSON and 256 attributes.
- Batch: at most 100 events or 1 MiB canonical JSON; compressed HTTP body at most 512 KiB; server decompressed body at most 2 MiB.
- Server ingestion: 20 decoding requests and a 32 MiB canonical-byte writer queue; reject excess with `429`.
- Retry timeout is 24 hours; batch and loss-report ledger retention is seven days; retry backoff caps at 30 seconds.
- Query defaults: two active, four waiting for at most two seconds, 15-second execution, 10,000 rows, 10 MiB response, 1 GiB DuckDB memory.
- Store container limit is 2 GiB. Query spill is the lesser of 4 GiB or 25% of the volume.
- Generic free-space floor is the greater of 2 GiB or 10% of the volume. Backup additionally requires database bytes plus the greater of 512 MiB or 10% of database bytes.
- `/up` returns `200` while reads remain routable and reports `write_ready: false`; ingestion returns `507` under disk pressure.
- Public administration, mutating SQL, external access, extension loading, and unbounded queues are forbidden.
- Generated Kamal configuration sets `minimum_version: 2.10.0` and pins the store image; it never uses `latest`.
- Preserve the existing OTel, log, memory, custom-sink, Rails/Ruby matrix, and no-hard-OTel-dependency behavior.

## Delivery Phases

1. Tasks 1–6 produce a standalone, containerized store that can be tested with `curl` and SQL.
2. Tasks 7–9 produce the Rails capture, delivery, and query clients, testable against HTTP fakes.
3. Tasks 10–12 produce the five-to-ten-minute Kamal setup and cross-runtime release gate.

---

### Task 1: Store module, configuration, and protocol validation

**Files:**
- Create: `store/go.mod`
- Create: `store/go.sum`
- Create: `store/internal/config/config.go`
- Create: `store/internal/config/config_test.go`
- Create: `store/internal/protocol/events.go`
- Create: `store/internal/protocol/events_test.go`

**Interfaces:**
- Produces: `config.Load(getenv func(string) string) (config.Config, error)`.
- Produces: `protocol.DecodeBatch(io.Reader) (protocol.Batch, []byte, error)` where the byte slice is canonical JSON charged to the writer queue.
- Produces: `protocol.Batch`, `protocol.Event`, `protocol.Ack`, and typed `protocol.ValidationError`.
- Consumes: no earlier task.

- [ ] **Step 1: Write failing configuration and protocol tests**

```go
func TestLoadDefaults(t *testing.T) {
    cfg, err := Load(mapEnv(map[string]string{
        "WIDE_EVENTS_DATABASE": "/data/events.duckdb",
        "WIDE_EVENTS_SERVICE": "billing",
        "WIDE_EVENTS_ENVIRONMENT": "production",
        "WIDE_EVENTS_INGEST_TOKEN": strings.Repeat("a", 64),
        "WIDE_EVENTS_QUERY_TOKEN": strings.Repeat("b", 64),
    }))
    require.NoError(t, err)
    assert.Equal(t, "0.0.0.0:7421", cfg.Listen)
    assert.Equal(t, int64(32<<20), cfg.WriterQueueBytes)
    assert.Equal(t, 30, cfg.RetentionDays)
}

func TestDecodeBatchRejectsOversizedEventAndNonPrimitiveAttribute(t *testing.T) {
    _, _, err := DecodeBatch(strings.NewReader(batchJSON(map[string]any{
        "nested": map[string]any{"not": "flat"},
    })))
    assert.ErrorIs(t, err, ErrInvalidAttribute)
}
```

- [ ] **Step 2: Initialize the module and verify the tests fail**

Run: `cd store && go mod init github.com/adammiribyan/wide_events/store && go get github.com/duckdb/duckdb-go/v2@v2.10505.0 github.com/google/uuid@v1.6.0 github.com/stretchr/testify@v1.11.1 && go test ./internal/config ./internal/protocol`

Expected: FAIL because `Load`, `DecodeBatch`, and protocol types do not exist.

- [ ] **Step 3: Implement exact configuration defaults and validation**

```go
type Config struct {
    Listen, Database, Service, Environment string
    IngestToken, QueryToken, AdminSocket string
    RetentionDays, QueryActive, QueryWaiting int
    WriterQueueBytes, FreeFloorBytes, QueryTempBytes int64
    QueryTimeout, QueryWaitTimeout time.Duration
}

const (
    ProtocolVersion = 1
    MaxEvents = 500
    MaxAttributes = 256
    MaxEventBytes = 64 << 10
    MaxDecodedBodyBytes = 2 << 20
)
```

`protocol.Batch` includes `DroppedEventsSinceLastBatch uint64` and `LossReportID *string`. Require a valid UUID when the count is non-zero and require `nil` when it is zero, so a loss report has an independently deduplicated identity.

Reject missing service/environment/database/tokens, identical tokens, tokens shorter than 64 hexadecimal characters, retention below one day, and non-positive resource limits. Default the admin socket to `/run/wide-events/admin.sock`.

- [ ] **Step 4: Implement streaming JSON validation and canonicalization**

Define `Event` with `DurationMS *float64`, accept only `request` and `job` in v1, require duration for both, and permit only `string`, `float64`, `bool`, and `nil` JSON attribute values. Use `json.Decoder.UseNumber`, reject trailing JSON, validate UUID/RFC3339 fields, cap 500 events/256 attributes/64 KiB per canonical event, then `json.Marshal` the accepted `Batch` to produce charged queue bytes.

- [ ] **Step 5: Run focused tests and commit**

Run: `cd store && go test ./internal/config ./internal/protocol`

Expected: PASS, including boundary tests at and one byte beyond every limit.

```bash
git add store/go.mod store/go.sum store/internal/config store/internal/protocol
git commit -m "feat(store): define configuration and event protocol"
```

### Task 2: DuckDB schema, migrations, and idempotent batch writer

**Files:**
- Create: `store/internal/storage/database.go`
- Create: `store/internal/storage/migrations.go`
- Create: `store/internal/storage/writer.go`
- Create: `store/internal/storage/status.go`
- Create: `store/internal/storage/storage_test.go`
- Create: `store/internal/storage/testdata/variant_queries.sql`

**Interfaces:**
- Consumes: `config.Config`, `protocol.Batch`, canonical bytes from Task 1.
- Produces: `storage.Open(context.Context, config.Config) (*storage.Database, error)`.
- Produces: `(*Database).CommitBatch(context.Context, protocol.Batch) (storage.CommitResult, error)`.
- Produces: `(*Database).PruneLedger(context.Context, time.Time) error`, `Checkpoint`, `Status`, and `Close`.

- [ ] **Step 1: Write failing migration, insert, deduplication, and VARIANT tests**

```go
func TestCommitBatchIsAtomicAndIdempotent(t *testing.T) {
    db := openTestDatabase(t)
    batch := validBatch("018f6f50-2f9d-7fb0-a258-6d9f95f20c01")
    first, err := db.CommitBatch(context.Background(), batch)
    require.NoError(t, err)
    second, err := db.CommitBatch(context.Background(), batch)
    require.NoError(t, err)
    assert.False(t, first.Duplicate)
    assert.True(t, second.Duplicate)
    assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events"))
}

func TestVariantQueryCorpus(t *testing.T) {
    got := queryString(t, db, `SELECT variant_extract(attributes, 'report.format')::VARCHAR FROM wide_events`)
    assert.Equal(t, "pdf", got)
}
```

- [ ] **Step 2: Run the storage tests to verify they fail**

Run: `cd store && go test ./internal/storage -run 'TestCommitBatch|TestVariant' -v`

Expected: FAIL because the database and schema do not exist.

- [ ] **Step 3: Open one DuckDB instance with locked settings and monotonic migrations**

Use `duckdb.NewConnector` and `sql.OpenDB`. Initialize connections with the exact internal settings below, then lock configuration after startup:

```sql
SET memory_limit = '1GiB';
SET enable_external_access = false;
SET autoinstall_known_extensions = false;
SET autoload_known_extensions = false;
SET allow_community_extensions = false;
SET temp_directory = '/var/lib/wide-events/tmp';
SET max_temp_directory_size = '<computed bytes>';
SET lock_configuration = true;
```

Pass `QueryTempBytes` as a bound parameter/string produced from a validated integer; Task 6 lowers its 4 GiB default to 25% of the actual volume before `Open`. Derive `temp_directory` beside the configured database rather than trusting a client-supplied SQL path.

Create `wide_events`, `wide_events_ingestion_batches`, `wide_events_loss_reports`, `wide_events_store_state`, and a one-row `wide_events_store_status` view. Store migration version in `wide_events_schema_migrations`; run each embedded migration once inside a transaction before readiness.

- [ ] **Step 4: Implement transactional inserts and promoted columns**

Insert batch-ledger row, a non-zero loss report if its `loss_report_id` is unseen, and all events in one transaction. Return `Duplicate: true` when `batch_id` already exists. Re-carried loss reports with a new batch ID do not increment the status total twice. Promote exactly:

```go
route := stringAttr(event.Attributes, "http.route.controller")
jobClass := stringAttr(event.Attributes, "job.class")
status := intAttr(event.Attributes, "http.response.status_code")
isError := boolAttr(event.Attributes, "error", false)
requestID := stringAttr(event.Attributes, "http.request.id")
```

Bind the full sanitized attribute JSON and cast it with `json(?::VARCHAR)::VARIANT`. Set configured service/environment and transaction-time `received_at`; never trust client values for stored tenancy.

- [ ] **Step 5: Implement ledger pruning and the status relation**

Prune batch and loss-report ledgers at `committed_at < now - INTERVAL '7 days'`. Update the one-row state table through typed methods. The view exposes rows in the last hour/day, oldest/newest receipt, database-file/free-disk bytes, queued ingest bytes, rejected batches, deduplicated reported drops, batch/loss-ledger rows, `write_ready`/reason, last checkpoint/retention/backup timestamps, last maintenance/backup errors, and store/protocol/DuckDB versions. Versions remain absent from `/up`.

- [ ] **Step 6: Run storage tests and commit**

Run: `cd store && go test ./internal/storage -v`

Expected: PASS for atomic rollback, duplicate batch, nullable duration schema, promotion, drift-safe tenancy, ledger pruning, status view, reopen/WAL recovery, and `variant_queries.sql` on DuckDB 1.5.5.

```bash
git add store/internal/storage
git commit -m "feat(store): persist idempotent DuckDB batches"
```

### Task 3: Authenticated ingestion, byte backpressure, and routing health

**Files:**
- Create: `store/internal/ingest/writer.go`
- Create: `store/internal/ingest/writer_test.go`
- Create: `store/internal/httpapi/auth.go`
- Create: `store/internal/httpapi/body.go`
- Create: `store/internal/httpapi/server.go`
- Create: `store/internal/httpapi/ingest_test.go`
- Create: `store/internal/httpapi/health_test.go`

**Interfaces:**
- Consumes: `protocol.DecodeBatch`, `storage.Database`, configuration limits.
- Produces: `ingest.NewWriter(*storage.Database, maxBytes int64) *ingest.Writer`.
- Produces: `(*Writer).Submit(context.Context, protocol.Batch, []byte) (storage.CommitResult, error)` and `Close(context.Context)`.
- Produces: `httpapi.New(config.Config, Dependencies) http.Handler` with `POST /v1/events` and `GET /up`.

- [ ] **Step 1: Write failing HTTP boundary and authentication tests**

```go
func TestGzipExpansionStopsAtTwoMiB(t *testing.T) {
    body := gzipBytes(bytes.Repeat([]byte(" "), protocol.MaxDecodedBodyBytes+1))
    res := postEvents(t, server, ingestToken, body, "gzip")
    assert.Equal(t, http.StatusRequestEntityTooLarge, res.StatusCode)
}

func TestDiskDegradedHealthRemainsRoutable(t *testing.T) {
    deps.Status.SetWriteReady(false, "disk_space_low")
    assert.Equal(t, 200, get(t, server, "/up").StatusCode)
    assert.Equal(t, 507, postValidBatch(t, server).StatusCode)
}
```

- [ ] **Step 2: Run focused tests and confirm failure**

Run: `cd store && go test ./internal/ingest ./internal/httpapi -run 'TestGzip|TestDisk|TestAuth|TestQueue' -v`

Expected: FAIL because no writer or handlers exist.

- [ ] **Step 3: Implement constant-time bearer authentication and bounded bodies**

Compare SHA-256 digests with `subtle.ConstantTimeCompare`; never log tokens. Wrap the compressed body with `http.MaxBytesReader` at 512 KiB. For gzip, wrap the decoder in `io.LimitedReader{N: 2<<20 + 1}` and reject before JSON decoding when the extra byte is observed. Allow at most 20 decoding handlers through a semaphore.

- [ ] **Step 4: Implement a canonical-byte writer queue**

The queue tracks `len(canonical)` under a mutex, rejects an admission that would exceed 32 MiB, and stores only canonical bytes plus the validated batch and a one-result channel. Publish queued bytes and rejected admissions through the typed status updater. One goroutine calls `CommitBatch`; HTTP acknowledges only after that result. Return typed errors mapped to `429`, `507`, validation `400`, protocol `426`, and internal `503` without accepting unbounded work.

- [ ] **Step 5: Implement acknowledgement, drift warnings, and `/up`**

Return:

```json
{"protocol_version":1,"batch_id":"...","duplicate":false,"warnings":[]}
```

When client service/environment differs, commit configured values, increment the status counter, log expected/received values, and add one warning. `/up` returns only `protocol_version`, `write_ready`, and `degradation_reason`; it remains `200` for disk pressure/maintenance and returns `503` only when the database cannot serve normal API traffic.

- [ ] **Step 6: Run package tests and commit**

Run: `cd store && go test ./internal/ingest ./internal/httpapi -v`

Expected: PASS for gzip bombs, body/event/batch boundaries, 20-request semaphore, 32 MiB admission, duplicate acknowledgement, mismatched tenancy, bad tokens, database errors, `507`, and routing-health semantics.

```bash
git add store/internal/ingest store/internal/httpapi
git commit -m "feat(store): add bounded ingestion API"
```

### Task 4: Read-only query policy, concurrency, and result limits

**Files:**
- Create: `store/internal/query/policy.go`
- Create: `store/internal/query/executor.go`
- Create: `store/internal/query/limited_writer.go`
- Create: `store/internal/query/query_test.go`
- Create: `store/internal/query/testdata/rejected.sql`
- Create: `store/internal/httpapi/query.go`
- Create: `store/internal/httpapi/query_test.go`

**Interfaces:**
- Consumes: the query connection from `storage.Database` and query-token auth from Task 3.
- Produces: `query.NewExecutor(*storage.Database, query.Limits, *slog.Logger) *query.Executor`.
- Produces: `(*Executor).Execute(context.Context, query.Request, io.Writer) (query.ResultMeta, error)`.
- Extends: `httpapi.New` with authenticated `POST /v1/query`.

- [ ] **Step 1: Write failing statement-policy and contention tests**

```go
func TestPolicyRejectsMutatingAndMultiStatements(t *testing.T) {
    for _, sql := range []string{
        "DELETE FROM wide_events", "COPY wide_events TO '/tmp/x'",
        "ATTACH '/tmp/x.db'", "INSTALL httpfs", "SELECT 1; SELECT 2",
    } {
        t.Run(sql, func(t *testing.T) { assert.ErrorIs(t, executor.Validate(ctx, sql), ErrReadOnlyRequired) })
    }
}

func TestFifthWaiterGetsBusyAfterTwoSeconds(t *testing.T) {
    occupyTwoActiveAndFourWaiting(t, executor)
    _, err := executor.Execute(ctx, Request{SQL: "SELECT 1"}, io.Discard)
    assert.ErrorIs(t, err, ErrBusy)
}
```

- [ ] **Step 2: Run query tests and verify failure**

Run: `cd store && go test ./internal/query ./internal/httpapi -run 'TestPolicy|TestFifth|TestTimeout|TestResult' -v`

Expected: FAIL because the query executor does not exist.

- [ ] **Step 3: Classify prepared statements using DuckDB, not SQL prefixes**

Use `(*sql.Conn).Raw` to access `*duckdb.Conn`, prepare once for classification, cast the driver statement to `*duckdb.Stmt`, and call `StatementType()`. Permit only `STATEMENT_TYPE_SELECT`; DuckDB 1.5.5 reports `SELECT`, read-only `WITH`, `SHOW`, and `DESCRIBE` as that type. Reject `EXPLAIN` because DuckDB also classifies `EXPLAIN ANALYZE DELETE ...` as `EXPLAIN`, making the inner operation unprovable from statement type. Reject `STATEMENT_TYPE_MULTI` and every unrecognized value by default. Keep `rejected.sql` as a bypass corpus covering comments, Unicode whitespace, nested CTE DML, `EXPLAIN`, `EXPLAIN ANALYZE` over DML, `PRAGMA`, `CALL`, `SET`, `LOAD`, `EXPORT`, `ATTACH`, and `COPY`. Separate locked-setting tests prove SELECT-shaped `read_csv`, `read_json`, `read_parquet`, `glob`, and remote URL attempts cannot read files or networks when external access is disabled.

```go
func allowed(t duckdb.StmtType) bool {
    return t == duckdb.STATEMENT_TYPE_SELECT
}
```

- [ ] **Step 4: Implement independent busy and execution limits**

Use a two-token active semaphore and a separately counted four-request waiting budget. Waiters time out after two seconds with `ErrBusy`; admitted queries receive a child context with a 15-second deadline. A cancellation is `ErrExecutionLimit`, not `ErrBusy`.

- [ ] **Step 5: Stream bounded JSON and CSV results**

Accept one request object:

```json
{"protocol_version":1,"sql":"SELECT count(*) FROM wide_events","format":"json"}
```

JSON returns `columns`, `rows`, and `truncated`; CSV writes a header and rows. Stop at 10,000 rows or before a write would cross 10 MiB, cancel the query, and return a typed limit error. Normalize `time.Time`, `[]byte`, UUID, decimal, list, and null values deterministically. Log duration, row count, byte count, status, and SHA-256 query hash, never raw SQL.

- [ ] **Step 6: Wire `POST /v1/query`, test, and commit**

Map busy to `429` plus `Retry-After: 2`, query limits to `422`, invalid/read-write statements to `400`, and temporary maintenance to `503`. Require only the query token; prove the ingest token is rejected.

Run: `cd store && go test ./internal/query ./internal/httpapi -v`

Expected: PASS for the allow/reject corpus, two-active/four-waiting behavior, cancellation, row/byte bounds, JSON/CSV encoding, token separation, and raw-SQL-free logs.

```bash
git add store/internal/query store/internal/httpapi
git commit -m "feat(store): expose bounded read-only SQL"
```

### Task 5: Disk guard, retention, backups, status, and local administration

**Files:**
- Create: `store/internal/disk/space.go`
- Create: `store/internal/disk/space_unix.go`
- Create: `store/internal/disk/space_test.go`
- Create: `store/internal/maintenance/gate.go`
- Create: `store/internal/maintenance/manager.go`
- Create: `store/internal/maintenance/backup.go`
- Create: `store/internal/maintenance/manager_test.go`
- Create: `store/internal/admin/server.go`
- Create: `store/internal/admin/client.go`
- Create: `store/internal/admin/admin_test.go`

**Interfaces:**
- Consumes: `storage.Database`, writer/query admission hooks, and `config.Config`.
- Produces: `disk.Probe` and `disk.StatFSProbe`.
- Produces: `maintenance.NewManager(Dependencies) *maintenance.Manager`, `Start`, `RunDaily`, `Backup`, and `Close`.
- Produces: `admin.Listen(socket string, mode fs.FileMode, manager AdminManager) error` and `admin.RunClient(socket string, args []string, stdout, stderr io.Writer) int`.

- [ ] **Step 1: Write failing disk, retention, backup, and socket tests**

```go
func TestBackupRequiresDatabaseSizePlusMarginAndFloor(t *testing.T) {
    probe := fakeProbe{Free: 5 << 30, Total: 50 << 30}
    manager := newManager(t, probe, withDatabaseBytes(8<<30))
    err := manager.Backup(context.Background())
    assert.ErrorIs(t, err, ErrInsufficientBackupSpace)
    assert.NoFileExists(t, manager.nextBackupPath())
}

func TestAdminClientUsesSocketAndNeverOpensDatabase(t *testing.T) {
    manager := &fakeAdminManager{}
    socket := startAdminServer(t, manager)
    assert.Equal(t, 0, RunClient(socket, []string{"backup"}, io.Discard, io.Discard))
    assert.Equal(t, 1, manager.BackupCalls)
    assert.Equal(t, os.FileMode(0600), socketMode(t, socket))
}
```

- [ ] **Step 2: Run maintenance tests and verify failure**

Run: `cd store && go test ./internal/disk ./internal/maintenance ./internal/admin -v`

Expected: FAIL because disk probing, the gate, backup, and IPC do not exist.

- [ ] **Step 3: Implement the generic write-space guard**

`StatFSProbe` reports total/free bytes for the database filesystem and publishes free bytes plus the current database-file size to status on every probe. Before each write, require `free >= max(2 GiB, total/10)`. On failure set state to `write_ready=false, reason=disk_space_low`; the ingest API returns `507`, `/up` remains `200`, and queries remain admitted. Clear the degraded state only after a later probe passes. Convert DuckDB/OS ENOSPC into the same typed error and ensure the transaction rolls back.

- [ ] **Step 4: Implement daily retention, ledger pruning, and checkpointing**

At most once per UTC day, close writer admission, drain accepted work, delete event rows by `received_at`, prune batch-ledger rows older than seven days, run `CHECKPOINT`, update status timestamps/errors, and reopen admission. After retention succeeds and normal admission resumes, invoke the automatic backup path once for that UTC day. Scheduled retention or backup failures log once with structured fields, remain queryable in status, and never crash the process.

- [ ] **Step 5: Implement size-aware verified backups**

Check capacity before closing the gate and again after drain/checkpoint. Require:

```go
margin := max(int64(512<<20), databaseBytes/10)
requiredFree := max(genericFloor, databaseBytes+margin)
```

If insufficient, do not create a file, preserve existing backups, record required/available bytes, and return a typed error. Otherwise create a non-existent timestamped path, then execute internally:

```sql
ATTACH '<validated-generated-path>' AS wide_events_backup;
COPY FROM DATABASE <main-catalog> TO wide_events_backup;
CHECKPOINT wide_events_backup;
DETACH wide_events_backup;
```

Open the copy read-only in the same service process, query schema version and row count, then retain the newest three verified files. Remove incomplete files on every failure. During the exclusive copy, ingestion and query endpoints return retryable `503`, while `/up` remains `200` with `write_ready=false, degradation_reason=maintenance`.

- [ ] **Step 6: Implement the private Unix-socket admin protocol**

Create `/run/wide-events` as uid/gid 1000, bind `admin.sock`, chmod `0600`, and accept newline-delimited local JSON commands. V1 supports `{"command":"backup"}` only. `wide-events admin backup` connects to this socket and prints the resulting backup path or required/available byte error; it contains no DuckDB imports and never receives the database path.

- [ ] **Step 7: Run maintenance tests and commit**

Run: `cd store && go test ./internal/disk ./internal/maintenance ./internal/admin ./internal/httpapi -v`

Expected: PASS for low-disk read availability, ENOSPC rollback, both backup preflights, partial cleanup, three-copy retention, failed verification, ledger retention, daily scheduling, gate responses, socket permissions, and admin delegation.

```bash
git add store/internal/disk store/internal/maintenance store/internal/admin store/internal/httpapi
git commit -m "feat(store): add retention and verified local backups"
```

### Task 6: Store executable, shutdown, container, and store CI

**Files:**
- Create: `store/cmd/wide-events/main.go`
- Create: `store/cmd/wide-events/main_test.go`
- Create: `store/Dockerfile`
- Create: `store/.dockerignore`
- Create: `store/README.md`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: configuration, database, writer, query, maintenance, admin, and HTTP packages from Tasks 1–5.
- Produces: one `wide-events` binary with server mode and `admin backup` subcommand.
- Produces: a non-root multi-architecture OCI image exposing internal port `7421`.

- [ ] **Step 1: Write a failing process-level lifecycle test**

Start the command with a temporary database/listen address/socket, wait for `/up`, send one event, query it, deliver `SIGTERM`, and assert exit zero plus successful database reopen. Also invoke `admin backup` against the running process and assert the helper output names a verified file.

- [ ] **Step 2: Run the process test and verify failure**

Run: `cd store && go test ./cmd/wide-events -run TestServerLifecycle -v`

Expected: FAIL because the executable does not exist.

- [ ] **Step 3: Assemble dependencies and graceful shutdown**

`main` loads config, probes the database volume, lowers `QueryTempBytes` to the lesser of 4 GiB or 25% of total bytes, opens/migrates DuckDB, starts the single writer, maintenance scheduler, Unix socket, and `http.Server`, then marks routing ready. On `SIGINT`/`SIGTERM`, stop HTTP admission, allow two seconds for writer drain, stop maintenance/admin, checkpoint, close DuckDB, and return non-zero on an incomplete startup or unsafe shutdown.

- [ ] **Step 4: Add the multi-stage image**

Use `golang:1.24-bookworm` with `CGO_ENABLED=1` to build both supported Buildx platforms, then copy into `debian:bookworm-slim` with `ca-certificates` and `libstdc++6`. Create uid/gid 1000, `/var/lib/wide-events`, and `/run/wide-events`; run as `1000:1000`; set `ENTRYPOINT ["/usr/local/bin/wide-events"]` and `EXPOSE 7421`. Inject semantic version with `-ldflags -X main.version=$VERSION`.

- [ ] **Step 5: Add store test/build jobs to CI**

Add an Ubuntu job using `actions/setup-go` with `go-version-file: store/go.mod`, then run:

```bash
cd store
go test ./...
go vet ./...
```

Add a Buildx job that builds `store/Dockerfile` for `linux/amd64,linux/arm64` without pushing on pull requests.

- [ ] **Step 6: Run all store verification and commit**

Run: `cd store && go test ./... && go vet ./...`

Run: `docker buildx build --platform linux/amd64,linux/arm64 -f store/Dockerfile .`

Expected: all Go tests/vet pass and both image platforms build.

```bash
git add store .github/workflows/ci.yml
git commit -m "build(store): ship the DuckDB service image"
```

### Task 7: Total-duration capture and canonical Ruby event envelopes

**Files:**
- Modify: `lib/wide_event/middleware.rb`
- Modify: `lib/wide_event/job_instrumentation.rb`
- Modify: `lib/wide_event/registry/defaults.yml`
- Create: `lib/wide_event/store/envelope.rb`
- Create: `test/store_envelope_test.rb`
- Modify: `test/middleware_test.rb`
- Modify: `test/job_instrumentation_test.rb`

**Interfaces:**
- Consumes: existing `WideEvent.sanitize` and the unchanged `sink.flush(attrs)` contract.
- Produces: a registered `duration_ms` attribute for requests/jobs.
- Produces: `WideEvent::Store::Envelope.build(attrs, clock:, uuid:, env:) -> String`, one canonical event JSON string.

- [ ] **Step 1: Write failing duration and envelope tests**

```ruby
test "middleware records total duration before flushing" do
  sink = WideEvent::Sinks::Memory.new
  WideEvent.configure { |c| c.sink = sink }
  call_middleware(->(_env) { [200, {}, ["ok"]] })
  assert_operator sink.events.first["duration_ms"], :>=, 0.0
end

test "envelope preserves sanitized primitive attribute types" do
  json = WideEvent::Store::Envelope.build(
    { "duration_ms" => 12.5, "report.format" => "pdf", "error" => false },
    clock: -> { Time.utc(2026, 8, 5, 12) }, uuid: -> { "018f6f50-2f9d-7fb0-a258-6d9f95f20c01" },
    env: { "KAMAL_VERSION" => "abc123", "HOSTNAME" => "web-1" }
  )
  assert_equal "request", JSON.parse(json).fetch("kind")
  assert_equal 12.5, JSON.parse(json).fetch("duration_ms")
end
```

- [ ] **Step 2: Run focused Ruby tests and verify failure**

Run: `bundle exec rake test`

Expected: FAIL because `duration_ms` and `Store::Envelope` do not exist.

- [ ] **Step 3: Measure total duration with the monotonic clock**

Capture `Process.clock_gettime(Process::CLOCK_MONOTONIC)` immediately before entering application request/job work. In each existing `ensure`, set rounded elapsed milliseconds before `WideEvent.flush(attrs)`. Preserve exception re-raise and disabled fast paths. Register `duration_ms` as float, `pii: none`.

- [ ] **Step 4: Build a size-checked canonical event**

`Envelope.build` sanitizes once, derives `kind` from presence of `job.class`, requires duration, sets UUID and UTC RFC3339 milliseconds, uses `KAMAL_VERSION` for deployment and `KAMAL_CONTAINER_NAME`, then `HOSTNAME`, then `Socket.gethostname` for source. It JSON-encodes with string keys, rejects over 256 attributes or 64 KiB, and returns frozen bytes. It does not perform I/O or mutate the input.

- [ ] **Step 5: Run tests and commit**

Run: `bundle exec rake test`

Expected: PASS for success/exception duration, request/job kind, nullable future storage compatibility, types, deterministic metadata, source fallback, and event-size rejection through `WideEvent.handle_error`.

```bash
git add lib/wide_event/middleware.rb lib/wide_event/job_instrumentation.rb lib/wide_event/registry/defaults.yml lib/wide_event/store/envelope.rb test
git commit -m "feat: capture total wide-event duration"
```

### Task 8: Ruby HTTP client, bounded sender, store sink, and configuration

**Files:**
- Create: `lib/wide_event/store/client.rb`
- Create: `lib/wide_event/store/sender.rb`
- Create: `lib/wide_event/sinks/store.rb`
- Create: `test/store_client_test.rb`
- Create: `test/store_sender_test.rb`
- Create: `test/store_sink_test.rb`
- Modify: `lib/wide_event/configuration.rb`
- Modify: `lib/wide_event.rb`
- Modify: `test/configuration_test.rb`
- Modify: `test/wide_event_test.rb`

**Interfaces:**
- Consumes: canonical event strings from Task 7 and `/v1/events` from Task 3.
- Produces: `WideEvent::Store::Client#ingest(gzip_body) -> Ack`.
- Produces: `WideEvent::Store::Sender#enqueue(event_json) -> true|false` and `#shutdown(timeout: 2.0)`.
- Produces: `WideEvent::Sinks::Store#flush(attrs)` without changing the custom-sink interface.
- Extends: `WideEvent::Configuration` with store URL/service/environment/tokens and finite sender limits.

- [ ] **Step 1: Write failing client, queue, retry, and fork tests**

```ruby
test "enqueue never calls the HTTP client" do
  client = Minitest::Mock.new
  sender = build_sender(client: client, autostart: false)
  assert sender.enqueue(event_json)
  client.verify
end

test "PID mismatch discards inherited state before enqueue" do
  sender = build_sender(pid: 100, process_id: -> { 200 })
  sender.enqueue(event_json("child"))
  assert_equal [event_json("child")], sender.__queue_for_test
end

test "loss count remains pending until acknowledgement" do
  sender = build_sender(client: scripted_client(:timeout, :permanent_failure, :ok))
  overflow_queue(sender, by: 3)
  sender.flush_once
  assert_equal 3, sender.pending_drops
  sender.flush_once
  assert_equal 0, sender.pending_drops
end
```

- [ ] **Step 2: Run focused tests and verify failure**

Run: `bundle exec rake test`

Expected: FAIL because the client, sender, store sink, and configuration do not exist.

- [ ] **Step 3: Implement the standard-library HTTPS client**

Use `URI`, `Net::HTTP`, `OpenSSL`, `JSON`, and `Zlib`; require HTTPS except for explicit loopback test URLs. Set one-second open and two-second request timeouts. Send bearer ingest token, `Content-Type: application/json`, and `Content-Encoding: gzip`. Classify timeout/network, `429`, `507`, and `5xx` as retryable; other `4xx` as permanent; parse protocol/warnings only from bounded response bodies. Never include tokens or event bodies in errors.

- [ ] **Step 4: Implement the PID-aware dual-bound sender queue**

Under one mutex/condition variable, retain at most 1,000 canonical event strings or 8 MiB. Drop oldest until both bounds fit, increment `pending_drops`, and rate-limit one warning per minute. On every enqueue compare `Process.pid`; a mismatch discards inherited queue/thread/socket references and initializes child-local state before appending.

The worker seals at 100 events, 1 MiB uncompressed batch JSON, or one second; gzip and split again until each body is at most 512 KiB. Batch envelope contains protocol/batch UUID, configured service/environment, pending-drop snapshot with a stable `loss_report_id`, and event objects. It retries one batch with exponential jittered backoff capped at 30 seconds and age 24 hours. Subtract the carried drop snapshot only after a successful acknowledgement; if the batch is discarded, leave both count and report ID pending for the next batch. Continue accepting newer events subject to the same queue bounds.

- [ ] **Step 5: Wire the sink and lifecycle without changing hot-path guarantees**

`Sinks::Store#flush` builds the envelope and calls only `sender.enqueue`. Add `:store` resolution. Defaults are:

```ruby
@store_url = ENV["WIDE_EVENTS_URL"]
@enabled = @store_url.present? || ENV["OTEL_EXPORTER_OTLP_ENDPOINT"].present?
@sink = @store_url.present? ? :store : :otel
@store_service = ENV["WIDE_EVENTS_SERVICE"]
@store_environment = ENV["WIDE_EVENTS_ENVIRONMENT"]
@store_ingest_token = ENV["WIDE_EVENTS_INGEST_TOKEN"]
@store_query_token = ENV["WIDE_EVENTS_QUERY_TOKEN"]
```

Validate store configuration lazily on sink resolution and report errors through `handle_error`. `WideEvent.shutdown!` invokes a best-effort two-second flush; the Railtie registers it with `at_exit`. `reset_configuration!` shuts down an existing sender in tests to avoid leaked threads.

- [ ] **Step 6: Prove non-blocking and failure behavior, then commit**

Run: `bundle exec rake test`

Expected: PASS for no enqueue-time HTTP, both queue bounds, oldest-first drop, batch split/gzip, response classification, 24-hour discard, warning rate limit, loss reset on ack, PID change, shutdown deadline, HTTPS verification, drift warning, and never-raise sink behavior.

```bash
git add lib/wide_event lib/wide_event.rb test
git commit -m "feat: deliver wide events to the DuckDB store"
```

### Task 9: SQL client, terminal formatting, and Rails tasks

**Files:**
- Modify: `lib/wide_event/store/client.rb`
- Create: `lib/wide_event/store/query_result.rb`
- Create: `lib/wide_event/store/formatter.rb`
- Create: `lib/wide_event/tasks/store.rake`
- Modify: `lib/wide_event/railtie.rb`
- Create: `test/store_query_test.rb`
- Modify: `test/tasks_test.rb`

**Interfaces:**
- Consumes: `/v1/query` from Task 4 and query configuration from Task 8.
- Produces: `Client#query(sql, format: :json, deadline: 30) -> QueryResult|String`.
- Produces: `Formatter.table(QueryResult, io:)`, JSON, and CSV output.
- Produces: `bin/rails wide_events:sql` for interactive or piped SQL.

- [ ] **Step 1: Write failing query and task tests**

```ruby
test "query retries busy responses within the command deadline" do
  client = build_client(responses: [response(429, "Retry-After" => "2"), response(200, query_json)])
  assert_equal [[1]], client.query("SELECT 1").rows
end

test "sql task reads piped SQL and prints CSV" do
  ENV["WIDE_EVENTS_FORMAT"] = "csv"
  assert_output("count\n3\n") { invoke_store_task(stdin: StringIO.new("SELECT count(*) AS count FROM wide_events")) }
end
```

- [ ] **Step 2: Run query tests and verify failure**

Run: `bundle exec rake test`

Expected: FAIL because query methods and the task do not exist.

- [ ] **Step 3: Implement bounded query requests and busy retry**

Use the query token and the same TLS rules, but a 30-second overall monotonic deadline. Retry only `429` using bounded `Retry-After`; do not retry policy/limit errors. Limit response reads to 10 MiB plus one byte. Parse JSON columns/rows into immutable `QueryResult`; return CSV verbatim after validating status.

- [ ] **Step 4: Implement terminal table, JSON, CSV, and prompt behavior**

The table formatter calculates widths only from the bounded result, renders null as `NULL`, and never emits ANSI when output is not a TTY. `wide_events:sql` reads all stdin when piped. On a TTY, use standard-library `Readline` with prompt `wide_events> `, execute on a semicolon-terminated buffer, and exit on `quit`, `exit`, or EOF. `WIDE_EVENTS_FORMAT=table|json|csv` selects output; default is table.

- [ ] **Step 5: Load the task, test errors, and commit**

Load `tasks/store.rake` beside registry tasks. Missing URL/query token exits non-zero with exact setup guidance; busy/timeout/policy messages remain actionable and omit tokens/raw server bodies.

Run: `bundle exec rake test`

Expected: PASS for all formats, piped and interactive input, multiline SQL, busy retry/deadline, server errors, response cap, and missing configuration.

```bash
git add lib/wide_event/store lib/wide_event/tasks/store.rake lib/wide_event/railtie.rb test
git commit -m "feat: query wide events from Rails"
```

### Task 10: Idempotent Kamal accessory generator and durable secrets

**Files:**
- Create: `lib/wide_event/kamal/deploy_editor.rb`
- Create: `lib/wide_event/kamal/secrets_editor.rb`
- Create: `lib/generators/wide_events/store/store_generator.rb`
- Create: `lib/generators/wide_events/store/templates/accessory.yml.erb`
- Create: `test/kamal_deploy_editor_test.rb`
- Create: `test/kamal_secrets_editor_test.rb`
- Create: `test/store_generator_test.rb`
- Create: `test/fixtures/kamal/default_deploy.yml`
- Create: `test/fixtures/kamal/existing_sections.yml`
- Create: `test/fixtures/kamal/conflicting_deploy.yml`
- Modify: `test/generators_test.rb`

**Interfaces:**
- Consumes: store image/protocol from Tasks 1–6 and gem `WideEvent::VERSION`.
- Produces: `WideEvent::Kamal::DeployEditor#apply -> Result` with `changed?`, `snippet`, and conflicts.
- Produces: `WideEvent::Kamal::SecretsEditor#apply` without regenerating existing tokens.
- Produces: `bin/rails generate wide_events:store --host=IP --hostname=DNS --retention-days=30`.

- [ ] **Step 1: Write failing editor and generator tests**

```ruby
test "adds structured owned directory and minimum Kamal version idempotently" do
  first = editor(default_deploy).apply
  second = editor(first.content).apply
  assert_equal first.content, second.content
  parsed = YAML.safe_load(first.content)
  assert_equal "2.10.0", parsed.fetch("minimum_version")
  assert_equal "1000:1000", parsed.dig("accessories", "wide_events", "directories", 0, "owner")
end

test "refuses ERB and prints a complete snippet without partial writes" do
  result = editor("service: <%= ENV.fetch('SERVICE') %>\n").apply
  refute result.changed?
  assert_includes result.snippet, "wide_events:"
end
```

- [ ] **Step 2: Run generator tests and verify failure**

Run: `bundle exec rake test`

Expected: FAIL because the editors and store generator do not exist.

- [ ] **Step 3: Implement a comment-preserving, safely refusing deploy editor**

Parse with `Psych.safe_load` only to detect structure/conflicts; modify original lines so unrelated comments/order survive. Refuse before writing if the file contains ERB, YAML aliases/merge keys, tabs, duplicate root keys, non-mapping `env`/`accessories`/`aliases`, an existing non-owned `wide_events` accessory, conflicting `WIDE_EVENTS_*`, or a higher-complexity shape not covered by fixtures. For accepted files:

- set or raise root `minimum_version` to `2.10.0`, never lower a higher value;
- merge the three clear and two secret app variables under `env`;
- add exactly one `accessories.wide_events` block from the template;
- add `telemetry` and `wide-events-backup` aliases if absent;
- surround each insertion with `# BEGIN wide_events store` / `# END wide_events store` markers for deterministic updates.

On refusal, return the complete mergeable YAML snippet and leave the file byte-for-byte unchanged.

- [ ] **Step 4: Implement stable secret files and references**

Generate each missing token with `SecureRandom.hex(32)` into `.kamal/wide-events-ingest-token` and `.kamal/wide-events-query-token`, mode `0600`. Add exact gitignore lines without duplication. Add these stable assignments to `.kamal/secrets` without exposing values:

```bash
WIDE_EVENTS_INGEST_TOKEN=$(cat .kamal/wide-events-ingest-token)
WIDE_EVENTS_QUERY_TOKEN=$(cat .kamal/wide-events-query-token)
```

Never overwrite an existing token file; abort if it exists but is empty or group/world-readable.

- [ ] **Step 5: Implement the parameterized store generator**

Validate host as an IP or DNS name, telemetry hostname as DNS without scheme/path, and retention as a positive integer. Render image `ghcr.io/adammiribyan/wide-events-store:<WideEvent::VERSION>`, internal port 7421, owned host directory `/var/lib/<service>-wide-events`, 2 CPUs/2 GiB, proxy SSL/healthcheck, and the exact aliases from the design. Apply deploy and secret edits as one transaction: write temporary content only after every preflight succeeds.

- [ ] **Step 6: Run editor/generator tests and commit**

Run: `bundle exec rake test`

Expected: PASS for a Rails 8 default, existing root sections, comments, idempotence, higher minimum version, each conflict/refusal, stable tokens, permissions, gitignore, and exact pinned image.

```bash
git add lib/wide_event/kamal lib/generators/wide_events/store test
git commit -m "feat: generate the Kamal DuckDB accessory"
```

### Task 11: Setup checker, installed operating skill, and post-deploy verification

**Files:**
- Create: `lib/wide_event/setup/command_runner.rb`
- Create: `lib/wide_event/setup/checker.rb`
- Create: `lib/wide_event/tasks/setup.rake`
- Create: `test/setup_checker_test.rb`
- Modify: `lib/wide_event/railtie.rb`
- Create: `skills/setting-up-wide-events-store/SKILL.md`
- Modify: `skills/debugging-with-wide-events/SKILL.md`
- Modify: `lib/generators/wide_events/install/install_generator.rb`
- Modify: `lib/generators/wide_events/skills/skills_generator.rb`
- Modify: `test/generators_test.rb`
- Modify: `test/tasks_test.rb`

**Interfaces:**
- Consumes: generated Kamal configuration from Task 10 and client/query interfaces from Tasks 8–9.
- Produces: `WideEvent::Setup::Checker#run -> Result` with state `needs_dns`, `ready_to_deploy`, `healthy`, `degraded`, or `failed`, plus named checks and actionable messages.
- Produces: `bin/rails wide_events:setup:check` as the idempotent resume command.
- Produces: a third bundled skill installed by both `wide_events:install` and `wide_events:skills`.

- [ ] **Step 1: Write failing setup-state and skill-installation tests**

```ruby
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
```

- [ ] **Step 2: Run setup and generator tests to verify failure**

Run: `bundle exec rake test`

Expected: FAIL because the checker, task, and third skill do not exist.

- [ ] **Step 3: Implement deterministic command and configuration checks**

Use `Open3.capture3` with argv arrays and injected runner/clock/resolver/sleeper for tests. Run `kamal version`, parse `Gem::Version`, and fail below 2.10.0 before mutations. Run `kamal config` and preserve its actionable stderr. Preflight the host directory with:

```text
kamal server exec --hosts=<store-host> -- install -d -m 0750 -o 1000 -g 1000 /var/lib/<service>-wide-events
```

If root ownership is unavailable, print that exact one-time administrator command and stop before accessory boot.

- [ ] **Step 4: Implement bounded DNS and deployed endpoint checks**

Resolve A/AAAA records every three seconds for at most 120 monotonic seconds and require the configured store host address. On timeout, retain generated files and print only the record still needed plus `bin/rails wide_events:setup:check`. When DNS resolves, try `/up`: connection refusal/404 means `ready_to_deploy` and prints `kamal setup`; an HTTP response must have protocol 1 and `write_ready: true` or returns `degraded`/`failed`. A healthy deployed endpoint receives a synthetic request event with a unique ID, is queried back by ID, and prints its row plus the route-count/p95 query. This makes the same task useful before first deploy, after DNS propagation, and after deployment without flags.

- [ ] **Step 5: Write the store setup skill and update debugging guidance**

The setup skill asks only store SSH address, telemetry hostname, and retention days. It then invokes the store generator, shows the exact DNS record, runs the checker, runs `kamal setup` when state is `ready_to_deploy`, reruns the checker, and asks the user to exercise one real route. After that confirmation it executes a latest-real-request query through `kamal telemetry` and displays the captured row, route count, and p95. It must state that DNS timeout is resumable, backups are local-only, and query access uses a separate token. Update debugging guidance with `kamal telemetry`, `wide_events_store_status`, slow-route, job-failure, unhandled-exception, and deployment comparison SQL while retaining ClickHouse/log sections.

- [ ] **Step 6: Install all three skills through both generators**

Extract one copy routine used by `InstallGenerator` and `SkillsGenerator`. Assert exact installed paths:

```text
.claude/skills/instrumenting-wide-events/SKILL.md
.claude/skills/debugging-with-wide-events/SKILL.md
.claude/skills/setting-up-wide-events-store/SKILL.md
```

Repeated generator runs update owned skill files without duplicating `AGENTS.md` sections.

- [ ] **Step 7: Run setup/generator tests and commit**

Run: `bundle exec rake test`

Expected: PASS for old Kamal, invalid YAML, directory failure, DNS success/timeout, `/up` protocol/write state, synthetic round trip, safe messages, skill copy/refresh, and repeat runs.

```bash
git add lib/wide_event/setup lib/wide_event/tasks/setup.rake lib/wide_event/railtie.rb lib/generators skills test
git commit -m "feat: automate Wide Events store setup"
```

### Task 12: Cross-host integration, version gates, documentation, and release

**Files:**
- Create: `test/integration/store_round_trip_test.rb`
- Create: `test/integration/store_failure_test.rb`
- Create: `test/integration/kamal/config/deploy.yml`
- Create: `test/integration/kamal/.kamal/secrets`
- Create: `script/test-kamal-config`
- Create: `.github/workflows/release-store.yml`
- Modify: `.github/workflows/ci.yml`
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Modify: `CONTRIBUTING.md`
- Modify: `wide_events.gemspec`
- Modify: `bin/release`

**Interfaces:**
- Consumes: every prior task.
- Produces: one release gate proving Ruby/store protocol compatibility, Kamal configuration compatibility, image platforms, and packaged skills.
- Produces: tagged `ghcr.io/adammiribyan/wide-events-store:<version>` images matching the gem's generated pin.

- [ ] **Step 1: Write the failing cross-runtime tests**

Build the Go binary into a temporary directory, start it on a random loopback port with a temporary DuckDB directory, and create three independent Ruby senders named `web-1`, `web-2`, and `jobs-1`. Enqueue request/job events concurrently, shut down all senders, and assert one SQL query sees every source with no duplicates. Add tests for response loss after commit, restart/reopen, store outage until queue overflow, disk-degraded `507` with successful query, and backup through the Unix-socket CLI.

- [ ] **Step 2: Run integration tests and verify any missing behavior fails**

Run: `bundle exec ruby -Itest -e 'Dir["test/integration/**/*_test.rb"].sort.each { |f| require File.expand_path(f) }'`

Expected: FAIL until all runtime wiring and fixture helpers are complete.

- [ ] **Step 3: Add an exact Kamal compatibility script and fixture**

The fixture must contain the real structured directory object. `script/test-kamal-config` installs an isolated requested Kamal gem, runs `VERSION=test kamal config`, and asserts normalized output contains:

```yaml
local: /var/lib/myapp-wide-events
remote: /var/lib/wide-events
mode: '0750'
owner: 1000:1000
```

CI runs the script for 2.10.0 and 2.12.0 expecting success, then 2.9.0 expecting rejection. It also asserts proxy host/SSL/app port and `/up` healthcheck fields.

- [ ] **Step 4: Add integration and packaging jobs to CI**

After Ruby and Go unit jobs, build the store binary and run integration tests on Ubuntu. Build the gem and inspect its file list for all store Ruby files and all three skills. Keep the existing Ruby/Rails matrix unchanged. Run RuboCop and `gofmt -w`/`go vet`; fail CI on a dirty diff after generated-format checks.

- [ ] **Step 5: Publish the store image from version tags**

On `v*` tags, authenticate to GHCR with `GITHUB_TOKEN`, build `store/Dockerfile` for `linux/amd64,linux/arm64`, and push both the exact semver tag and a matching immutable Git SHA tag. Do not publish `latest`. Make `bin/release` verify that `WideEvent::VERSION`, the tag, and the generated store image pin agree before pushing the gem/tag.

- [ ] **Step 6: Rewrite first-run and operations documentation**

Add the default five-to-ten-minute flow, exact `deploy.yml` fragment, DNS step, query examples, token separation, disk sizing, `wide_events_store_status`, backup/restore limitations, upgrade/rollback, and OTel/log alternatives. Create `## Unreleased` in `CHANGELOG.md` describing the companion store. Extend contributing instructions with Go, Docker, integration, and Kamal fixture commands.

- [ ] **Step 7: Run the complete release gate and commit**

Run: `bundle exec rake test`

Run: `bin/rubocop`

Run: `cd store && go test ./... && go vet ./...`

Run: `bundle exec ruby -Itest -e 'Dir["test/integration/**/*_test.rb"].sort.each { |f| require File.expand_path(f) }'`

Run: `script/test-kamal-config 2.10.0 success && script/test-kamal-config 2.12.0 success && script/test-kamal-config 2.9.0 failure`

Run: `gem build wide_events.gemspec`

Expected: all commands pass; the gem contains all three skills; the standard Kamal fixture preserves owner/mode; the integration suite combines two web sources and one job source; no generated diff remains.

```bash
git add .github README.md CHANGELOG.md CONTRIBUTING.md wide_events.gemspec bin/release script test/integration
git commit -m "release: integrate the bundled DuckDB store"
```
