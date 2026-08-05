+# Task 5 Report: Disk pressure, retention, backups, and local administration

## Status

Complete. The store now enforces binding filesystem limits, coordinates finite
writer/query maintenance admission, performs daily retention and verified local
backups through the owner process, and exposes backup only over a private Unix
socket.

## Implementation

- Added `disk.StatFSProbe`, the binding write floor
  `max(2 GiB, volume/10)`, and query spill cap
  `min(4 GiB, volume/4)`. Every write preflight publishes current database
  bytes and available filesystem bytes.
- Added maintenance admission hooks whose ownership follows accepted ingestion
  through commit completion. Disk degradation blocks writes with the typed
  ingest `507` path while queries remain admitted. Actual OS/DuckDB no-space
  errors publish the same degraded state.
- Added a finite, UTC-daily manager. Retention closes only writer admission,
  drains accepted commits, deletes events strictly before the configured
  receipt cutoff, prunes both ledgers strictly before seven days, checkpoints,
  records status, reopens writes, and then attempts one automatic backup.
- Backups preflight before and after the exclusive writer/query gate. Required
  free bytes are `max(generic floor, database bytes + max(512 MiB, 10% DB))`.
  Typed failures carry required and available bytes and preserve existing
  backups.
- The owner database performs `ATTACH`, `COPY FROM DATABASE`, backup
  checkpoint, and detach. The final file is fsynced, reopened read-only in the
  same process, checked for current schema and readable row count, directory
  metadata is fsynced, and only the three newest verified regular files remain.
  Partial or failed-verification candidates are removed.
- DuckDB is allowed to access only the private backup directory while general
  external access stays disabled and locked. Public query policy now rejects
  file readers before execution, including paths inside that internal
  allowlist.
- Added newline-delimited bounded JSON administration over an absolute,
  symlink-safe Unix socket. The default directory/socket use uid/gid 1000 and
  mode `0600`; the client supports only `admin backup`, prints the resulting
  path, and reports typed capacity bytes. The admin package has no DuckDB or
  database-path dependency.
- Scheduler, gate waits, admin connections, writer notifications, cleanup
  status writes, and shutdown joins all have finite bounds.

## TDD evidence

Initial RED:

```text
GOCACHE=/tmp/wide-events-go-cache go test ./internal/disk ./internal/maintenance ./internal/admin -v

undefined: WriteFloor
undefined: StatFSProbe
undefined: newGate
undefined: ErrInsufficientBackupSpace
undefined: Listen
undefined: RunClient
FAIL
```

Subsequent RED cycles demonstrated:

- the second backup preflight and typed generic-floor error were absent;
- disk degradation did not clear after a later generic-floor pass;
- storage lacked transactional retention and owner-only backup operations;
- DuckDB's locked external-access setting rejected the internal `ATTACH`;
- query spill still used the configurable 64 MiB test value instead of the
  binding volume-derived cap;
- writer/query admission hooks did not exist;
- filename sorting retained four files under clock-shaped name skew;
- scheduler `Close` returned before joining its goroutine; and
- the narrow internal backup allowlist made `read_csv` executable from the
  public query path.

Each RED was followed by the minimal implementation and a focused GREEN run.
The internal attach conflict was resolved with DuckDB's documented directory
allowlist while the new policy regression test keeps that directory
unreachable to public queries.

## Final verification

```text
GOCACHE=/tmp/wide-events-go-cache go test ./... -count=1
PASS: all 9 packages

GOCACHE=/tmp/wide-events-go-cache go test -race ./... -count=1
PASS: all 9 packages, no race reports

GOCACHE=/tmp/wide-events-go-cache go vet ./...
PASS

git diff --check
PASS
```

The race linker emitted the pre-existing nonfatal macOS DuckDB
`LC_DYSYMTAB` warnings; every package exited successfully.

## Self-review

- The admin helper communicates only through the Unix socket and cannot receive
  or open a database path.
- Retention gates writes only; disk pressure leaves queries routable; backup
  gates both write and query traffic while health remains represented by the
  existing typed maintenance state.
- Both backup capacity checks use freshly statted database bytes and filesystem
  availability. No candidate is created before the first check, and the second
  check runs after drain/checkpoint.
- Backup cleanup targets only the generated candidate, ignores symlinks during
  retention, preserves existing copies on preflight failure, and fsyncs before
  recording success.
- The operation slot prevents overlapping scheduled/manual maintenance; daily
  state is UTC keyed; shutdown cancels and joins the finite scheduler.
- No public administration route was added and no helper imports DuckDB.
