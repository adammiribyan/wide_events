---
name: setting-up-wide-events-store
description: Use when a human asks to set up, deploy, or finish deploying the bundled Wide Events DuckDB store (the Kamal accessory that receives production telemetry). Covers the store generator, DNS, `kamal setup`, and post-deploy verification end to end.
---

# Setting up the Wide Events store

This is the guided path from "wide_events is installed" to "production
requests are queryable." It wraps deterministic generator/checker code -
never hand-edit `config/deploy.yml` or `.kamal/secrets` yourself; every
mutation here is idempotent, so re-running any step (including this whole
skill) is always safe.

## What to ask for

Exactly three things, nothing else:

1. the SSH address (IP or DNS name) of the VM that will run the store;
2. the public DNS hostname end users/agents will query through (no scheme,
   no path); and
3. retention in days (default 30 if the human has no preference).

## Steps

1. **Generate the accessory.**

   ```bash
   bin/rails generate wide_events:store --host=<ssh-address> --hostname=<telemetry-hostname> --retention-days=<n>
   ```

   This edits `config/deploy.yml` (accessory, app environment, `telemetry`
   and `wide-events-backup` aliases) and writes durable, gitignored tokens to
   `.kamal/wide-events-ingest-token` and `.kamal/wide-events-query-token`. If
   it refuses instead of editing (ambiguous YAML, a conflicting existing
   key), it prints a complete snippet - merge that by hand, then continue.

2. **Show the exact DNS record.** Tell the human to point the telemetry
   hostname at the store host now, before deploying, so propagation happens
   in parallel with the next steps instead of after them.

3. **Run the checker.**

   ```bash
   bin/rails wide_events:setup:check
   ```

   This is the same idempotent command at every stage - before the first
   deploy, while waiting on DNS, and after `kamal setup` - so there is never
   a different command to remember. It reports one of five states:

   - `failed` - a Kamal version/config problem, or a host-directory
     permission problem. Fix what the message says (it includes the exact
     one-time administrator command if root is needed to create the data
     directory) and re-run.
   - `needs_dns` - DNS has not resolved yet. **This is expected and
     resumable, not an error.** Nothing generated is reverted or lost;
     re-running the same command later picks up exactly where it left off.
     Wait for propagation and run it again.
   - `ready_to_deploy` - DNS is good and the store isn't answering yet.
     Proceed to step 4.
   - `degraded` - the store is routable but not accepting writes (usually
     low disk space). Do not proceed; investigate on the host.
   - `healthy` - already fully deployed and working (useful if you're
     re-running this skill against an existing setup).

4. **Deploy, once the checker says `ready_to_deploy`.**

   ```bash
   kamal setup
   ```

5. **Re-run the checker.** `bin/rails wide_events:setup:check` again. On a
   healthy deploy it sends one synthetic event, queries it back by ID, and
   prints that row plus a route-count/p95-latency query - proof the whole
   path (ingest token, query token, TLS, DuckDB) works before any real
   traffic exists.

6. **Ask the human to exercise one real route** of the app now that the
   store is live (any request or job is fine).

7. **After they confirm, query it back:**

   ```bash
   kamal telemetry
   ```

   opens an interactive SQL prompt against the store (or pipe SQL to it
   non-interactively). Run a query for the most recent real request, e.g.:

   ```sql
   SELECT id, occurred_at, route, duration_ms, status
   FROM wide_events
   ORDER BY occurred_at DESC
   LIMIT 1;
   ```

   Display that captured row, then the route count and p95 latency for the
   exercised route, and state both numbers back to the human.

## Things to tell the human, unprompted

- **DNS timeouts are resumable.** If `wide_events:setup:check` reports
  `needs_dns`, that is normal while waiting on propagation, not a failure -
  running the exact same command again later continues correctly.
- **Backups are local-only.** The accessory takes automatic daily local
  backups (kept three most recent) via the `wide-events-backup` alias; this
  protects against logical mistakes and bad upgrades, not loss of the VM or
  volume itself. There is no off-host backup in v1.
- **Query access uses a separate token from ingestion.** `.kamal/wide-events-
  query-token` is read-only and distinct from `.kamal/wide-events-ingest-
  token`; sharing query access (e.g. with a teammate or another agent) never
  requires sharing the ability to write events.

## Where full querying guidance lives

Once the store is live, use the `debugging-with-wide-events` skill for the
standing queries (slow routes, job failures, unhandled exceptions, deploy
comparisons) and the `wide_events_store_status` operational view.
