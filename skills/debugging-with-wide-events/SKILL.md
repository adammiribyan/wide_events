---
name: debugging-with-wide-events
description: Use when investigating production behavior by querying wide events (a slow route, an error spike, a suspicious deploy, "why is this happening for account X"). Covers the data model, the symptom-to-query workflow, and standing queries worth running.
---

# Debugging with wide events

Every request and job execution is one row: the OTel root span with
`main = true`, carrying route, user/account ids, build SHA, status, error
state, per-dependency query counts, cache hits, feature flags, phase
timings, and whatever domain attributes the app set. Cross-request analysis
never needs child spans: filter `main = true` and aggregate.

## Where the rows live

- **Wide Events store (bundled DuckDB accessory):** the `wide_events` table.
  `route`, `job_class`, `status`, `error`, `request_id`, `duration_ms`,
  `kind`, `deployment`, and `source` are promoted columns; everything else
  the app set is in the `attributes` JSON column, read with `attributes->>
  'key.name'`. Query it with `kamal telemetry` (an interactive prompt, or
  pipe SQL to it non-interactively) - never SSH in and open the database
  file directly.
- **ClickHouse (HyperDX/ClickStack default schema):** the `otel_traces`
  table; wide-event attributes are in the `SpanAttributes` map, resource
  attributes (build, host, environment) in `ResourceAttributes`. Adapt table
  and column names to the backend at hand.
- **Log sink deployments:** one JSON object per line; query with your log
  store's JSON operators.

Filter by environment when staging and production share storage (the
DuckDB store is single-tenant per deployment, so this mainly applies to the
ClickHouse/log backends).

## Operational health (DuckDB store)

`SELECT * FROM wide_events_store_status;` is the one-row operational view:
row counts for the last hour/day, oldest/newest received event, database and
free-disk bytes, write-readiness and its reason, last checkpoint/retention/
backup times and errors, and the store/protocol/DuckDB versions. Check this
first when `bin/rails wide_events:setup:check` reports `degraded`, or before
trusting a "no events" result - it tells you whether anything has arrived at
all.

## Standing queries (DuckDB store)

"The reports route is slow":

```sql
SELECT route,
       count(*) AS requests,
       quantile_cont(duration_ms, 0.5) AS p50_ms,
       quantile_cont(duration_ms, 0.95) AS p95_ms
FROM wide_events
WHERE route = 'ReportsController#index'
  AND occurred_at > current_timestamp - INTERVAL 1 DAY
GROUP BY route
ORDER BY p95_ms DESC
```

"Which jobs are failing":

```sql
SELECT job_class, count(*) AS failures
FROM wide_events
WHERE kind = 'job' AND error
  AND occurred_at > current_timestamp - INTERVAL 1 DAY
GROUP BY job_class
ORDER BY failures DESC
```

"Unhandled exceptions" - `error` true with no `exception.slug` set is the
standing "instrument this rescue" query (see instrumenting-wide-events):

```sql
SELECT route, job_class, attributes->>'exception.type' AS exception_type, count(*) AS occurrences
FROM wide_events
WHERE error
  AND (attributes->>'exception.slug' IS NULL OR attributes->>'exception.slug' = '')
  AND occurred_at > current_timestamp - INTERVAL 1 DAY
GROUP BY route, job_class, exception_type
ORDER BY occurrences DESC
```

"Did the deploy change anything" - group by `deployment` (the Kamal version
string) and compare:

```sql
SELECT deployment,
       count(*) AS requests,
       avg(duration_ms) AS avg_ms,
       quantile_cont(duration_ms, 0.95) AS p95_ms,
       sum(error::INT) AS errors
FROM wide_events
WHERE route = 'ReportsController#index'
  AND occurred_at > current_timestamp - INTERVAL 1 DAY
GROUP BY deployment
ORDER BY deployment DESC
```

"Why did this specific request fail" - filter by `request_id` and read
every promoted column and attribute of the one row before reaching for logs.

## Symptom to query (ClickHouse)

Start from the unit of work, not from logs. One GROUP BY usually replaces a
cross-tool investigation. (On the DuckDB store, use the equivalent queries
above instead - same symptoms, promoted columns instead of `SpanAttributes`.)

"The reports route is slow":

```sql
SELECT SpanAttributes['user.account.id'] AS account,
       ResourceAttributes['service.version'] AS build,
       count() AS requests,
       quantile(0.5)(Duration/1e6) AS p50_ms,
       avg(toFloat64OrZero(SpanAttributes['stats.postgres_query_count'])) AS avg_queries
FROM otel_traces
WHERE SpanAttributes['main'] = 'true'
  AND SpanAttributes['http.route.controller'] = 'ReportsController#index'
  AND Timestamp > now() - INTERVAL 1 DAY
GROUP BY account, build
ORDER BY p50_ms DESC
LIMIT 20
```

"Did the deploy change anything": group the affected route by
`ResourceAttributes['service.version']` and compare p50/p95 and
`stats.postgres_query_count` across builds.

"Why did this specific request fail": filter by
`SpanAttributes['http.request.id']` and read every attribute of the one row
before reaching for logs or the trace waterfall.

## Standing queries (ClickHouse)

- **Unnamed exception paths**: `SpanAttributes['error'] = 'true' AND
  SpanAttributes['exception.slug'] = '' AND SpanAttributes['exception.type'] != ''`:
  exceptions that escaped without a named handled failure. Investigate each
  path; add a rescue and `error!` only when handling it is intentional (see the
  instrumenting-wide-events skill).
- **Expected-error drift**: count by `exception.slug` week over week; a
  slug trending up is a quiet regression.
- **Query-count outliers**: max `stats.postgres_query_count` per route; the
  top entries are N+1 candidates with route names and account IDs attached.
- **Cold starts**: correlate latency with `uptime_sec_log10` to separate
  deploy warm-up from real regressions.

## Verifying your own fix

After a deploy, don't ask a human to check a dashboard. Query the affected
route/job filtered to the new build (`deployment` on the DuckDB store,
`ResourceAttributes['service.version']` on ClickHouse) and compare against
the prior build: error rate, p50/p95, query count, and the specific
attribute your change was supposed to move. State the numbers in your
summary.
