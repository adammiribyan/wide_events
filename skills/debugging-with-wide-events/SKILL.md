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

- **ClickHouse (HyperDX/ClickStack default schema):** the `otel_traces`
  table; wide-event attributes are in the `SpanAttributes` map, resource
  attributes (build, host, environment) in `ResourceAttributes`. Adapt table
  and column names to the backend at hand.
- **Log sink deployments:** one JSON object per line; query with your log
  store's JSON operators.

Filter by environment when staging and production share storage.

## Symptom to query

Start from the unit of work, not from logs. One GROUP BY usually replaces a
cross-tool investigation.

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

## Standing queries

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
route/job filtered to the new `service.version` and compare against the
prior build: error rate, p50, query count, and the specific attribute your
change was supposed to move. State the numbers in your summary.
