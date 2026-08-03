# ClickHouse + HyperDX quickstart and query cookbook

Wide events ride your OTel traces, so any OTLP backend works. ClickHouse with
HyperDX (together: [ClickStack](https://clickhouse.com/use-cases/observability))
is a proven self-hosted pairing: columnar storage built for high-cardinality
aggregation, a search UI on top, both open source.

## Local quickstart

One container runs ClickHouse, HyperDX, and an OTLP collector:

```bash
docker run -p 8080:8080 -p 4317:4317 -p 4318:4318 \
  docker.hyperdx.io/hyperdx/hyperdx-all-in-one
```

Point the app at it and start it:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
OTEL_SERVICE_NAME=myapp \
bin/rails server
```

Open [http://localhost:8080](http://localhost:8080), pick the Traces source, and filter
`SpanAttributes['main'] = 'true'`. Every request and job in your app is a row.
For production deployments (retention, disk sizing, auth), see the
[ClickStack docs](https://clickhouse.com/docs/use-cases/observability/clickstack/getting-started).

## Cookbook

Wide-event attributes live in the `SpanAttributes` map of `otel_traces`;
per-process attributes (service version, host) in `ResourceAttributes`. Map
values are strings: compare against `'true'`, cast with `toFloat64OrZero` /
`toInt32OrZero` before math.

Which routes are slow, doing how many queries:

```sql
SELECT SpanAttributes['http.route.controller'] AS controller,
       count() AS requests,
       round(quantile(0.5)(Duration/1e6)) AS p50_ms,
       round(quantile(0.95)(Duration/1e6)) AS p95_ms,
       round(avg(toFloat64OrZero(SpanAttributes['stats.postgres_query_count']))) AS avg_queries
FROM otel_traces
WHERE SpanAttributes['main'] = 'true'
  AND Timestamp > now() - INTERVAL 1 DAY
GROUP BY controller ORDER BY p50_ms DESC LIMIT 20
```

Did the deploy change anything (compare builds):

```sql
SELECT ResourceAttributes['service.version'] AS build,
       count() AS requests,
       round(quantile(0.5)(Duration/1e6)) AS p50_ms,
       countIf(SpanAttributes['error'] = 'true') AS errors
FROM otel_traces
WHERE SpanAttributes['main'] = 'true'
  AND SpanAttributes['http.route.controller'] = 'ReportsController#index'
  AND Timestamp > now() - INTERVAL 2 DAY
GROUP BY build ORDER BY build
```

Who is affected (slice any symptom by account):

```sql
SELECT SpanAttributes['user.account.id'] AS account,
       count() AS requests,
       round(quantile(0.95)(Duration/1e6)) AS p95_ms
FROM otel_traces
WHERE SpanAttributes['main'] = 'true'
  AND SpanAttributes['http.route.controller'] = 'TodosController#index'
  AND Timestamp > now() - INTERVAL 1 DAY
GROUP BY account ORDER BY p95_ms DESC LIMIT 20
```

Unnamed exception paths (unhandled exceptions have no slug):

```sql
SELECT SpanAttributes['exception.type'] AS type,
       SpanAttributes['http.route.controller'] AS controller,
       count() AS occurrences
FROM otel_traces
WHERE SpanAttributes['main'] = 'true'
  AND SpanAttributes['error'] = 'true'
  AND SpanAttributes['exception.slug'] = ''
  AND SpanAttributes['exception.type'] != ''
  AND Timestamp > now() - INTERVAL 7 DAY
GROUP BY type, controller ORDER BY occurrences DESC
```

Expected-error drift (a slug trending up is a quiet regression):

```sql
SELECT toStartOfDay(Timestamp) AS day,
       SpanAttributes['exception.slug'] AS slug,
       count() AS occurrences
FROM otel_traces
WHERE SpanAttributes['main'] = 'true'
  AND SpanAttributes['exception.slug'] != ''
  AND Timestamp > now() - INTERVAL 14 DAY
GROUP BY day, slug ORDER BY day, occurrences DESC
```

Queue pressure per job class:

```sql
SELECT SpanAttributes['job.class'] AS job,
       count() AS executions,
       round(quantile(0.5)(toFloat64OrZero(SpanAttributes['job.queue_latency_ms']))) AS p50_wait_ms,
       countIf(SpanAttributes['error'] = 'true') AS failures
FROM otel_traces
WHERE SpanAttributes['main'] = 'true'
  AND SpanAttributes['job.class'] != ''
  AND Timestamp > now() - INTERVAL 1 DAY
GROUP BY job ORDER BY p50_wait_ms DESC
```

One request, every attribute (before reaching for logs or the waterfall):

```sql
SELECT SpanAttributes
FROM otel_traces
WHERE SpanAttributes['http.request.id'] = '<request id>'
  AND SpanAttributes['main'] = 'true'
```
