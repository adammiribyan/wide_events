# Wide Events store

`wide-events` is the single-process DuckDB service for durable event ingest,
bounded read-only queries, local backups, and retention. The process owns the
database; operational commands reach that owner through its private Unix
socket and never open DuckDB themselves.

## Run the server

The required settings are:

- `WIDE_EVENTS_DATABASE` — DuckDB file, normally `/var/lib/wide-events/wide-events.duckdb`
- `WIDE_EVENTS_SERVICE` and `WIDE_EVENTS_ENVIRONMENT` — stored tenancy
- `WIDE_EVENTS_INGEST_TOKEN` and `WIDE_EVENTS_QUERY_TOKEN` — distinct tokens of at least 64 hexadecimal characters

The HTTP server listens on `0.0.0.0:7421` by default. Its routes are
`GET /up`, `POST /v1/events`, and `POST /v1/query`. The private admin socket
defaults to `/run/wide-events/admin.sock`.

For a container, persist `/var/lib/wide-events`. The image runs as uid/gid
`1000:1000`, so the mounted directory must be writable by that identity.

```sh
docker run --rm -p 7421:7421 \
  -v wide-events-data:/var/lib/wide-events \
  -e WIDE_EVENTS_DATABASE=/var/lib/wide-events/wide-events.duckdb \
  -e WIDE_EVENTS_SERVICE=checkout \
  -e WIDE_EVENTS_ENVIRONMENT=production \
  -e WIDE_EVENTS_INGEST_TOKEN="$INGEST_TOKEN" \
  -e WIDE_EVENTS_QUERY_TOKEN="$QUERY_TOKEN" \
  wide-events:local
```

## Create a backup

Run the helper in the same container namespace as the server so it can reach
the private socket:

```sh
wide-events admin backup
```

It prints the absolute path of a verified backup. Backups are stored beside
the database under `backups/`; the newest three verified copies are retained.

## Build

From the repository root:

```sh
docker build --build-arg VERSION=1.2.3 -f store/Dockerfile -t wide-events:1.2.3 .
```
