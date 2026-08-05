-- comments cannot hide mutation
/* read this */ DELETE FROM wide_events
----
 DELETE FROM wide_events
----
WITH doomed AS (DELETE FROM wide_events RETURNING *) SELECT * FROM doomed
----
EXPLAIN SELECT 1
----
EXPLAIN ANALYZE DELETE FROM wide_events
----
PRAGMA database_size
----
CALL checkpoint()
----
SET memory_limit = '2GB'
----
LOAD httpfs
----
EXPORT DATABASE '/tmp/export'
----
ATTACH '/tmp/other.db'
----
COPY wide_events TO '/tmp/events.csv'
----
SELECT nextval('query_policy_sequence')
----
SELECT * FROM query('DELETE FROM wide_events RETURNING id')
----
SELECT * FROM query('SELECT nextval(''query_policy_sequence'')')
----
SELECT pg_sleep(1)
