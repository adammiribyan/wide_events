# DEV article brief: production context for Rails coding agents

## Audience

Rails developers who use coding agents and have found that those agents can
read the repository but cannot efficiently reason about production behavior.

## Promise

Show that one application-shaped event per request or job gives an agent a
compact index into production. The event does not replace traces. It tells the
agent which trace to inspect and what question to ask next.

## Thesis

Most Rails observability data is optimized for collection rather than for the
limited context window of an investigator. Wide events move the important
request, tenant, feature, dependency, domain, and outcome context onto the
OpenTelemetry root span. An agent can start with one row, form a hypothesis,
and drill into child spans only when needed.

## Opening case study

Use an anonymized production semantic-search request:

- Total request duration: 30,010.7 ms
- Result count: 20
- Semantic candidates fetched and retained: 48 and 4
- Top cosine score: 0.5012
- Postgres: 87 queries totaling 445.58 ms
- HTTP: 8 calls totaling 29,383.73 ms
- Full trace: 82 spans, 437 span attributes, 20,261 bytes of attribute JSON
- Root event: 40 attributes, 1,420 bytes of attribute JSON
- Trace drill-down: one downstream POST took 28,901.9 ms

The event immediately rules out Postgres as the primary bottleneck. The trace
then identifies the slow dependency.

## Second case study

Use an anonymized asynchronous LLM reply-draft flow. Be precise that it is two
units of work and therefore two events:

- Initiating request: 230.6 ms, status 204, 59 Postgres queries
- Generation job: 3,159.7 ms
- Model latency: 2,995.96 ms
- Tokens: 3,035 input and 112 output
- Cost: $0.009268
- Context shape: 12,553 input characters, plus counts for messages, notes,
  goals, and todos
- Output shape: 522 characters
- Content policy: no prompt or completion content
- Job trace: 35 spans and 288 attributes
- Job root event: 42 attributes and 1,384 bytes of attribute JSON, compared
  with 11,534 bytes across the trace

Explain that an opaque generation identifier links the request and job without
putting user content into telemetry.

## Supporting production evidence

In the seven-day production slice ending August 4, 2026:

- 1,185,563 root events
- 1,185,563 distinct trace IDs
- Zero duplicate root rows
- 976,160 controller requests
- 198,587 jobs
- 10,816 other root units, such as unmatched or non-controller work
- Newest event was ten seconds old when queried

Use only the grain and uniqueness facts if the traffic volume distracts from
the argument.

## Measurement notes

- Source: the production `otel_traces` table in self-hosted ClickStack
- Root-event filter: `SpanAttributes['main'] = 'true'`
- Environment filter: `ResourceAttributes['deployment.environment'] = 'production'`
- Duration conversion: OpenTelemetry nanoseconds divided by 1,000,000
- Attribute bytes: `length(toJSONString(SpanAttributes))` in ClickHouse
- Density comparison: the root event's span-attribute map versus the sum of
  span-attribute maps across the selected trace
- Privacy: queries projected only classifications, counts, timings, model
  metadata, and attribute sizes. They excluded user and account identifiers,
  request identifiers, URLs, SQL statements, exception messages, prompt text,
  completion text, and other free text.

The byte comparison is a concrete payload-density observation from two traces,
not a general benchmark and not a model-token measurement.

## Design points

1. Build on the OpenTelemetry root span already exported.
2. Provide the same event as one JSON log line when tracing is unavailable.
3. Make instrumentation safe: inactive calls are no-ops and telemetry never
   raises into application code.
4. Treat the attribute registry as executable schema, validated in tests and
   CI, with documentation generated from the same source.
5. Name handled errors with stable slugs while preserving unhandled exception
   visibility.
6. Keep sensitive content out. Record identifiers only when policy permits,
   and prefer counts, sizes, classifications, and outcome metadata.
7. Install agent skills that teach agents how to add instrumentation and query
   it, including post-deployment verification by build SHA.

## Honest limitations

- A wide event is an index into a trace, not a replacement for one.
- The byte comparisons measure serialized span attributes, not model tokens.
  Describe the events as context-dense or a smaller starting surface. Do not
  claim a measured token reduction.
- One event per unit of work means an asynchronous user action may produce a
  request event and one or more job events.
- The naive unnamed-exception queue contains expected framework noise. In the
  inspected data it was dominated by routing errors. Production queries should
  exclude known noise or classify expected failures explicitly.
- High-cardinality dimensions require a suitable backend and a retention plan.
- Registry discipline is work. That cost is intentional because it gives
  humans and agents a dependable vocabulary.

## Article flow

1. The 30-second request and the first useful query
2. Why ordinary telemetry is awkward for coding agents
3. What a wide event is
4. Rails setup and domain instrumentation
5. The asynchronous LLM example
6. Registry, safety, and privacy
7. The agent debugging loop
8. Limits and tradeoffs
9. Installation and repository link

## Voice and formatting

- First person, concrete, and technically skeptical
- No em dashes
- Avoid marketing phrases such as agent-native unless immediately explained
- Prefer exact examples and short code blocks over lists of features
- State that the gem was extracted from a production Rails app
- State that production examples are anonymized and contain no user content
- Disclose that the article was prepared with AI assistance if the publication
  requires or encourages that disclosure

## Working title

One Rails request, one event: production context for coding agents
