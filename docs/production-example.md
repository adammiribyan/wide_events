# Production example

Wide Events was extracted from a multi-tenant Rails app subject to
healthcare privacy rules, where it runs in staging and production today:
one row per request and job execution, exported over OTLP into
self-hosted ClickStack. Everything below is adapted from that app's
instrumentation. The attribute names are real; the IDs and values are
fictional, and the event is a composite assembled to show the attribute
families in one place.

## One event

A coach-facing request that runs a hybrid search and drafts a reply with
an LLM lands as one row:

```json
{
  "main": true,
  "http.request.id": "1c4f9c2e-77b0-4b0e-9d3a-52d1a8c90b17",
  "http.route.controller": "MessagesController#index",
  "http.response.status_code": 200,
  "user.id": 48213,
  "user.type": "User",
  "user.account.id": 3072,
  "session.id": "0198c5f2-6e5a-7cc3-9f4e-b1d20b6a4a11",
  "feature_flag.semantic_search": true,
  "feature_flag.assist_drafts": "variant-b",
  "search.semantic": true,
  "search.result_count": 12,
  "search.semantic_fetched": 80,
  "search.semantic_kept": 9,
  "search.semantic_top_cosine": 0.6114,
  "assist.generation_id": 90417,
  "assist.kind": "reply_draft",
  "assist.status": "completed",
  "assist.prompt_version": 3,
  "assist.model": "gpt-4.1-mini",
  "assist.input_tokens": 3480,
  "assist.output_tokens": 214,
  "assist.latency_ms": 1874.22,
  "assist.cost_usd": 0.0019,
  "stats.postgres_query_count": 38,
  "stats.postgres_query_duration_ms": 64.8,
  "stats.openai_call_count": 1,
  "db.duration_ms": 61.2,
  "view.duration_ms": 18.4,
  "user_agent.browser": "Chrome",
  "user_agent.os": "macOS",
  "uptime_sec": 15840,
  "error": false
}
```

One query against this row answers questions that would otherwise span
four systems: which account saw it, which flag variants were active, how
good the semantic search leg was, what the LLM call cost, and how many
Postgres queries the whole thing took. On a failure row you'd see
`error: true` plus `exception.type`, and `exception.slug` when the
failure was rescued and named with `error!`; an unhandled exception gets
`error: true` with no slug, which is what makes the standing query below
work.

## The instrumentation behind it

Four patterns from the production app, adapted to the gem's API.

**User and account on every request.** A controller concern runs after
authentication and stamps the context no SDK can add automatically.
Every attribute downstream becomes sliceable by account and user type:

```ruby
# app/controllers/concerns/set_wide_event_context.rb
# Included in ApplicationController; runs after auth populates Current.
module SetWideEventContext
  extend ActiveSupport::Concern

  included do
    before_action :set_wide_event_context
  end

  private
    def set_wide_event_context
      return unless WideEvent.active?

      WideEvent.set(
        "user.id" => Current.person&.id,
        "user.type" => Current.person.class.name,
        "user.account.id" => Current.account&.id,
        # Client input, so cap the length.
        "session.id" => request.headers["X-POSTHOG-SESSION-ID"].presence&.first(64)
      )
    end
end
```

The real concern also stamps `user.impersonated` and
`user.impersonated_by` when an admin is acting as someone else, so
support sessions never pollute per-user analysis.

**Feature flags at evaluation time.** The app wraps its flag SDK in one
helper, so every evaluation lands on the event and any symptom can be
sliced by flag state without joining to the flag service:

```ruby
def self.enabled?(flag_key, person = Current.person)
  result = evaluate_locally(flag_key, person)   # your flag SDK here
  WideEvent.set("feature_flag.#{flag_key}" => result)
  result
rescue => e
  WideEvent.error!(slug: "err-flag-eval-failed", exception: e, expected: true)
  false
end
```

**Search quality, including graceful degradation.** The controller
records what the search returned; the search model records when the
semantic leg fell back to keyword-only:

```ruby
# Controller: what the search returned
WideEvent.set(
  "search.result_count" => @results.size,
  "search.semantic" => search.semantic?
)

# Search model: an embedding failure degrades to keyword-only
# instead of breaking, and the event records that it happened
def query_embedding
  Embedding.embed_query(query)
rescue Embedding::Error => error
  WideEvent.error!(slug: "err-search-embedding-failed", exception: error, expected: true)
  nil
end
```

The `search.semantic_fetched` / `search.semantic_kept` /
`search.semantic_top_cosine` attributes accumulate across search legs.
They exist so the similarity cutoffs can be recalibrated against live
traffic instead of guesses: the app's cosine floors were set by querying
these attributes, not by tuning in development.

**LLM generations: tokens, latency, cost; content never.** Every
generation records its telemetry on whichever request or job triggered
it, on both terminal transitions (completed and failed):

```ruby
def record_generation_telemetry(model:, input_tokens:, output_tokens:, latency:, error: nil, expected: nil)
  WideEvent.set(
    "assist.generation_id" => id,
    "assist.kind" => kind,
    "assist.model" => model,
    "assist.input_tokens" => input_tokens,
    "assist.output_tokens" => output_tokens,
    "assist.latency_ms" => (latency * 1000.0).round(2),
    "assist.cost_usd" => cost_usd(model, input_tokens, output_tokens),
    "assist.status" => status
  )
  WideEvent.error!(slug: "err-assist-generation-failed", exception: error, expected: expected) if error
end
```

Prompt and completion content never leave the app; the deployment
handles health data, so the policy is counts and sizes only. The real
version also stamps prompt-shape metrics (how many thread messages,
notes, and goals the prompt was built from), so draft quality can be
segmented against context richness later.

Jobs get the same treatment with `phase`: a report-upload job wraps its
work in `WideEvent.phase("report_upload")` and sets
`report_upload.status` to success or failure on the way out, so slow and
failing uploads show up in the same table as everything else.

## Standing queries

Two queries from that deployment worth adopting on day one. First, the
uninstrumented-failure to-do list: every row is an exception nobody
wrote a rescue for, in priority order (the
[cookbook](clickhouse-hyperdx.md) has the full version):

```sql
SELECT SpanAttributes['exception.type'] AS type,
       SpanAttributes['http.route.controller'] AS controller,
       count() AS occurrences
FROM otel_traces
WHERE SpanAttributes['main'] = 'true'
  AND SpanAttributes['error'] = 'true'
  AND SpanAttributes['exception.slug'] = ''
GROUP BY type, controller ORDER BY occurrences DESC
```

Second, LLM cost and reliability per model and kind, computed from
request and job rows alone, no separate LLM observability pipeline:

```sql
SELECT SpanAttributes['assist.model'] AS model,
       SpanAttributes['assist.kind'] AS kind,
       count() AS generations,
       countIf(SpanAttributes['assist.status'] = 'failed') AS failures,
       round(quantile(0.5)(toFloat64OrZero(SpanAttributes['assist.latency_ms']))) AS p50_latency_ms,
       round(sum(toFloat64OrZero(SpanAttributes['assist.cost_usd'])), 2) AS cost_usd
FROM otel_traces
WHERE SpanAttributes['main'] = 'true'
  AND SpanAttributes['assist.generation_id'] != ''
  AND Timestamp > now() - INTERVAL 7 DAY
GROUP BY model, kind ORDER BY cost_usd DESC
```

## Footnote: sharing context with product analytics

The same app forwards a whitelisted slice of the open wide event onto
every outgoing PostHog event through the client's `before_send` hook:
route, user and account IDs, flag results (renamed to `$feature/<key>`),
and the session ID (renamed to `$session_id`, which links backend events
to the session replay they happened in). Exception messages are excluded
on purpose, since they can quote user input. The result is that
telemetry and product analytics share identifiers without sharing
content, and `WideEvent.peek` is the only integration point.
