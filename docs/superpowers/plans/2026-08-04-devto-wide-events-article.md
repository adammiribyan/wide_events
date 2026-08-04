# Wide Events DEV Article Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a publication-ready DEV article that explains how Wide Events gives Rails coding agents compact, trustworthy production context, using verified and anonymized production evidence.

**Architecture:** The article opens with a real diagnostic case, explains the event model through minimal Rails code, then proves the agent workflow with a second asynchronous LLM example. Verification is split into factual, privacy, publication-policy, and prose checks so the final copy can be published without unsupported claims.

**Tech Stack:** Markdown, Ruby 3.2+, Rails 7.1+, OpenTelemetry, ClickHouse/ClickStack, DEV front matter

## Global Constraints

- Use no em dashes.
- Keep the production app anonymous.
- Exclude identifiers, URLs, SQL statements, exception messages, prompts, completions, and other user content.
- Label the measurements as production snapshots from August 4, 2026.
- Describe byte-size comparisons as payload density, not measured model-token savings.
- State that asynchronous flows produce one event per request or job, not one event for the complete user action.
- Present the root event as an index into traces, not a replacement for traces.
- Link to `https://github.com/adammiribyan/wide_events`.

---

### Task 1: Verify the source pack and publication constraints

**Files:**
- Read: `README.md`
- Read: `lib/wide_event.rb`
- Read: `lib/wide_event/middleware.rb`
- Read: `lib/wide_event/job_middleware.rb`
- Read: `lib/wide_event/registry/defaults.yml`
- Read: `skills/instrumenting-wide-events/SKILL.md`
- Read: `skills/debugging-with-wide-events/SKILL.md`
- Read: `docs/devto-article-brief.md`

**Interfaces:**
- Consumes: approved article brief and current gem implementation
- Produces: a checked list of claims, code signatures, minimum versions, and publication rules used by the draft

- [ ] **Step 1: Verify public API signatures**

Check the implementation and tests for `WideEvent.set`, `WideEvent.phase`,
`WideEvent.error!`, `WideEvent.active?`, and the install generator. Copy only
signatures and examples that exist in the current release.

- [ ] **Step 2: Verify compatibility and transport claims**

Confirm the Ruby and Rails version floors, OpenTelemetry behavior, log-sink
fallback, error semantics, and registry strict mode from the README and source.

- [ ] **Step 3: Verify current DEV publication guidance**

Read DEV's current posting and AI-assistance guidance. Add any required
front-matter or disclosure to the draft, using DEV's own wording only when a
short quotation is necessary.

- [ ] **Step 4: Confirm the production evidence ledger**

Check every number in `docs/devto-article-brief.md` against the captured query
results in the working conversation. Mark the evidence as a dated snapshot and
avoid extrapolating beyond the two inspected traces.

### Task 2: Draft the production case study and thesis

**Files:**
- Create: `docs/devto-wide-events-for-rails-agents.md`

**Interfaces:**
- Consumes: the verified source pack from Task 1
- Produces: DEV front matter, opening case study, thesis, and wide-event model

- [ ] **Step 1: Add DEV front matter**

Use the title `One Rails request, one event: production context for coding agents`,
a concise description, `published: false`, and no more than four relevant tags.

- [ ] **Step 2: Write the 30-second request opening**

Present the sanitized root fields in a compact JSON block. Explain that 445.58
ms of Postgres time cannot account for a 30,010.7 ms request, while 29,383.73
ms of HTTP time can. Then show that trace drill-down found a 28,901.9 ms POST.

- [ ] **Step 3: Establish the agent-context thesis**

Contrast repository context with production context. Explain that an agent can
query one application-shaped row first, then inspect a trace only after it has
a concrete hypothesis.

- [ ] **Step 4: Define a wide event precisely**

Define it as the OpenTelemetry root span marked `main=true` and enriched across
the request or job. Explain the JSON-log fallback and the one-unit-of-work grain.

### Task 3: Draft the Rails implementation and production LLM example

**Files:**
- Modify: `docs/devto-wide-events-for-rails-agents.md`

**Interfaces:**
- Consumes: verified API signatures from Task 1 and article foundation from Task 2
- Produces: install, instrumentation, registry, privacy, and asynchronous-flow sections

- [ ] **Step 1: Write the minimal installation path**

Show the Gemfile entry, bundle install command, generator command, and the first
JSON event path without requiring tracing.

- [ ] **Step 2: Show domain instrumentation**

Use short Ruby examples for request context, semantic-search outcome fields,
`WideEvent.phase`, and `WideEvent.error!`. Explain why instrumentation belongs
at the point where the application knows the answer.

- [ ] **Step 3: Explain the registry contract**

Show a small YAML registry excerpt. Explain strict tests, CI validation, docs
generation, types, ownership, and PII review without reproducing the full schema.

- [ ] **Step 4: Write the asynchronous LLM case study**

Show the initiating request and generation job as two linked events. Include
model, timing, token, cost, and context-shape metadata while stating that no
prompt or completion content is recorded.

- [ ] **Step 5: Explain the agent skills and verification loop**

Describe the generated instrumentation and debugging skills. Give a concrete
loop: inspect registry, query a narrow cohort, inspect one trace, change code,
deploy, then verify the new build SHA in production.

### Task 4: Add tradeoffs and run editorial QA

**Files:**
- Modify: `docs/devto-wide-events-for-rails-agents.md`

**Interfaces:**
- Consumes: complete draft from Tasks 2 and 3
- Produces: publication-ready article with verified links and no unsupported claims

- [ ] **Step 1: Add limitations and failure modes**

Cover high-cardinality storage, registry maintenance, asynchronous event joins,
trace drill-down, and expected framework errors polluting naive unnamed-error
queries. Include a corrected query pattern rather than presenting the naive
query as a perfect incident queue.

- [ ] **Step 2: Add the repository link and closing invitation**

Link the first mention of the gem or the final installation section to
`https://github.com/adammiribyan/wide_events`. Ask for feedback on the registry
workflow, naming conventions, and agent verification loop.

- [ ] **Step 3: Run mechanical checks**

Search for em dashes, leaked production identifiers, app names, request IDs,
account IDs, URLs, SQL text, exception messages, and unqualified token-efficiency
claims. Validate Markdown structure and links visible in the repository.

- [ ] **Step 4: Run factual checks**

Compare every version, method name, generator command, field name, numeric
measurement, and SQL key against source or the evidence ledger. Correct any
disagreement in the article rather than weakening the source.

- [ ] **Step 5: Perform a skeptical editorial pass**

Remove repeated claims, feature-list prose, and anything that sounds like a
launch announcement. Make the production investigation carry the argument.
Keep the article useful even for readers who do not install the gem.

- [ ] **Step 6: Commit the publication-ready draft**

Run `git diff --check`, stage only the article and plan files, and commit with
the message `docs: draft agent-focused Wide Events article`.
