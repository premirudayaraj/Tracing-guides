# Instruction — derive the sample TraceQL result tables for the tracing presentation

**Who this is for:** a Claude that already has the tracing guides (00–09 + README) checked out
alongside the target service code. This does NOT restate how traces are placed — the guides own that.
Your job is to read the code THROUGH those guides and produce the sample result tables the
presentation needs.

**Do NOT query Tempo.** Tempo is empty today. Everything here is *derived from the code*, not fetched.
Say plainly, in each output, that the rows are derived samples.

---

## 1. What to pick — a real connected chain (non-negotiable)

Pick **three services in one real call chain**: A calls B, B calls C. Not three unrelated services.
- Confirm the chain from the code: A has an outbound client to B (guide 04), B has one to C.
- Confirm the **correlation id header** ({{CORRELATION_HEADER}} from README §0) is read on entry and
  forwarded on every outbound call, so the SAME id travels A → B → C.
- If you cannot find a real 3-service chain, use the longest real chain you can prove (2 is
  acceptable, invented links are not). State what you found.

The whole point: searching that one correlation id shows it travelling across every service. A broken
chain kills the story — verify the id actually propagates before you produce anything.

## 2. How to derive the spans

For each service in the chain, apply the guides you already have:
- Classify it (00), run that role's analysis, and list every span the rules place: the request/door
  span (01 §3), auth-chain and request decisions (01 §4–5), each outbound handover (04), each retry
  span (04 §4.4), producer/consumer spans (05), job roots (06), UI/BFF spans (07).
- For each span, read off the attributes the guides put on it: correlation_id, caller.service,
  peer.service, error_code (03), the <dependency>.<action>.failure key and failure_count,
  retry.count / retry.outcome.
- Give the file:line for every span so the table is auditable. Mark what you READ from the code
  versus what you INFERRED.

## 3. What to output — the result tables, exactly these columns

The presentation renders TraceQL result rows. Produce a Markdown table per query, one row per span,
these columns in this order:

`Start | Trace id | correlation_id | Service | Span | caller.service | Status | error_code | Failure | retry.count | Duration`

Rules for values:
- Same **Trace id** and **correlation_id** across every row of one request — that is the chain.
- **Service** in call order (A, then B, then C); **Span** = span kind + operation (e.g.
  `POST /billing/charge`).
- **Status** ok/error; **error_code** only on the service that failed the request (03 §2.1); blank (—)
  elsewhere.
- **Failure** short and readable: `timeout`, `HTTP 502`, `retries exhausted (3)` — never a stack trace.
- **Duration** realistic and consistent: a parent's duration ≥ the sum of its children; a 5 s timeout
  reads 5.0 s; retries stack (3 × 5 s ≈ 15 s on the parent).

Produce these query + table pairs, all on the SAME chain and ids so they cross-reference:
1. `{ span.correlation_id = "<id>" }` — the full happy-path chain (all ok).
2. `{ span.correlation_id = "<id2>" }` — the SAME chain, one hop failing, with retries and an error code.
3. `{ status = error }` — a handful of failing rows across the three services (mix of causes).
4. `{ resource.service.name = "<B>" && span.retry.count > 0 }` — the retry rows for the middle service.
5. `{ resource.service.name = "<C>" } | count_over_time() by (span.caller.service, status)` — the
   service-map table for C: who calls it and ok/error counts. (Note: needs TraceQL metrics enabled.)

## 4. Also produce, for the Confluence slide — one row per trace, per service

For EACH of the three services, a table of every trace the service makes: one row per operation
against a dependency (inbound entry, each outbound call from guide 04, each Kafka/Rabbit publish or
consume from guide 05, each job from guide 06). This mirrors guide 09 §4 sections 5 (Outbound
handovers) and 6 (Messaging), written in plain words. Columns, in this order:

`Trace | Dependency | Purpose | Outcomes you'll see | Retry | What it tells you`

- **Trace** — a plain name for the operation, e.g. `Call to DAH: <action> completed`,
  `Kafka publish: <topic> completed`, `Kafka consume: <topic> received`. Not a class name, not a raw span.
- **Dependency** — the peer.service or topic/exchange, exactly as the code names it.
- **Purpose** — what the call is for, in the service's business terms (09 §4 rendering rules).
- **Outcomes you'll see** — the outcome / failure values the guides put on it (`<dep>.outcome`
  values, `<dependency>.<action>.failure` values, ack / receipt).
- **Retry** — `up to N` from the retry span (04 §4.4) if one exists, else `—`.
- **What it tells you** — one sentence a support engineer acts on: what a failure here means and who owns it.

Add file:line for each row in a separate evidence list, not in the table.

## 5. Also produce, for the retry-trend slide

A small day-by-day count for one retrying call: `Day | Mon…Sun | requests that retried`, a rising
trend. Derived/illustrative — say so.

## 6. Hand back

- The five query+table pairs, the three per-service Confluence tables and the trend table, in one Markdown file.
- A short list: the three services, the chain (A→B→C), the correlation id field you verified, and the
  file:line evidence that the id propagates.
- One line stating these are samples derived from code, to replace the placeholder tables in the
  presentation ("What we can achieve with Tempo" and "How to achieve this immediately").

Keep it tight. No prose essays — the tables and the evidence list are the deliverable.
