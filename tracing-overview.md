# Distributed Tracing — Presentation Rewrite (rough overview)

> Examples and tables get filled from the ECP application docs later.

## 1. Introduction — the goal
- Everyone in North America works on different projects and has no visibility beyond the microservices they work on.
- The goal: give everyone visibility across all the microservices, so they can quickly understand what is happening inside any service, with the documentation to go with it.
- This is practical only through distributed tracing. When every application registers traces alongside logs, the decision-making process inside each service becomes fully visible.
- Even before looking into the code, people should be able to tell from the distributed traces alone:
  - what is going wrong with the application
  - what has to be fixed
  - which teams to contact

## 2. Why — the missing data in the transaction table
- The missing database is not a DBMS. It is the Tempo database, the transaction table where distributed traces are registered (in Grafana, Tempo stores the traces and TraceQL shows them as this table).
- Today span registration into that table exists, but it is very minimal. The traces inside each application are missing.
- Show the plain transaction table as it was created at the start: only the HTTP request, service name, start, duration and trace id. Nothing about who, why or what was decided, and no correlation id.

| Start | Trace id | Service | Span (HTTP request) | Duration |
|---|---|---|---|---|
| 09:02:00.000 | a7c1e1 | UECP-PO-MANAGEMENT | GET /v1/po/accounts/can | 4.8 s |
| 09:02:00.030 | a7c1e1 | platform-billing-account-service | POST /billing/v2/billingAccounts/account-search/company | 4.7 s |

- When that table gets filled, we get real distributed tracing, and that is how issues get found.
- In a monolith, logs were the plain source for everything. In microservices, logs only explain; distributed tracing is how you find the issue.
- Role of log: Loki. Role of trace: Tempo.

| Logs (Loki) | Distributed tracing (Tempo) |
|---|---|
| Explain the unique issue in detail | Why |
| | Where |
| | How |
| | Impact radius |


## 3. A span's attributes are short messages
- Explain what a span is and how the span trace happens.
- How a span is placed: show the per-request span view, one bar per span with its duration, the failed span in red (screenshot: span bars from the old presentation; use the ECP chain PO → PBAS → database from v2 §3).
- How one attribute is placed on a span: the code line `RequestSpan.set("pbas.usermail", "xyz@mail.com")` puts `pbas.usermail = xyz@mail.com` on that request's span. Show the line, then the span with the new key on it. (The animation is for understanding only: in the code, `RequestSpan.set` places attributes on the request span.)
- What the spans hold, question by question (screenshot: v2 §5 "When, to whom, why, where, how"):

| Question | Attributes | Example |
|---|---|---|
| When | span start, duration | 09:02:00.030 · 4.7 s |
| To whom | user.id, org_id, channel_type, actor.present | u-30418 · org-55012 · portal · false |
| Who called | caller.service, caller.source | uecp-po-management · source-app-header |
| Where | service name, span name, parent span | platform-billing-account-service · POST …/account-search/company |
| Why | error_code, error_key, error.type, failure keys | EQ-1306162 · ACCOUNT_SEARCH_FAILED · SQLTimeoutException |
| How | path of spans, upstream.name, handled, pbas.outcome | PO → PBAS → database · pbas · mapped |
| Description | Loki: the one error log line, found by correlation_id | full exception text + stack trace |

- Syntax analogy: logs use `log.debug`, `log.error`; tracing uses `RequestSpan.set(...)` the same way. Show side by side.
- Log vs trace, for the same moment:
  - the logs: a big scrollable chunk, four services mixed with other requests (screenshot: v2 §7 Loki panel)
  - the traces: the same failure as short notes (attributes) on the spans
- How short attribute messages are read: who, where, why, how, when in a few seconds, and every note is searchable.

## 4. Issue-finding with the table
- This is where error debugging is shown, on the transaction table (screenshot: v2 §12, every attribute a column, query buttons, click a correlation id to follow it).
- One case: a create-billing-account request that starts in PBAS (platform-billing-account-service). Every service involved shows in the same table, because every row carries the same correlation id: authorization in PBAS, DIH, ATS, and the Kafka listener. That correlation id on every row is what joins them, and it is what is missing today.
- Example A — PBAS refuses: the user is not authorised to create a billing account. The table shows the authorization decision and the error code on the PBAS span.
- Example B — creation passes, PBAS calls DIH, the message id is registered; the Kafka listener picks the message up (maybe days later) and fails at one step. The point: one query on the correlation id still brings all of it back in one table.
- Rough tables below. Values are made up (Tempo is empty); DIH values are imaginary. Attribute names are the ones in v2 and the guides.

Example A — `{ span.correlation_id = "CID-8d14f2" }`

| Start | Trace id | Service | Span | Status | Duration | correlation_id | authn.result | authz.decision | authz.reason | error_code |
|---|---|---|---|---|---|---|---|---|---|---|
| 10:14:03.000 | b71e20 | platform-billing-account-service | POST create billing account | error | 0.2 s | CID-8d14f2 | ok | deny | no create permission | EQ-13xxxxx (not authorised) |

Example B — `{ span.correlation_id = "CID-3f9a07" }`

| Start | Trace id | Service | Span | Status | Duration | correlation_id | authz.decision | messaging.message.id | dih.outcome | error_code |
|---|---|---|---|---|---|---|---|---|---|---|
| Day 1 10:20:00.000 | c40d91 | platform-billing-account-service | POST create billing account | ok | 0.9 s | CID-3f9a07 | allow | | | |
| Day 1 10:20:00.600 | c40d91 | platform-billing-account-service | publish oracle-ban-create-req (to DIH) | ok | 0.1 s | CID-3f9a07 | | msg-77a2 | | |
| Day 1 10:20:01.000 | c40d91 | DIH (imaginary) | consume oracle-ban-create-req | ok | 0.3 s | CID-3f9a07 | | msg-77a2 | accepted | |
| [ATS row] | | ATS | [from our ATS traces] | | | CID-3f9a07 | | | | |
| Day 3 08:05:12.000 | e51b36 | platform-billing-account-service | DIHAccountMessageV2Listener receive | error | 0.4 s | CID-3f9a07 | | msg-91c4 | | EQ-13xxxxx (step that failed) |

## 5. Alerts on combination
- In a microservice environment, logs cannot efficiently find a combination of issues. Issues might happen on a combination of different steps in different services. This can be derived using the structured data of the transaction table.
- Place a mesh diagram with several services.
- Alerts will also be set based on a combination of events in multiple services. Just like an SQL query, TraceQL will trigger on the combination to find the error.
- Graph representation of each event using a timeline and TraceQL.

## 6. Predictive analytics
- This opens up new possibilities in analytics and monitoring, with high-level precision.
- Answers like how many of the users got genuine issues.
- How many were impacted, and for what duration, when the service was down.
- What are the chances we might expect a failure in certain applications where retries are high.

## 7. How can we bring this up immediately?
- Currently we have no data at all in the transaction DB. Traces are a much lower-level threat than logs.
- So I have created .md files with detailed tracing guides. They follow the OpenTelemetry standards to place attributes on the spans of any selected project.
- The README section has the how-to-use guide: give it to Claude and tell it to place the traces as per the skill. Claude places the traces; the team reviews the change before it ships.
- Additionally, I have added a feature to create a Confluence page. It places the documentation for all the traces created, and self-explains why each was created and how we have to use it.

## 8. Error codes — mandatory on every error, registered on the span
- Today we already have the error code format: `EQ-` + the 4-digit service module + the 3-digit code.
- On every error, we register that error code, return it in the response, and register it on the trace.
- Our tracing skill (the MD guides) places the error code on hard failures: when there is a hard failure, it puts that error code on the span and in the response.
