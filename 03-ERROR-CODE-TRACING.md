# 03 — Error Code Tracing: The Outcome on the Span, in the Log Line and in the Response

> **Scope:** the *placement* of failure information so that any failure — a denied request, a failed upstream, a non-fatal error the code caught and carried on past, an empty 200 — can be found in `{{TRACE_BACKEND}}`, in `{{LOG_BACKEND}}` and in the client response with one identifier. Building the codes is `02`; the request span and decisions are `01`; outbound calls `04`; messages `05`; jobs `06`; browser/Node `07`. Placeholders are defined in `README.md §0`.
>
> **How to use with an AI assistant:** *"Wire the outcomes of `<service>` into the span and the logs per 03-ERROR-CODE-TRACING.md."*
>
> Legend: **MUST** = rule; **already** = usually in current code; ➕ = add; ⚠ = confirm at runtime.

---

## 0. Reference patterns and the OpenTelemetry rules this guide follows

| What | Observed / rule |
|---|---|
| Error → response body (the only place that sets `errorCode` + `correlationId`) | one `@RestControllerAdvice` at `HIGHEST_PRECEDENCE` per service (status from the application exception or 500, body = error array, correlation id from the request header or MDC); a policy advice that builds the code from the HTTP status (02 §7.1 defect); advices that turn `*NoRecordException` into a **200 empty typed body**; services that rely on the in-house library's global handler and have no advice of their own; a WebFlux `ErrorWebExceptionHandler` in the authorisation service (name-based codes, trace id as correlation id) |
| Dependency outcome mapping | `onErrorMap` / `handleErrorResponse` in WebClient services, a client utility in the pass-through service, `catch (RestClientResponseException)` blocks in RestTemplate services; one client that reports a socket timeout as `status=PROCESSED` |
| What the agent already puts on spans | `http.response.status_code`; span status `ERROR` for 5xx on SERVER spans and for 4xx/5xx on CLIENT spans; `error.type`; `exception.type/message/stacktrace` on any span whose instrumented method throws |
| Hidden-200 outcomes | no-record advices; "permission lookup failed → proceed without filter"; enrichment `.exceptionally` returning un-enriched rows; a support-case creation failure absorbed by `onErrorReturn`; an e-mail dispatch wrapped in a catch-all; a timeout reported as processed; consumer error handlers that skip (05 K3) |
| OpenTelemetry *Recording errors* semantic conventions | status `Error` only when the operation failed; "errors that were retried or handled … SHOULD NOT be recorded"; `error.type` only on failure; the same exception is not recorded twice; server-side 4xx leaves status unset, client-side 4xx sets it; 404 is an error only when the resource was expected. Span events are being deprecated in favour of logs — decisions are attributes, exceptions are `recordException` (still supported) or a log line with `trace_id` |

---

## 1. The three sinks and the one key

| Sink | Carries today | ➕ Should carry | Joined by |
|---|---|---|---|
| **Response body** | `errorCode`, `errorMessage`, `details`, `additionalInfo[]`, `correlationId`; real HTTP status (02 §7) | `{{CORRELATION_HEADER}}` response header on every response (01 §3) | `correlationId` |
| **Span** | `http.response.status_code`, status (5xx), `error.type`, `exception.*` (agent), `http.request.header.<correlation header>` (captured) | `error_code` (hard failures only, §2.1), `error_key`, `<dependency>.<action>.failure` + `failure_count` (non-fatal errors, §2.1), `retry.count` (on per-purpose retry spans), `correlation_id`, `upstream.name/status/code`, `handled`, `<dep>.outcome`, `no_record`, `fallback`, `authz.*` (01), `consumer.action`/`publish.result` (05), `job.status` (06) | `correlation_id` |
| **Logs** | one ERROR/WARN line at the point of failure with the MDC correlation id and the in-app `traceId` | the agent's `trace_id`/`span_id` in the pattern so the line opens the trace; the common MDC key `correlationId` | `correlationId`, `trace_id` |

Support flow: client shows `{{ERR_PREFIX}}-4210465` + correlation id → trace query `{ span.correlation_id = "…" }` → the root span carries `error_code={{ERR_PREFIX}}-4210465`, `upstream.name=account-platform`, `upstream.status=503`, `upstream.code={{ERR_PREFIX}}-4310500`, `handled=mapped`; the CLIENT child has `peer.service=account-platform`, `http.response.status_code=503` → log query by `correlationId` shows the masked upstream body logged once in the mapping method.

---

## 2. Rule A — one writer of the outcome: the exception handler

Each service has exactly one component that turns an exception into the response. **That component sets `error_code` on the root span**, once, through one helper:

```java
// ➕ tracing/SpanOutcome.java — one per service (or in the shared REST library); no other class writes error_code
public final class SpanOutcome {
  public static void record(String errorCode, String errorKey, int httpStatus, Throwable cause) {
    Span span = RequestSpan.root();                                       // 01 §6; no-op span when the agent is off
    span.setAttribute("error_code", errorCode);                // "{{ERR_PREFIX}}-4210465" / "<PRODUCT>-<NAME>" — only on failures
    span.setAttribute("error_key", errorKey);                  // "ACCOUNT_PLATFORM_API_ERROR" — the catalogue key, never the message text
    span.setAttribute("error.type", cause != null ? cause.getClass().getName() : errorCode);   // standard: exception class, or the domain code when there is no exception
    if (httpStatus >= 500) {                                              // the request really failed
      if (cause != null) span.recordException(cause);                    // once — not again in a lower layer
      span.setStatus(StatusCode.ERROR, errorCode);
    }                                                                     // 4xx (validation, 401, 403, 404): expected outcome — code + error.type, status untouched
  }
  /** a NON-FATAL error (caught, logged, the request carried on): NO error code here — one key named after the failed call, <call>.failure = short reason (§2.1) */
  public static void nonFatal(String call, Throwable cause) { nonFatal(call, Reason.of(cause), cause); }
  public static void nonFatal(String call, String reason, Throwable cause) {   // call = "<dependency>.<action>", e.g. "support-case.create", "account-events.publish", "notification.email-send"
    RequestSpan.failure(call, reason);                                    // 01 §6: support-case.create.failure = "HTTP 503" and failure_count on the root
    log.warn("non-fatal error call={} reason={} correlationId={}", call, reason, MDC.get("correlationId"), cause);   // the exception goes to the log, once, here — not onto the span (the agent's CLIENT/JDBC child already carries error.type for the call that failed)
  }
  // Reason.of(e): WebClientResponseException / RestClientResponseException → "HTTP <status>"; timeouts → "timeout"; ConnectException → "connection refused"; otherwise the exception's simple class name — never the message text
}
```
* **5xx → `recordException` + `ERROR`; 4xx → attributes only.** A denied request (`403`, `authz.decision=deny`) or a validation failure is a *correct* answer, not an error; `status=error` and error-rate panels then stay meaningful (the agent already marks 5xx SERVER spans; this adds the *code*).
* Where to call it: the service's `@RestControllerAdvice` handlers (application-exception handler with its status and first `errorCode`; catch-all with `{{ERR_PREFIX}}-<module>030`/`500`; validation handlers with the validation code + `validation.field_count`); the policy advice; the WebFlux `ErrorWebExceptionHandler`. A service that has **no advice of its own** (the library's global handler builds the body) → tag **at the throw site** (`SpanOutcome.record(code, key, status, null)` right before `throw new AppException(...)`) and add a `HandlerInterceptor.afterCompletion` that, for `response.getStatus() >= 500`, records the exception and sets `ERROR` (read the status — `ex` is null when the library handler consumed it).
* `error_code` exists **only** on failures; `{ span.error_code != nil }` is the failure set. One code per span; multiple validation failures = one code + `validation.field_count`.
* Upstream codes never become `error_code` — they are `upstream.code` (rule B); the client saw *this* service's code.
* Never record the same exception twice (agent + advice + client layer): the agent records it on the CLIENT/JDBC child that threw; the handler records it on the root once. In practice: `SpanOutcome.record` is called **only** from the exception handler (or throw sites); `SpanOutcome.nonFatal` **only** where a failure is caught and the program continues (04 §4.3, 05 K1/K3); `catch` blocks that rethrow record nothing. `05` (`@Recover`, consumer error path) and `06` (`JobRun`) call the same two methods — no other class writes `error_code`.

### 2.1 The hard-failure rule — where the minted code goes, and where it does not

The error code from `02` is the mark of a **hard failure**: the program itself treated the operation as failed (the request answers with an error body; the listener gives the record up to retry/DLT; the job run ends `failed`). It is written **only** there, by `SpanOutcome.record`. Three cases follow from that:

| Case | What the program did | What goes on the span | What does NOT |
|---|---|---|---|
| **Hard failure** | stopped / failed the operation | `error_code`, `error_key`, `error.type`; 5xx → `recordException` + `ERROR` | — |
| **Non-fatal error** | caught it and the request carried on (`onErrorReturn`, catch-all log, `.exceptionally`, fallback, "treated as processed") | one key per failed call, `<dependency>.<action>.failure` = short reason (`support-case.create.failure = "HTTP 503"`, `notification.email-send.failure = "timeout"`, `account-events.publish.failure = "retries exhausted (3)"`), + `failure_count`, + the decision facets of 04 (`<dep>.outcome=ignored\|fallback\|treated_as_success`, `handled`, `upstream.status/code`) | **no minted code**, no `error_key`, no `recordException`, status untouched — the client got a success and the span says so |
| **Retried call** | tried again | a **count** on the call's own per-purpose span (`retry.count`, 04 §4.4) | no code for the attempts; a code only if the *final* outcome is a hard failure (handled by the two rows above) |

* **The key names the call; the value says what went wrong.** `<dependency>` is the `peer.service` name (for messaging: the topic/exchange; for a job: the job name), `<action>` is the purpose of the call — the same words as `call.purpose` and the retry span name (04 §4.4), so the call attributes, the retry span and the failure key of one call read alike. The value is short and readable (`HTTP 503`, `timeout`, `connection refused`, `retries exhausted (3)`, or the exception's simple class name) — never the exception message, never an id.
* **One key per call, never merged.** Two different calls to the same dependency get two keys (`billing-hub.register-user.failure`, `billing-hub.fetch-invoices.failure`). The same call failing twice in one request keeps the first reason. One call failing for many items inside a loop is **counted** (`job.items_failed`), not keyed per item.
* **`failure_count`** is the number of `.failure` keys on the span. It exists because a query cannot match attribute *names* by pattern: `{ span.failure_count > 0 }` finds every request that carried on past a failure; the keys themselves say which calls. Key segments contain hyphens, so quote the key in TraceQL: `span."support-case.create.failure"`.
* **The same problem may appear twice, in two places, and that is correct**: the service that hard-failed carries the full `{{ERR_PREFIX}}-` code on *its* span (it answered with an error); the caller that caught that answer and carried on carries `<dependency>.<action>.failure` (plus `upstream.code` = the code it received). Reading the trace top-down: failure key on the parent → code on the child.
* Consequently `{ span.error_code != nil }` is exactly the set of spans whose service failed an operation, across the estate; `{ span.failure_count > 0 }` is the set of requests that succeeded while a call under them did not.
* **Naming.** The key names the call that failed and ends in `.failure`; nothing in it describes what the code did with the exception — that is the `handled` / `<dep>.outcome` value `ignored` (caught, logged, carried on).

What one trace looks like (an order is created; the support-case service is down; the order API catches the failure and still answers 201):
```
order-api     SERVER  POST /orders                 status OK   http.response.status_code=201
                      support-case.create.failure = "HTTP 503"   failure_count = 1   ← the call that failed, on the request that carried on
                      support-case.outcome = ignored   handled = ignored   upstream.status = 503   upstream.code = {{ERR_PREFIX}}-5120460
                      (no error_code — the order API did not fail)
  └─ CLIENT  POST support-case                     error.type=503                               ← agent
support-case  SERVER  POST /v1/tickets             status ERROR   http.response.status_code=503
                      error_code = {{ERR_PREFIX}}-5120460   error_key = TICKET_DB_ERROR   ← the real code, on the service that really failed
```

---

## 3. Rule B — dependency outcomes are registered where the call is made (04 §4.3)

```java
// existing handleErrorResponse / onErrorMap / catch(RestClientResponseException) — the CLIENT span has already ended; write on the ROOT
RequestSpan.set("account-platform.outcome", "mapped");            // per dependency (default "pending" written before the call, "ok" on success)
RequestSpan.setIfAbsent("upstream.name", "account-platform");    // first failing dependency wins; == peer.service of the CLIENT child
RequestSpan.setIfAbsent("upstream.status", response.statusCode().value());
RequestSpan.setIfAbsent("upstream.code", UpstreamCode.extract(body));   // first "errorCode" in the JSON body, "" if none — never the body
RequestSpan.setIfAbsent("handled", "mapped");                     // mapped | passthrough | ignored | fallback | treated_as_success | fail_closed | rethrown (retries live on their own span — 04 §4.4)
log.warn("upstream={} status={} code={} body={}", "account-platform", status, code, mask(first200(body)));   // once, here
throw new AppException(INTERNAL_SERVER_ERROR, errorProperties.getErrorCode(ErrorCode.ACCOUNT_PLATFORM_API_ERROR));   // rule A records the code in the handler
```
* `<dep>.outcome=ignored|fallback|treated_as_success` (+ `handled` if it is the first failure) MUST still be written when the code continues as if nothing happened — with `fallback=<what>` and `SpanOutcome.nonFatal("<dep>.<action>", e)` so the root carries `<dep>.<action>.failure` (§2.1) even though the HTTP status is 200 — and **no** `error_code`: the program did not fail. `upstream.code` keeps the code the upstream answered with, if any.
* Datastores: the JDBC span carries `exception.*` from the driver; the mapping to `<STORE>_DB_ERROR` happens in the advice (rule A) — `upstream.name=<store>-db` there.
* Consumers and jobs: the "advice" is the listener's catch / `JobRun` (05 K3, 06 J1) — same helper, same keys.

---

## 4. Rule C — outcomes hidden behind success are registered as decisions (01 §5)

| Attribute | Set where | Meaning |
|---|---|---|
| `no_record=true` | the no-record advices when a `*NoRecordException` becomes an empty 200; any service method returning an empty page | the only business outcome hidden behind 200 — an upstream outage is otherwise indistinguishable from "no invoices" |
| `result_count` | service, after the page is built | `0` without `no_record` = genuinely empty |
| `grants.accounts_fallback=true`, `fallback=no_permitted_accounts` | the permitted-accounts lookup when it fails and the code proceeds | success despite an authorisation-data failure |
| `fallback=enrichment_failed` | an enrichment `.exceptionally` | rows returned without the enrichment because an upstream failed |
| `support-case.outcome=ignored`, `fallback=empty_support_case`, `support-case.create.failure="HTTP 503"` | a support-case creation wrapped in `onErrorReturn` | order created, ticket never created — a failure key, not a code (§2.1) |
| `integration.outcome=treated_as_success` | a socket timeout reported as `PROCESSED` | the most dangerous class — always registered |
| `consumer.action=skipped`, `skip_reason` | consumer filters / listener catch (05) | message dropped |
| `publish.result=exhausted` | `@Recover` (05) | event never emitted |
| `authz.decision=deny\|error`, `authz.reason` | authorisation engines (01 §4) | why a 401/403 happened, or a fail-closed deny |

Rule: **default first, overwrite on the exceptional path**, so every span of the endpoint has the key.

---

## 5. Rule D — logging of failures (what, where, once)

| Layer | Level | What (MDC supplies `correlationId`, `traceId`, ➕ `trace_id`) |
|---|---|---|
| Controller input (Bean Validation) | DEBUG | field + reason count, not every value |
| Authn / authz | WARN | provider, policy/action, decision, reason code; **never** the policy input document, the token, resource names with user data |
| Service business denial | WARN (short) | `"<KEY> account={} org={}"` — ids only |
| Repository | ERROR | mapper/DAO name + driver exception once; SQL only sanitised at DEBUG |
| Outbound HTTP | ERROR (5xx / transport) or WARN (4xx) | dependency, status, masked URL, first 200 chars of the masked body — exactly once, in the mapping method |
| Consumer / job | ERROR | topic/partition/offset/key/message id or job/run id + exception; never the record value |
| Exception handler | ERROR for 5xx, WARN for 4xx | `"errorCode={} key={} status={} route={}"` — split any handler that logs a stack for every failure so 4xx do not log stacks |

Never logged/traced: JWTs, API keys, SASL/cloud credentials, DB passwords, full bodies, names, e-mails, policy inputs.

---

## 6. TraceQL / LogQL recipes

```
{ span.error_code != nil } | by(resource.service.name, span.error_code)                       -- top error codes, any service, both families
{ kind = server && status = error } | by(resource.service.name, span.http.route, span.error_code)  -- real failures (5xx) only
{ span.error_code =~ "{{ERR_PREFIX}}-[0-9]{4}(46[0-9]|47[0-9]|48[0-9]|030)" } | rate() by(resource.service.name)   -- upstream / messaging / datastore / unknown bands (02 §4)
{ span.error_code = "{{ERR_PREFIX}}-4210465" } | select(span.upstream.name, span.upstream.status, span.upstream.code, span.handled)
{ span.error_code = "{{ERR_PREFIX}}-4210465" } >> { span.peer.service = "account-platform" } | select(span.http.response.status_code)
{ kind = server && span.http.response.status_code = 200 && span.handled = "ignored" } | by(span.upstream.name)   -- success that was not
{ span.failure_count > 0 } | by(resource.service.name, span.http.route)                                                          -- requests that carried on past a failed call (§2.1)
{ span."support-case.create.failure" != nil } | by(span."support-case.create.failure")                                  -- one call: how often, and why
{ name =~ ".* retry$" && span.retry.count > 0 } | by(name, span.retry.outcome)                                      -- per-purpose retry spans (04 §4.4)
{ span.no_record = true } | by(span.http.route)
{ span.authz.decision = "deny" } | by(span.authz.provider, span.authz.reason)
{ span.error.type = "java.net.SocketTimeoutException" } | by(span.peer.service)
{ span.correlation_id = "<uuid>" }                                                                  -- one request end-to-end (HTTP + Kafka + jobs)
```
```
# log labels come from your log shipper (container / namespace / pod labels), not "app"
{k8s_container_name="billing-api"} |= "correlationId:<uuid>"                 # services with a bracketed text pattern
{k8s_container_name="account-platform"} | json | correlationId="<uuid>"    # services with JSON logs (add |= "<code>" when the JSON has no errorCode field)
{k8s_container_name="billing-api"} | regexp "trace_id:(?P<trace_id>\\w+)"   # → derived field → View trace (once trace_id is in the pattern)
```

---

## 7. Per-role placement (where the calls go)

| Role | Rule A | Rule B | Rule C |
|---|---|---|---|
| API with its own advice | the `@RestControllerAdvice` handlers (+ policy advice) | every client mapping method (`onErrorMap`, `handleErrorResponse`, `catch (RestClientResponseException)`), fail-closed clients (`fail_closed`), "timeout → processed" clients (`treated_as_success`) | no-record advices; fallback paths; enrichment `.exceptionally`; gates; caches |
| API using the library's global handler | throw sites + `afterCompletion` interceptor | the client utility (+ `upstream.name` from the caller name; 400/404 = `passthrough`) | ignored support-case creation failure, e-mail dispatch (`publish.result`), async dispatch |
| WebFlux service | the `ErrorWebExceptionHandler` (name-based codes) | policy client (its retry loop is a per-purpose retry span, 04 §4.4), gRPC clients | act-on-behalf filter (`authz.*`) |
| Consumers / jobs | listener catch / `JobRun` (05 K3, 06 J1) | same as API | skip reasons, exhausted publishes |
| Node servers | global error middleware (07 N5) | axios interceptor / proxy `onProxyRes` (07 N4) | cache fallbacks, ignored notification failures |

---

## 8. Checklist — outcomes for one endpoint / listener / job

- [ ] `SpanOutcome.record(code, key, status, cause)` called from the single exception handler (or throw sites) — only writer of `error_code`; `error.type` set; 5xx → `recordException` once + `ERROR`; 4xx → attributes only
- [ ] Every outbound mapping method writes `<dep>.outcome` and, on the first failure, `upstream.name/status/code` + `handled` on the root before throwing/returning; ignored, fallback and treated-as-success paths write `<dependency>.<action>.failure` (`SpanOutcome.nonFatal`) and carry **no** minted code (§2.1)
- [ ] Every retrying call (HTTP, messaging, anything) has its own per-purpose span with `retry.count` / `max` / `outcome` (04 §4.4); no code for the attempts, no span per attempt written by code
- [ ] Hidden-200 outcomes flagged (`no_record`, `fallback`, `handled`), defaults first
- [ ] Authorisation denials carry `authz.*` + the code; a second code family keeps its own values
- [ ] Failure logged once at the point of failure (masked) and once in the handler, both with the correlation id and `trace_id`; no secrets/PII
- [ ] Response body carries `errorCode` + `correlationId`; `{{CORRELATION_HEADER}}` response header on every response
- [ ] TraceQL in §6 returns the failures by code, by upstream and by correlation id; a log line opens the trace by `trace_id`
