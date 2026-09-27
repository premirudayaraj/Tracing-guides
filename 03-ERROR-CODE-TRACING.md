# 03 — Error Code Tracing: The Outcome on the Span, in the Log Line and in the Response

> **Scope:** the *placement* of failure information so that any failure — a denied request, a failed upstream, a swallowed exception, an empty 200 — can be found in `{{TRACE_BACKEND}}`, in `{{LOG_BACKEND}}` and in the client response with one identifier. Building the codes is `02`; the request span and decisions are `01`; outbound calls `04`; messages `05`; jobs `06`; browser/Node `07`. Placeholders are defined in `README.md §0`.
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
| Hidden-200 outcomes | no-record advices; "permission lookup failed → proceed without filter"; enrichment `.exceptionally` returning un-enriched rows; a support-case creation swallowed with `onErrorReturn`; an e-mail dispatch wrapped in a catch-all; a timeout reported as processed; consumer error handlers that skip (05 K3) |
| OpenTelemetry *Recording errors* semantic conventions | status `Error` only when the operation failed; "errors that were retried or handled … SHOULD NOT be recorded"; `error.type` only on failure; the same exception is not recorded twice; server-side 4xx leaves status unset, client-side 4xx sets it; 404 is an error only when the resource was expected. Span events are being deprecated in favour of logs — decisions are attributes, exceptions are `recordException` (still supported) or a log line with `trace_id` |

---

## 1. The three sinks and the one key

| Sink | Carries today | ➕ Should carry | Joined by |
|---|---|---|---|
| **Response body** | `errorCode`, `errorMessage`, `details`, `additionalInfo[]`, `correlationId`; real HTTP status (02 §7) | `{{CORRELATION_HEADER}}` response header on every response (01 §3) | `correlationId` |
| **Span** | `http.response.status_code`, status (5xx), `error.type`, `exception.*` (agent), `http.request.header.<correlation header>` (captured) | `{{PREFIX}}.error_code`, `{{PREFIX}}.error_key`, `{{PREFIX}}.correlation_id`, `{{PREFIX}}.upstream.name/status/code`, `{{PREFIX}}.handled`, `{{PREFIX}}.<dep>.outcome`, `{{PREFIX}}.no_record`, `{{PREFIX}}.fallback`, `{{PREFIX}}.authz.*` (01), `{{PREFIX}}.consumer.action`/`{{PREFIX}}.publish.result` (05), `{{PREFIX}}.job.status` (06) | `{{PREFIX}}.correlation_id` |
| **Logs** | one ERROR/WARN line at the point of failure with the MDC correlation id and the in-app `traceId` | the agent's `trace_id`/`span_id` in the pattern so the line opens the trace; the common MDC key `correlationId` | `correlationId`, `trace_id` |

Support flow: client shows `{{ERR_PREFIX}}-4210465` + correlation id → trace query `{ span.{{PREFIX}}.correlation_id = "…" }` → the root span carries `{{PREFIX}}.error_code={{ERR_PREFIX}}-4210465`, `{{PREFIX}}.upstream.name=account-platform`, `{{PREFIX}}.upstream.status=503`, `{{PREFIX}}.upstream.code={{ERR_PREFIX}}-4310500`, `{{PREFIX}}.handled=mapped`; the CLIENT child has `peer.service=account-platform`, `http.response.status_code=503` → log query by `correlationId` shows the masked upstream body logged once in the mapping method.

---

## 2. Rule A — one writer of the outcome: the exception handler

Each service has exactly one component that turns an exception into the response. **That component sets `{{PREFIX}}.error_code` on the root span**, once, through one helper:

```java
// ➕ tracing/SpanOutcome.java — one per service (or in the shared REST library); no other class writes {{PREFIX}}.error_code
public final class SpanOutcome {
  public static void record(String errorCode, String errorKey, int httpStatus, Throwable cause) {
    Span span = RequestSpan.root();                                       // 01 §6; no-op span when the agent is off
    span.setAttribute("{{PREFIX}}.error_code", errorCode);                // "{{ERR_PREFIX}}-4210465" / "<PRODUCT>-<NAME>" — only on failures
    span.setAttribute("{{PREFIX}}.error_key", errorKey);                  // "ACCOUNT_PLATFORM_API_ERROR" — the catalogue key, never the message text
    span.setAttribute("error.type", cause != null ? cause.getClass().getName() : errorCode);   // standard: exception class, or the domain code when there is no exception
    if (httpStatus >= 500) {                                              // the request really failed
      if (cause != null) span.recordException(cause);                    // once — not again in a lower layer
      span.setStatus(StatusCode.ERROR, errorCode);
    }                                                                     // 4xx (validation, 401, 403, 404): expected outcome — code + error.type, status untouched
  }
  /** a failure that the code handled (swallowed / fallback / treated as success): the request still succeeds, but the failure is searchable */
  public static void recordHandled(String errorCode, String errorKey, Throwable cause) {
    Span span = RequestSpan.root();
    span.setAttribute("{{PREFIX}}.error_key", errorKey);                  // NOT {{PREFIX}}.error_code — the client did not receive a code
    span.setAttribute("{{PREFIX}}.handled_error_code", errorCode);        // the code it would have been
    if (cause != null) { span.setAttribute("error.type", cause.getClass().getName()); span.recordException(cause); }   // once, here; status untouched
  }
}
```
* **5xx → `recordException` + `ERROR`; 4xx → attributes only.** A denied request (`403`, `{{PREFIX}}.authz.decision=deny`) or a validation failure is a *correct* answer, not an error; `status=error` and error-rate panels then stay meaningful (the agent already marks 5xx SERVER spans; this adds the *code*).
* Where to call it: the service's `@RestControllerAdvice` handlers (application-exception handler with its status and first `errorCode`; catch-all with `{{ERR_PREFIX}}-<module>030`/`500`; validation handlers with the validation code + `{{PREFIX}}.validation.field_count`); the policy advice; the WebFlux `ErrorWebExceptionHandler`. A service that has **no advice of its own** (the library's global handler builds the body) → tag **at the throw site** (`SpanOutcome.record(code, key, status, null)` right before `throw new AppException(...)`) and add a `HandlerInterceptor.afterCompletion` that, for `response.getStatus() >= 500`, records the exception and sets `ERROR` (read the status — `ex` is null when the library handler consumed it).
* `{{PREFIX}}.error_code` exists **only** on failures; `{ span.{{PREFIX}}.error_code != nil }` is the failure set. One code per span; multiple validation failures = one code + `{{PREFIX}}.validation.field_count`.
* Upstream codes never become `{{PREFIX}}.error_code` — they are `{{PREFIX}}.upstream.code` (rule B); the client saw *this* service's code.
* Never record the same exception twice (agent + advice + client layer): the agent records it on the CLIENT/JDBC child that threw; the handler records it on the root once. In practice: `SpanOutcome.record` is called **only** from the exception handler (or throw sites); `SpanOutcome.recordHandled` **only** where a failure is swallowed/fallen back (04 §4.3, 05 K3); `catch` blocks that rethrow record nothing. `05` (`@Recover`, consumer error path) and `06` (`JobRun`) call the same two methods — no other class writes `{{PREFIX}}.error_code`.

---

## 3. Rule B — dependency outcomes are registered where the call is made (04 §4.3)

```java
// existing handleErrorResponse / onErrorMap / catch(RestClientResponseException) — the CLIENT span has already ended; write on the ROOT
RequestSpan.set("{{PREFIX}}.account-platform.outcome", "mapped");            // per dependency (default "pending" written before the call, "ok" on success)
RequestSpan.setIfAbsent("{{PREFIX}}.upstream.name", "account-platform");    // first failing dependency wins; == peer.service of the CLIENT child
RequestSpan.setIfAbsent("{{PREFIX}}.upstream.status", response.statusCode().value());
RequestSpan.setIfAbsent("{{PREFIX}}.upstream.code", UpstreamCode.extract(body));   // first "errorCode" in the JSON body, "" if none — never the body
RequestSpan.setIfAbsent("{{PREFIX}}.handled", "mapped");                     // mapped | passthrough | swallowed | fallback | treated_as_success | retried | fail_closed | rethrown
log.warn("upstream={} status={} code={} body={}", "account-platform", status, code, mask(first200(body)));   // once, here
throw new AppException(INTERNAL_SERVER_ERROR, errorProperties.getErrorCode(ErrorCode.ACCOUNT_PLATFORM_API_ERROR));   // rule A records the code in the handler
```
* `{{PREFIX}}.<dep>.outcome=swallowed|fallback|treated_as_success` (+ `{{PREFIX}}.handled` if it is the first failure) MUST still be written when the code continues as if nothing happened — with `{{PREFIX}}.fallback=<what>` and `SpanOutcome.recordHandled(code, KEY, e)` so the root carries `{{PREFIX}}.error_key` and the exception once, even though the HTTP status is 200 (status stays OK; the attributes make it searchable).
* Datastores: the JDBC span carries `exception.*` from the driver; the mapping to `<STORE>_DB_ERROR` happens in the advice (rule A) — `{{PREFIX}}.upstream.name=<store>-db` there.
* Consumers and jobs: the "advice" is the listener's catch / `JobRun` (05 K3, 06 J1) — same helper, same keys.

---

## 4. Rule C — outcomes hidden behind success are registered as decisions (01 §5)

| Attribute | Set where | Meaning |
|---|---|---|
| `{{PREFIX}}.no_record=true` | the no-record advices when a `*NoRecordException` becomes an empty 200; any service method returning an empty page | the only business outcome hidden behind 200 — an upstream outage is otherwise indistinguishable from "no invoices" |
| `{{PREFIX}}.result_count` | service, after the page is built | `0` without `{{PREFIX}}.no_record` = genuinely empty |
| `{{PREFIX}}.user.accounts_fallback=true`, `{{PREFIX}}.fallback=no_permitted_accounts` | the permitted-accounts lookup when it fails and the code proceeds | success despite an authorisation-data failure |
| `{{PREFIX}}.fallback=enrichment_failed` | an enrichment `.exceptionally` | rows returned without the enrichment because an upstream failed |
| `{{PREFIX}}.support-case.outcome=swallowed`, `{{PREFIX}}.fallback=empty_support_case`, `{{PREFIX}}.error_key=SUPPORT_CASE_API_ERROR` | a support-case creation wrapped in `onErrorReturn` | order created, ticket never created |
| `{{PREFIX}}.integration.outcome=treated_as_success` | a socket timeout reported as `PROCESSED` | the most dangerous class — always registered |
| `{{PREFIX}}.consumer.action=skipped`, `{{PREFIX}}.skip_reason` | consumer filters / listener catch (05) | message dropped |
| `{{PREFIX}}.publish.result=exhausted` | `@Recover` (05) | event never emitted |
| `{{PREFIX}}.authz.decision=deny\|error`, `{{PREFIX}}.authz.reason` | authorisation engines (01 §4) | why a 401/403 happened, or a fail-closed deny |

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
{ span.{{PREFIX}}.error_code != nil } | by(resource.service.name, span.{{PREFIX}}.error_code)                       -- top error codes, any service, both families
{ kind = server && status = error } | by(resource.service.name, span.http.route, span.{{PREFIX}}.error_code)  -- real failures (5xx) only
{ span.{{PREFIX}}.error_code =~ "{{ERR_PREFIX}}-[0-9]{4}(46[0-9]|47[0-9]|48[0-9]|030)" } | rate() by(resource.service.name)   -- upstream / messaging / datastore / unknown bands (02 §4)
{ span.{{PREFIX}}.error_code = "{{ERR_PREFIX}}-4210465" } | select(span.{{PREFIX}}.upstream.name, span.{{PREFIX}}.upstream.status, span.{{PREFIX}}.upstream.code, span.{{PREFIX}}.handled)
{ span.{{PREFIX}}.error_code = "{{ERR_PREFIX}}-4210465" } >> { span.peer.service = "account-platform" } | select(span.http.response.status_code)
{ kind = server && span.http.response.status_code = 200 && span.{{PREFIX}}.handled = "swallowed" } | by(span.{{PREFIX}}.upstream.name)   -- success that was not
{ span.{{PREFIX}}.no_record = true } | by(span.http.route)
{ span.{{PREFIX}}.authz.decision = "deny" } | by(span.{{PREFIX}}.authz.provider, span.{{PREFIX}}.authz.reason)
{ span.error.type = "java.net.SocketTimeoutException" } | by(span.peer.service)
{ span.{{PREFIX}}.correlation_id = "<uuid>" }                                                                  -- one request end-to-end (HTTP + Kafka + jobs)
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
| API using the library's global handler | throw sites + `afterCompletion` interceptor | the client utility (+ `{{PREFIX}}.upstream.name` from the caller name; 400/404 = `passthrough`) | swallowed support-case creation, e-mail dispatch (`{{PREFIX}}.publish.result`), async dispatch |
| WebFlux service | the `ErrorWebExceptionHandler` (name-based codes) | policy client (retry → `retried`), gRPC clients | act-on-behalf filter (`{{PREFIX}}.authz.*`) |
| Consumers / jobs | listener catch / `JobRun` (05 K3, 06 J1) | same as API | skip reasons, exhausted publishes |
| Node servers | global error middleware (07 N5) | axios interceptor / proxy `onProxyRes` (07 N4) | cache fallbacks, swallowed notifications |

---

## 8. Checklist — outcomes for one endpoint / listener / job

- [ ] `SpanOutcome.record(code, key, status, cause)` called from the single exception handler (or throw sites) — only writer of `{{PREFIX}}.error_code`; `error.type` set; 5xx → `recordException` once + `ERROR`; 4xx → attributes only
- [ ] Every outbound mapping method writes `{{PREFIX}}.<dep>.outcome` and, on the first failure, `{{PREFIX}}.upstream.name/status/code` + `{{PREFIX}}.handled` on the root before throwing/returning; swallowed, fallback and treated-as-success paths included (`SpanOutcome.recordHandled`)
- [ ] Hidden-200 outcomes flagged (`{{PREFIX}}.no_record`, `{{PREFIX}}.fallback`, `{{PREFIX}}.handled`), defaults first
- [ ] Authorisation denials carry `{{PREFIX}}.authz.*` + the code; a second code family keeps its own values
- [ ] Failure logged once at the point of failure (masked) and once in the handler, both with the correlation id and `trace_id`; no secrets/PII
- [ ] Response body carries `errorCode` + `correlationId`; `{{CORRELATION_HEADER}}` response header on every response
- [ ] TraceQL in §6 returns the failures by code, by upstream and by correlation id; a log line opens the trace by `trace_id`
