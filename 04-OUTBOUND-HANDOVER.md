# 04 — Outbound Handover: Every Call to Another Service or a Third Party

> **Role:** HTTP CLIENT — any code that calls another internal service (a data hub, an account platform, an identity service, an admin API, a document store, a contract service, master data, support cases, notifications…) or a third party (an ERP, a CRM through an integration middleware, a cloud identity provider and its file storage, e-signature, contract management, a feature-flag provider, an LLM proxy, a source-control API, a chat webhook). It covers the **authentication step before the call** (token endpoints, API keys), the **request** (which identifiers and flags go out), the **response** (which identifiers and status come back) and the **decision the code makes on the outcome** (mapped error code, ignored, retried, fallback, "treated as processed"). Placeholders are defined in `README.md §0`.
>
> **How to use with an AI assistant:** *"Trace the outbound calls of `<service>` per 04-OUTBOUND-HANDOVER.md: first produce the §2 inventory (one row per call site), then apply §3–§5."*
>
> Legend: **MUST** = rule; **already** = usually in current code; ➕ = add; ⚠ = confirm at runtime.

---

## 0. Reference patterns (what this is generalised from)

28 outbound call sites across three API services and one Node API server were read. Patterns that shape the rules:

* **Clients:** WebClients from an in-house factory with a `logRequest()` filter that masks `Authorization`, API-key and auth-token headers; `RestTemplate` with a logging interceptor and a response error handler; axios in Node.
* **Auth before the call:** static API key from config; caller's JWT forwarded (`Authorization: Bearer`, an auth-token header); **token endpoints** called per request (gateway token with basic auth + username/password, no cache; identity token with client credentials stored in a runtime holder; a file-storage OAuth token fetched **every call**; a cloud-identity token cached with TTL `min(expiresIn-300, cfg)`; an admin token cached at startup; a vault-provided bearer set at startup and never refreshed).
* **Correlation id forwarded** under different spellings per dependency (`X-Correlation-Id`, `Correlation-Id`, `correlationId`, `CORRELATION_ID`, `req.headers` spread in Node).
* **Decisions on the outcome:** non-2xx → application exception `(500, <DEP>_API_ERROR)`; a client utility that passes 400/404 through and maps the rest to 500; 404 → not-found exception / empty; 5xx → internal-server exception; **ignored** (caught, logged, carried on): a support-case creation with `onErrorReturn(new Response())` (order created, ticket silently missing), an e-mail dispatch wrapped in a catch-all, a chat notification in Node, a legal-agreements lookup returning an empty response; **misleading**: an integration client turning `SocketTimeoutException` into `status=PROCESSED`; **fail-closed**: a policy-sidecar client returning `false`; **timeout → empty**: a contact search `get(20 s)` → empty DTO; **redirect follow**: 3xx through a second WebClient; **retry**: `@Retryable` on message sends (05), policy engine ×3.
* **What the agent already gives:** one CLIENT span per call (`http.request.method`, `server.address`, `url.full`, `http.response.status_code`, `error.type` on failure, `traceparent` injected) — for WebClient, RestTemplate, Apache HttpClient, OkHttp, axios/`http` (Node SDK). It does **not** know *what the call was for*, *which identifiers were sent*, *what was read from the response*, or *what the caller decided afterwards*.

---

## 1. The model

```
 SERVER span  POST /billing/v3/…                              (root: 01)
 └─ INTERNAL  AccountService.enrichCharges                    (@WithSpan, fan-out)
     │   attrs on THIS (caller) span, before/after each call:  call.gateway-token.purpose=token  auth.provider=gateway  auth.token_source=fetched
     │                                                          call.billing-hub.purpose=invoice_summary  call.billing-hub.version=v2  call.billing-hub.ids_count=10  call.billing-hub.flags=charges
     │                                                          call.billing-hub.response.code=ZERO_RECORDS  call.billing-hub.response.count=0
     ├─ CLIENT  POST   peer.service=gateway-token   http.response.status_code=200          ← agent span; peer.service from the host mapping
     ├─ CLIENT  POST   peer.service=billing-hub     http.response.status_code=503  error.type=503   ← agent span
 (root) upstream.name=billing-hub  upstream.status=503  upstream.code=…  handled=mapped  billing-hub.outcome=mapped  account-platform.outcome=ok  error_code={{ERR_PREFIX}}-4210460 (03)
```

Four invariants:
1. **The call is the agent's CLIENT span; the caller never wraps it and never tries to reach it.** The CLIENT span is created inside the client library after interceptors/filters have run (RestTemplate `ClientHttpRequestInterceptor`, WebClient `ExchangeFilterFunction`, Feign `RequestInterceptor`, OkHttp application interceptors all see the *caller's* span as current — ⚠ verified only by reading the agent's instrumentation points, confirm once). Therefore: **request facts go on the caller's span before the call, response/decision facts on the caller's span after the call**, with `call.<purpose>.*` keys when a method makes several calls; the CLIENT span itself carries only what the agent puts there plus `peer.service` from the host mapping. Interceptors/filters (§5) set **headers only**.
2. **The dependency is named**, not the host: `peer.service` through the agent mapping.
3. **Auth steps are calls too.** A token request is a CLIENT span with `call.purpose=token`, and the caller registers *whether a token was fetched or reused* (`auth.token_source=cache|fetched|static`) — never the token.
4. **The decision on the outcome is registered on the root**: which upstream, what status/code it returned, and what the code did about it (`mapped`, `passthrough`, `ignored`, `fallback`, `treated_as_success`, `fail_closed`, `rethrown`). An ignored answer gets a `<dependency>.<action>.failure` key, never this service's error code (03 §2.1); a retried call gets its own per-purpose span with a count (§4.4). Every dependency the request touched gets `<dep>.outcome=ok|<handled value>` on the root (one key per dependency, so a fan-out to three systems keeps three answers); `upstream.name/status/code` + `handled` describe the **first failing** dependency only (first writer wins — `RequestSpan.setIfAbsent`). A non-fatal error is still registered as a failure of the *call*, even if the request succeeds.

---

## 2. The analysis you run before writing code (mandatory)

For **every** outbound call site (grep: `WebClient`, `RestTemplate`, `RestClient`, `@FeignClient`, `HttpClient`, `HttpURLConnection`, `OkHttp`, `WebServiceTemplate`, gRPC stubs `newBlockingStub`/`@GrpcClient`, `JavaMailSender`, workflow-engine clients (`WorkflowClient`/`WorkflowStub`), cloud SDK clients (STS, schema registry), `RedisTemplate`/Lettuce, `axios`, `got`, `undici`/`fetch(`, `http-proxy-middleware`; messaging sends `StreamBridge`, `kafkaTemplate.send`, `RabbitTemplate` → 05) produce one row:

| # | file:method | dependency (peer.service) | endpoint (template) | **auth**: how the credential is obtained (static key / forwarded JWT / token endpoint + cache policy) | **request**: identifiers and flags that decide what the upstream does | **response**: fields the code reads | **on failure**: mapped code / ignored / fallback / retry / timeout behaviour | attributes to set |

Questions that must be answered per row:
* Is there a **prior call** to get a credential? Is it per request, cached with TTL, cached forever, or refreshed by a scheduler (06)? A token fetched per request is a **decision** (cost + failure mode) and must be visible.
* Which **request fields are decisive**? Ids (`accountNumbers[]`, `masterDataIds[]`, `partyNumber`, `documentIds`, `agreementNumbers`, `orgId`, `projectId`), enums/flags (`searchScope`, `resourceType`, `sourceSystem`, `status`, `agreementStatus=ACTIVATED`, `ancestors=true`), pagination, and the **version** of the contract (`/v1/accounts/search` vs `/v2` vs `/v3/billingAccounts/search`).
* Which **response fields** does the code read and branch on? (`result.authorized`, `result.reason`, `success`, `ticketId`, `access_token`+`expires_in`, `pagination.total`, `data[].status`, `contactStatus`, `agreementNumber`, `errorCode` in an upstream error body, a "zero records" marker in a 2xx body.)
* What happens on **4xx / 5xx / timeout / connection error / redirect**? Mapped to which `{{ERR_PREFIX}}-` code; **ignored** (`onErrorReturn`, catch-all log); **fallback** (empty object, cached value, `null`); **retried** (attempts, backoff — becomes a per-purpose retry span, §4.4); **treated as success** (`SocketTimeout → PROCESSED`); **fail-closed** (`authorized=false`).
* Is the correlation id forwarded, under which header name, and is `traceparent` propagated (agent: yes for WebClient/RestTemplate/OkHttp/HttpClient; Node: only with the SDK; gRPC: agent yes)?

Every row becomes: `peer.service` mapping (§3), request attributes (§4.1), auth attributes (§4.2), response + decision attributes (§4.3).

---

## 3. R-PEER — name the dependency

* ➕ Startup script / injected agent options, per environment (hosts differ per env, names do not):
  ```
  -Dotel.instrumentation.common.peer-service-mapping=<gateway-host>=gateway,<billing-hub-host>=billing-hub,<hierarchy-host>=hierarchy,<identity-host>=identity,policy-sidecar=policy-sidecar,localhost:8181=policy-engine,<master-data-host>=master-data,<erp-host>=erp-billing,<integration-host>=integration,<cloud-identity-host>=cloud-identity,<file-storage-host>=file-storage,<contract-host>=contract-mgmt,<agreements-host>=agreements,<notification-host>=notification,<admin-host>=admin-org,<document-host>=document-api,<support-host>=support-case,<llm-host>=llm-proxy,<scm-api-host>=source-control,<chat-webhook-host>=chat-webhook
  ```
  Result: `peer.service` on every CLIENT span → `{ span.peer.service = "billing-hub" && status = error }` survives host changes and works across services.
* Calls that go through an **API gateway** to another internal service share one host: `peer.service=gateway` plus `call.purpose=<target>.<operation>` on the caller's span is the disambiguator (the CLIENT span cannot be reached from application code — §1).
* Node (07): the OTel Node SDK's `http` instrumentation + a `SpanProcessor` that sets `peer.service` from a host table.

---

## 4. R-CALL — what the caller registers

### 4.1 Before the call: purpose, contract, decisive request fields (on the caller's span)

```java
Span s = Span.current();                                                  // the caller's span (@WithSpan method or SERVER span) — NOT the CLIENT span
s.setAttribute("call.purpose", "billing-hub.invoice_summary"); // stable enum per call site: <dep>.<operation>
s.setAttribute("call.version", "v2");                          // contract version chosen (v1|v2|v3)
s.setAttribute("call.ids_count", req.getAccountNumbers().size());   // how many ids were sent (never the list)
s.setAttribute("call.id", req.getAccountNumber());             // when exactly ONE id identifies the call (account, order number, document id, agreement number)
s.setAttribute("call.flags", "charges,ancestors");             // decisive booleans/enums as a short comma list, or one key each: call.<purpose>.search_scope=COMPANY
s.setAttribute("call.page_size", req.getLimit());
```
When one method makes several calls, prefix by dependency/purpose: `call.billing-hub.ids_count`, `call.account-platform.ids_count` — or put each call in its own `@WithSpan` unit if it is a fan-out anyway. The chosen upstream/version is also a root decision: `RequestSpan.set("billing-hub.version", "v3")` (01 §5).

### 4.2 The authentication step (on the span that performs it)

```java
// token endpoint call (gateway token, identity token, cloud-identity / file-storage OAuth, source-control token) — the agent creates the CLIENT span; add on the current span:
s.setAttribute("auth.provider", "cloud-identity");                  // gateway | identity | cloud-identity | file-storage | source-control | static-apikey | forwarded-jwt | vault-static
s.setAttribute("auth.grant", "client_credentials");                 // client_credentials | password | basic | apikey | bearer-forward
s.setAttribute("auth.token_source", cached ? "cache" : "fetched");  // cache | fetched | static | forwarded
s.setAttribute("auth.token_ttl_s", expiresIn);                      // when the response says so (number, not the token)
s.setAttribute("auth.result", ok ? "ok" : "failed");                // failed → the business call is not attempted, or attempted without auth: say which: auth.on_failure=abort|proceed|cached_stale
```
* MUST never put `access_token`, `client_secret`, `password`, `Authorization`, API keys on any span or in any log (the client's existing mask list is the minimum).
* A token fetched **per request** shows as an extra CLIENT span per request — that is the point; `auth.token_source=fetched` on every request is the signal that a cache is missing.
* Scheduled token refreshers are traced as jobs (06 §J2 — refresh outcome + `auth.*`), not as request work.

### 4.3 After the call: response identifiers, status and the decision (caller's span; outcome on the root)

```java
// success path — on the caller's span
s.setAttribute("call.response.count", resp.getData().size());        // or pagination.total
s.setAttribute("call.response.id", resp.getTicketId());              // the identifier the upstream created (ticket id, e-sign agreement id, document id, hierarchy id, request id)
s.setAttribute("call.response.status", resp.getStatus());            // an upstream enum when the code branches on it (contact status, agreement status, event status)
s.setAttribute("call.response.code", resp.getErrorCode());           // an upstream code carried in a 2xx body ("zero records", "no data found")
RequestSpan.set("billing-hub.outcome", "ok");                         // per-dependency outcome on the ROOT (default written before the call: "pending")

// failure / outcome decision (03 rule B) — on the ROOT so the request's story shows the cause; first failing dependency wins
RequestSpan.set("billing-hub.outcome", "mapped");                     // per dependency: ok | mapped | passthrough | ignored | fallback | treated_as_success | fail_closed | rethrown
RequestSpan.setIfAbsent("upstream.name", "billing-hub");
RequestSpan.setIfAbsent("upstream.status", status.value());          // HTTP status from the WebClientResponseException / RestClientResponseException
RequestSpan.setIfAbsent("upstream.code", upstreamBody.getErrorCode()); // if the upstream body has a code; never the body
RequestSpan.setIfAbsent("handled", "mapped");
// then throw the mapped exception — the exception handler records error_code / error.type / recordException ONCE (03 rule A). Do NOT set error_code or recordException here.
// ignored / fallback still registers the failure of the call — as a key named after the call, NOT as an error code (03 §2.1: the program continued, so it did not fail):
RequestSpan.set("support-case.outcome", "ignored");  RequestSpan.set("fallback", "empty_support_case");   // the onErrorReturn pattern — the facets you group by
SpanOutcome.nonFatal("support-case.create", e);                                  // → support-case.create.failure = "HTTP 503" + failure_count on the root; logs the exception once; no error_code, no error_key, status untouched
```
Keys that begin with the dependency's name (`<dep>.outcome`, `<dep>.version`, `<dep>.<action>.failure`) are built with `AttrName.of(...)` (01 §6), which adds `{{PREFIX}}.` only when the dependency's name is an OpenTelemetry namespace (a service literally called `db` → `{{PREFIX}}.db.outcome`); for every other dependency the key is exactly as written. `RequestSpan.setIfAbsent(k, v)` is the helper of 01 §6 with a first-writer guard (the OTel API cannot read attributes back).

Rules for the decision attribute `handled`:
| Value | Meaning | Reference examples |
|---|---|---|
| `mapped` | upstream failure → this service's `{{ERR_PREFIX}}-` code and a real HTTP status | `<DEP>_API_ERROR` 460–465; `InternalServerException`; the client utility for non-400/404 |
| `passthrough` | upstream 400/404 status and message forwarded | the client utility for 400/404 |
| `ignored` | logged and ignored, caller continues as success — `<dependency>.<action>.failure` key written (03 §2.1), no code | support-case creation, e-mail dispatch, Node chat notify, legal-agreements empty response |
| `fallback` | a default/empty/cached value replaces the answer — failure key written, no code | contact search 20 s → empty; OAuth token cache; help-topics cache; default project → null |
| `treated_as_success` | an error is reported as success upstream/downstream — failure key written, no code | `SocketTimeout → status=PROCESSED` — always register, this is the most dangerous class |
| `fail_closed` | error treated as deny | policy-sidecar client |
| `rethrown` | propagated unchanged | master-data batch, hierarchy `createAccount` |

`handled` describes what the code did with the **final** answer of a call. Retrying is not a final answer — it is recorded on the call's own retry span (§4.4), never as a `handled` value.

* MUST register `handled` **and** `upstream.status` for every non-2xx, timeout or connection failure, including ignored ones — otherwise the trace shows a green request while a support case / e-mail / agreement silently never happened.
* 4xx from an upstream on a CLIENT span sets `error.type` (agent) — that is correct for the client span; whether the **request** is an error is decided in the exception handler (03).

### 4.4 Retries: one span per purpose, a count on it — for every kind of call

Applies to **any** call that retries — outbound HTTP, a policy engine ×3, a token endpoint, a Kafka publish (05 K1), a database call under a retry template — not only messaging.

```java
// the span wraps the WHOLE retry loop (the caller of the @Retryable method, or the RetryTemplate.execute / Reactor retryWhen / resilience4j Retry.decorate call site) —
// NOT the @Retryable method itself (Spring Retry re-invokes that method per attempt: a @WithSpan there gives one span per attempt, which is exactly what this rule forbids)
Span retrySpan = tracer.spanBuilder("billing-hub register-user retry").setSpanKind(SpanKind.INTERNAL)   // "<dependency> <purpose> retry" — named by WHAT the call is for, never "billing-hub retries"
    .setAttribute("call.purpose", "register-user").setAttribute("retry.max", cfg.maxAttempts() - 1).startSpan();
try (Scope sc = retrySpan.makeCurrent()) {                                     // the agent's CLIENT span of every attempt becomes a child of this span
  R out = retryTemplate.execute(ctx -> client.registerUser(req),               // RetryContext.getRetryCount() counts the retries made so far
                                ctx -> { retrySpan.setAttribute("retry.outcome", "exhausted"); throw exhausted(ctx); });
  retrySpan.setAttribute("retry.outcome", "succeeded");
  return out;
} finally {
  retrySpan.setAttribute("retry.count", lastContext.getRetryCount());   // a NUMBER: 0 = first attempt succeeded, 3 = three retries after the first attempt; read it in a RetryListener.close(...) or from the context you kept
  retrySpan.end();
}
// @Retryable + @Recover: put the span in the CALLER of the @Retryable method; in @Recover set retry.outcome=exhausted and RetrySynchronizationManager.getContext().getRetryCount() as the count
// Reactor: .retryWhen(Retry.backoff(n, d).doAfterRetry(sig -> retrySpan.setAttribute("retry.count", sig.totalRetries() + 1)))   Node: axios-retry's retryCount on the response config
```
Rules:
* **One span per purpose.** Two calls to the same dependency for different purposes in one request (`billing-hub register-user retry`, `billing-hub fetch-invoices retry`) are two spans — never merged into one generic bucket; the name carries the purpose, `call.purpose` carries it as an attribute.
* **A count, not entries.** `retry.count` (retries performed, excluding the first attempt), `retry.max` (configured), `retry.outcome=succeeded|exhausted|aborted` (`aborted` = a non-retryable exception stopped the loop). The code never creates a span or an event per attempt; the agent's CLIENT children show the attempts on their own.
* **Not an error code.** The count says nothing about failure. What the program does *after* the loop decides the rest: exhausted and the operation fails → the exception handler writes the code (03 rule A); exhausted and the program continues → `SpanOutcome.nonFatal("<dependency>.<purpose>", "retries exhausted (" + count + ")", e)` → `<dependency>.<purpose>.failure` (03 §2.1); succeeded after retries → nothing more.
* A call that has no retry policy gets no retry span — the caller's span + the agent's CLIENT span are enough (§4.1–4.3).

---

## 5. ➕ Once per client, not per call site: the interceptor that sets the headers (and only the headers)

One interceptor per client type, registered where the client is built — it adds the canonical correlation header, **declares this service's name** in `{{SOURCE_APP_HEADER}}` so the callee can register who called it (01 §3.1), records the upstream's echoed ids **on the caller's span** (current when the interceptor runs) and never tries to touch the CLIENT span:

| Client | Hook | Where |
|---|---|---|
| WebClient | `ExchangeFilterFunction` next to the existing `logRequest()` | the service's WebClient config, or the in-house WebClient factory (library — preferred, 00 §5) |
| RestTemplate | `ClientHttpRequestInterceptor` | the existing logging interceptor (extend) |
| RestClient / Feign / OkHttp | `ClientHttpRequestInterceptor` / `RequestInterceptor` / `Interceptor` | as found by §2 |
| gRPC | `ClientInterceptor` adding `{{CORRELATION_HEADER}}` and `{{SOURCE_APP_HEADER}}` (lower-cased — gRPC metadata keys are lower-case) metadata | gRPC clients |
| axios / got / proxy | request interceptor / `onProxyReq` + `onProxyRes` | 07 §N4 |

```java
public static ExchangeFilterFunction correlationHandover() {
  return (req, next) -> {
    Span caller = Span.current();                                 // the caller's span; the agent creates the CLIENT span later, inside the exchange function
    String cid = firstNonBlank(MDC.get("correlationId"), MDC.get("<existing key>"));
    if (cid != null && !req.headers().containsKey("{{CORRELATION_HEADER}}"))    // keep the dependency-specific spellings the code already sets — add the canonical one
      req = ClientRequest.from(req).header("{{CORRELATION_HEADER}}", cid).build();
    if (!req.headers().containsKey("{{SOURCE_APP_HEADER}}"))                     // declare WHO IS CALLING: this service's own service.name (01 §3.1 reads it on the other side)
      req = ClientRequest.from(req).header("{{SOURCE_APP_HEADER}}", SERVICE_NAME).build();   // SERVICE_NAME = the same string as OTEL_SERVICE_NAME / service.name — one name per service everywhere
    return next.exchange(req).doOnNext(resp -> {
      resp.headers().header("{{CORRELATION_HEADER}}").stream().findFirst().ifPresent(v -> caller.setAttribute("call.response.correlation_id", v));   // did the upstream echo ours?
      resp.headers().header("X-Request-Id").stream().findFirst().ifPresent(v -> caller.setAttribute("call.response.request_id", v));                // upstream request id when they give one
    });
  };
}
```
* `peer.service` comes from the agent mapping (§3). For gateway-fronted targets that share a host, either accept `peer.service=gateway` + `call.purpose`, or give each per-target client bean its own host alias in the mapping when the environment allows distinct hostnames; otherwise the purpose attribute is the disambiguator.
* Also the place to assert that **no** `Authorization`/`{{APIKEY_HEADER}}` value is ever logged (reuse the existing mask list).
* For libraries that own the client (the in-house WebClient factory, a notification library), the interceptor belongs in the library (00 §5).

---

## 6. Verification (TraceQL)

```
{ kind = client } | by(span.peer.service, span.http.response.status_code)                                   -- dependency health across the estate
{ span.peer.service = "billing-hub" && status = error } >> { }                                              -- (reverse) which requests were hit
{ kind = server && span.upstream.name = "support-case" && span.handled = "ignored" }   -- order created, ticket missing
{ kind = server && span.handled = "treated_as_success" }                                          -- timeouts reported as PROCESSED
{ span.auth.token_source = "fetched" } | rate() by(span.auth.provider)                 -- token endpoints hit per request (cache missing)
{ kind = client && span.call.purpose = "billing-hub.invoice_summary" } | quantile_over_time(duration, .95) by(span.call.version)
{ kind = server && span.correlation_id = "<C>" } && { kind = client && span.call.response.correlation_id != "<C>" }   -- upstream did not echo the id
```

---

## 7. Checklist — one outbound dependency

- [ ] §2 inventory row exists (auth / request / response / on-failure) for every call site of the dependency
- [ ] `peer.service` mapping added for its host(s) (purpose attribute when behind the gateway)
- [ ] Before the call: `call.purpose`, `call.version`, decisive ids (count or single id) and flags on the caller's span; `<dep>.outcome=pending` on the root
- [ ] Auth step registered: `auth.provider/grant/token_source/result` (+ `token_ttl_s`), on the span that performs it; no secret on any span/log
- [ ] Every outbound client sends `{{SOURCE_APP_HEADER}}` = this service's `service.name` (§5) so the callee's door span can name the caller (01 §3.1)
- [ ] Every retrying call wrapped in its own per-purpose span (`<dependency> <purpose> retry`) with `retry.count/max/outcome` (§4.4); two purposes = two spans; no span per attempt written by code
- [ ] After the call: response identifier/count/status/code read by the code registered on the caller's span; `<dep>.outcome` on the root; on any failure `upstream.name/status/code` + `handled` (first failure wins) on the root; `error_code`/`recordException` only through the exception handler (03 rule A) — never directly in the client mapper; ignored/fallback paths call `SpanOutcome.nonFatal("<dependency>.<action>", e)` (03 §2.1)
- [ ] Correlation id forwarded (canonical `{{CORRELATION_HEADER}}` in addition to the dependency's own spelling); upstream echo captured
- [ ] Node clients: axios interceptor equivalent (07 §N4)
- [ ] Verified: the CLIENT span carries `peer.service`; the parent carries `call.*` and, on a forced failure in a non-production environment, `upstream.*` + `handled`
