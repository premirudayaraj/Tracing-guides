# 01 — The Request Span: Register at the Door, Register Every Decision (Spring Boot API services)

> **Role:** API (Spring MVC, WebFlux, gRPC) — any service with a controller, a filter chain and an authorisation step. Apply after `00` has confirmed the role; combine with `02`/`03` (errors), `04` (outbound calls), `05` (messages), `06` (jobs). Placeholders are defined in `README.md §0`.
>
> **How to use with an AI assistant:** *"Instrument `<service>` per 01-REQUEST-SPAN-AND-AUTH-CHAIN.md. First run the §2 analysis and list every decision point you found with file:line; then apply §3–§6; do not deviate from the vocabulary in §7."*
>
> Legend: **MUST** = rule; **already** = usually in current code, keep; ➕ = add; ⚠ = confirm at runtime.

---

## 0. Reference patterns (what this is generalised from)

Four request-handling chains of a real estate were read end to end. They are four *different* designs, which is why the rules are written per concept (identity source, policy decision, permitted-data lookup, impersonation) and not per class name. Anonymised:

| Reference service | Chain (servlet entry → controller) | Decision engines |
|---|---|---|
| **A — billing API** (in-house REST library) | library filter (JWT/headers → request-context object, 401 before app code) → MDC filter → policy interceptor (`@Policy` on the handler) → policy enforcer → policy-input provider (user id, actor user, actor org, customer org; an "internal support org" guard) → DAO that loads the user's permissions from DB views → **policy engine** `POST /v1/data/{policy.key}/allow` (no cache) → result handler (`result=false` → 403 with a catalogue code) → controller → permitted-accounts lookup (DB view), org-account lookup through an admin API (support-org mode), asset lookup | policy engine, DB views, admin API |
| **B — order API** (same REST library) | library filter (JWT + identity/actor/channel headers, 401) → path-sanitising filter → controller reads `actor user ?? user`, actor org only when the channel is the internal portal → permitted accounts + assets lookup (DB views) → contact lookup (403 when no accounts) → **permission API** of the identity system (`/v1/permissions/{user}/assets`, master-admin flag) → in-memory ownership check (401 with a catalogue code) → record-id validation (404) | permission API, DB views, in-memory ownership |
| **C — account platform API** (filters + aspects) | correlation-id filter → security-headers filter → actor-context filter (actor/channel headers) → Spring Security `permitAll` → environment interceptor → read-only-mode interceptor (**feature flag**) → logging interceptor → user-context interceptor (Redis cache TTL → identity-service profile → accounts by org (JPA) → assets by user (admin API) → org hierarchy service) → MDC aspect → JWT aspect (`@RequiredJWT` → JWT context {user key, username, org id, roles, expiry, service-account flag, root org, hierarchy flag}; impersonation via `@AllowImpersonation` + internal-user token + corporate e-mail domain gate) → permission aspect (identity service `isAuthorized`) → **policy-sidecar aspect** (`/authorize` → `result.authorized/reason`, fail-closed) → role aspect → account-ownership validator | identity service, policy sidecar, Redis, DB, hierarchy service, feature flags |
| **D — authorisation service** (WebFlux) | exception handler → tracing filter (returns the trace id header) → authentication filter (client/user auth headers, JWKS) → client-information filter (gRPC principal → client id, client group) → act-on-behalf filter (header → gRPC permission check) → **policy engine** `POST /v1/data/<package>/{path}/allow` (retry ×3) → Redis provider-config cache, in-memory principal cache | policy engine, gRPC identity/client services |

Common facts: one correlation header on the wire everywhere, but MDC key names differ per service; no service put a custom attribute on a span; the agent already captured the identity headers as `http.request.header.*` where startup scripts attached it; `@WithSpan`/`@SpanAttribute` (`opentelemetry-instrumentation-annotations`) was the only manual-span API in application code and is the one standardised here; the only hand-built spans were in an outbox library (05 K8).

---

## 1. The model

```
 SERVER span  POST /billing/v3/accounts/search                 ← agent names it; code never renames it
 ├─ door:      {{PREFIX}}.correlation_id, user.name (masked), user.id, {{PREFIX}}.org_id, {{PREFIX}}.actor.*, {{PREFIX}}.channel_type, session.id
 ├─ authn:     {{PREFIX}}.authn.source=library|jwt|apikey|identity-headers   {{PREFIX}}.authn.result=ok|missing|expired|invalid|rejected_actor
 ├─ authz:     {{PREFIX}}.authz.provider=policy-engine|identity|policy-sidecar|permission-api|db|role  {{PREFIX}}.authz.policy=billing/get  {{PREFIX}}.authz.decision=allow|deny|error  {{PREFIX}}.authz.reason=NOT_ENOUGH_PERMISSIONS
 │   └─ CLIENT span POST /v1/data/billing/get/allow  (agent)  peer.service=policy-engine
 ├─ grants:    {{PREFIX}}.user.context_source=cache|identity  {{PREFIX}}.user.accounts_count=7  {{PREFIX}}.user.assets_count=3  {{PREFIX}}.impersonation=none|user|org  {{PREFIX}}.support_org_mode=false
 ├─ decisions: feature_flag.key=…, feature_flag.result.variant=…, {{PREFIX}}.cache_hit, {{PREFIX}}.account.filter=permitted|org, {{PREFIX}}.fallback=…, {{PREFIX}}.result_count, {{PREFIX}}.no_record
 ├─ INTERNAL span AccountService.searchBillingAccounts  (@WithSpan, only for fan-out/async/hotspot)
 │   ├─ CLIENT POST  peer.service=account-platform   http.response.status_code=200     ← 04: request ids/flags + response ids + decision on outcome, on the caller's span / root
 │   ├─ CLIENT POST  peer.service=billing-hub        http.response.status_code=503  error.type=503
 │   └─ JDBC SELECT contacts.user_v
 └─ outcome:   http.response.status_code, error.type, {{PREFIX}}.error_code={{ERR_PREFIX}}-4210460, {{PREFIX}}.upstream.name=billing-hub, {{PREFIX}}.upstream.status=503   ← 03
```

Five invariants:
1. **One trace per inbound request, one SERVER span per hop**, named `<METHOD> <route template>` by the agent. Code never renames it and never puts ids in the name.
2. **The root span is the ledger.** Every decision is an attribute on it (or on the `@WithSpan` method span that made it, if the decision is local to that unit of work). Children are for timing and transport.
3. **Register as soon as known, default before the branch.** An attribute set after `return`/`throw` never reaches the backend.
4. **One correlation id**, read or generated once at the door, in MDC, on the span, forwarded on every outbound call, returned in every error body and ➕ as a response header.
5. **No secrets, no PII, no payloads** on spans (00 §2 Q4); ids and enums only.

---

## 2. The analysis you run before writing code (mandatory)

Walk the request path in this order and produce the list of decision points **with file:line**. Only then place attributes.

1. **Door.** Find the first filter that sees the request (servlet `Filter`/`OncePerRequestFilter` with the lowest order, WebFlux `WebFilter` with the lowest `@Order`, or the in-house library's request filter). Note: which headers become identity (`{{USER_HEADER}}`, `{{USER_KEY_HEADER}}`, `{{ORG_HEADER}}`, `{{ACTOR_USER_HEADER}}`, `{{ACTOR_ORG_HEADER}}`, `{{CHANNEL_HEADER}}`, `{{SESSION_HEADER}}`, the `{{SOURCE_APP_HEADER}}` header, `Authorization`, an auth-token header, `{{APIKEY_HEADER}}`, client-id / act-on-behalf headers), where the correlation id is read/generated, which MDC keys are set, what returns 401 before any application code.
2. **Authentication.** Where is the token/header *validated* (JWT decode, JWKS, expiry, API key compare, the library's context resolver)? What object holds the result (request context, JWT context, client context, user context)? Which failures exist (missing, expired, invalid signature, wrong audience, actor headers with a non-internal token, impersonator outside the corporate e-mail domain)?
3. **Authorisation decision(s).** Every call that returns allow/deny: policy engine (`/v1/data/.../allow` → `result`), identity service (`isAuthorized` → `success`), policy sidecar (`/authorize` → `result.authorized`, `result.reason`), permission API (`/v1/permissions/{user}/assets`), `@RoleAllowed`/`hasRole`, DB ownership checks (account-ownership validator, requested accounts ⊆ permitted accounts, record-id validation). For each: the **inputs that matter** (policy key / action id / permission code / resource type / target resource name — never the token), the **output read**, whether it is **cached**, what a **deny** returns (status + error code), and what an **error** (provider unreachable, timeout) does — fail-open or fail-closed.
4. **Grants / permitted data.** Lookups that decide *what the user may see*: permitted accounts (DB views, JPA by org, admin API), asset lists, roles (JWT roles, master-admin flag), org hierarchy, user-context cache (Redis). Register **counts and sources**, never the lists.
5. **Impersonation / support-org / channel.** Where actor headers or the channel header switch the identity or the query (`isImpersonating()`, `isSupportOrgMode()`, `channel == internal-portal`, an "internal org" guard, a reseller shortcut). Each switch is a decision.
6. **Business decisions on the path.** Feature flags (read-only mode, listener enable flags), cache hit/miss, fallbacks ("permission lookup failed → no permitted accounts"), filters that skip, "no record → 200 empty" advices, version selection (upstream v1/v2/v3), batching, read-only mode.
7. **Outbound calls** (04) and **messages published** (05): what identifiers go out, what comes back, and what the code decides on the result.
8. **Outcome mapping** (03): the `@ControllerAdvice`/`ErrorWebExceptionHandler` that turns exceptions into status + `{{ERR_PREFIX}}-…` code.

Deliverable of the analysis: a table `# | file:line | decision | inputs that matter | output | attribute(s) to set`. Every row becomes one or more `setAttribute` calls in §3–§6.

---

## 3. R-DOOR — register the request at the boundary

**Where:** the **first** filter the request meets — `@Order(Ordered.HIGHEST_PRECEDENCE)`, **before** the in-house library filter and before Spring Security, so that a 401 produced by the library or by Spring Security still has a correlation id and the raw identity on its span (typically those 401s carry nothing). If a correlation-id filter already exists at that position, extend it; if the correlation-id logic lives in a later MDC filter, move that part forward and keep the rest where it is; WebFlux: a `WebFilter` at `Ordered.HIGHEST_PRECEDENCE` (before the authentication filter). Do not add a second correlation-id generator.

```java
// io.opentelemetry.api.trace.Span — no-op when the agent is off; add dependency io.opentelemetry:opentelemetry-api
Span span = Span.current();                                            // the agent's SERVER span
String cid = firstNonBlank(request.getHeader("{{CORRELATION_HEADER}}"), MDC.get("<existing key>"), UUID.randomUUID().toString()); // generate ONCE, here only
MDC.put("<existing key>", cid);          // this service's existing MDC key — keep it (dashboards depend on it)
MDC.put("correlationId", cid);           // ➕ common key across all services (LogQL: | json | correlationId="…")
span.setAttribute("{{PREFIX}}.correlation_id", cid);
span.setAttribute("{{PREFIX}}.correlation_id_generated", request.getHeader("{{CORRELATION_HEADER}}") == null);   // tells you which client sends none
response.setHeader("{{CORRELATION_HEADER}}", cid);                      // ➕ echo on every response, success too

// identity as received (masked where PII), before any validation:
span.setAttribute("user.name", mask(header("{{USER_HEADER}}")));       // login, masked abc…xyz (first 3 + x… + last 3)
span.setAttribute("user.id", header("{{USER_KEY_HEADER}}"));           // opaque key — only if the service has it
span.setAttribute("{{PREFIX}}.org_id", header("{{ORG_HEADER}}"));      // customer org id
span.setAttribute("{{PREFIX}}.channel_type", headerOr("{{CHANNEL_HEADER}}", "-"));   // internal portal, customer portal, …
span.setAttribute("{{PREFIX}}.source_app", headerOr("{{SOURCE_APP_HEADER}}", "-"));
span.setAttribute("{{PREFIX}}.actor.present", hasActorHeaders);        // {{ACTOR_USER_HEADER}} / {{ACTOR_ORG_HEADER}} present
if (hasActorHeaders) { span.setAttribute("{{PREFIX}}.actor.user_name", mask(header("{{ACTOR_USER_HEADER}}"))); span.setAttribute("{{PREFIX}}.actor.org_id", header("{{ACTOR_ORG_HEADER}}")); }
span.setAttribute("session.id", header("{{SESSION_HEADER}}"));          // if present
span.setAttribute("{{PREFIX}}.authn.result", "not_evaluated");          // default; the validation step (§4.1) overwrites — an exception before it leaves this value, not "ok"
try (Scope root = RequestSpan.open(span)) { chain.doFilter(request, response); }   // §6: makes the SERVER span reachable from every child scope; scope closed in the try-with-resources
finally { MDC.clear(); }
```
WebFlux: the same filter writes the span into the Reactor context instead of a thread scope — `chain.filter(exchange).contextWrite(ctx -> ctx.put(RequestSpan.KEY, span))` and `RequestSpan.root()` reads it through the agent's Reactor context propagation (⚠ verify once). Kotlin coroutines: launch with `MDCContext()` so MDC follows the coroutine; the agent propagates the OTel context.

* MUST read-or-generate the correlation id once, at the door; MUST keep existing MDC keys and ➕ add the common `correlationId`.
* MUST NOT put `Authorization`, auth-token headers, `{{APIKEY_HEADER}}`, client-auth headers or any token on the span.
* The agent's header capture stays as it is — attribute key `http.request.header.<header-in-lower-case>` (agents older than 2.x wrote underscores instead of hyphens ⚠ check one span; quote the key in TraceQL: `span."http.request.header.x-correlation-id"`); `{{PREFIX}}.correlation_id` is the one key that works on **every** span kind (Kafka, jobs, browser), which is why it is set explicitly.
* Health/readiness routes are never instrumented: the collector drops `http.route =~ ^/.*actuator/health/.*`; ➕ extend the filter to cover services whose probes live under a management path (`^/(.*actuator|management)/health.*`).

---

## 4. R-AUTH — register the authentication and authorisation chain

The chain is a sequence of decisions; each is registered **where it is made**, on the current span (the SERVER span — filters, interceptors and aspects run on the request thread before the controller; in WebFlux use the Reactor context's span or `Span.current()` inside the operator).

### 4.1 Authentication (who is calling)

```java
// where the token/header is validated (the JWT aspect, the library's context resolver, after the library filter)
span.setAttribute("{{PREFIX}}.authn.source", "jwt");             // library | jwt | apikey | identity-headers | service-account | none
span.setAttribute("{{PREFIX}}.authn.result", "ok");              // written where validation CONCLUDES (door default is not_evaluated); on failure one of:
//   missing | expired | invalid | rejected_actor (user/service-account token with actor headers) | not_internal (impersonator outside the corporate domain) | forwarded (this layer only forwards the token — Node BFFs)
span.setAttribute("{{PREFIX}}.authn.token_type", jwt.isServiceAccount() ? "service-account" : "user");   // never the token
span.setAttribute(AttributeKey.stringArrayKey("user.roles"), jwtContext.getRoles());   // string[] of role names, low cardinality (RequestSpan.set has no array overload — use the key)
```
* A 401 produced *inside* the service is a traced span with `{{PREFIX}}.authn.result != ok` + `{{PREFIX}}.error_code` (03). When the 401/403 is produced by Spring Security (no `@ControllerAdvice` runs), register it in the `AuthenticationEntryPoint` / `AccessDeniedHandler` (or the WebFlux `ServerAuthenticationEntryPoint`) with the same `SpanOutcome.record` call (03 §2); when it is produced by an in-house library filter, the only place is the library itself — until it is changed, the door filter in §3 guarantees at least the correlation id and the raw identity are on that span. 401s produced by the API gateway / edge proxy never reach the service and are not spans.

### 4.2 Authorisation decisions (may the caller do this)

One attribute set per decision engine, registered in the handler that reads the answer:

```java
// policy engine (OPA-style) — policy enforcer / result handler (reference A), policy client (reference D)
span.setAttribute("{{PREFIX}}.authz.provider", "policy-engine");
span.setAttribute("{{PREFIX}}.authz.policy", policy.key());                       // "billing/get" — the policy package path, not the URL
span.setAttribute("{{PREFIX}}.authz.decision", allowed ? "allow" : "deny");       // "error" when the engine is unreachable / input empty
span.setAttribute("{{PREFIX}}.authz.reason", allowed ? "-" : "NOT_ENOUGH_PERMISSIONS");   // catalogue key of the code the deny maps to
span.setAttribute("{{PREFIX}}.authz.input.actor", input.actorUser != null);      // booleans/counts about the input, never the input
span.setAttribute("{{PREFIX}}.authz.input.permissions_count", input.permissions.size());

// identity service permission check — permission aspect → identity client isAuthorized (reference C)
span.setAttribute("{{PREFIX}}.authz.provider", "identity");
span.setAttribute("{{PREFIX}}.authz.policy", permissionCheck.action() + ":" + permissionCheck.resourceType());   // "VIEW:BILLING_ACCOUNT"
span.setAttribute("{{PREFIX}}.authz.decision", response.isSuccess() ? "allow" : "deny");

// policy sidecar — sidecar aspect → sidecar client authorize (reference C)
span.setAttribute("{{PREFIX}}.authz.provider", "policy-sidecar");
span.setAttribute("{{PREFIX}}.authz.policy", actionId);                           // "action:use/getBillingAccount"
span.setAttribute("{{PREFIX}}.authz.decision", authorized ? "allow" : (failed ? "error" : "deny"));   // fail-closed: error is also a deny
span.setAttribute("{{PREFIX}}.authz.reason", result.getReason());                // only if low-cardinality; otherwise a code
span.setAttribute("{{PREFIX}}.authz.accounts_checked", perAccount.size());

// permission API role check — authorizer of the identity system (reference B)
span.setAttribute("{{PREFIX}}.authz.provider", "permission-api");  span.setAttribute("{{PREFIX}}.authz.policy", "MASTER_ADMIN");  span.setAttribute("{{PREFIX}}.authz.decision", isMasterAdmin ? "allow" : "deny");
span.setAttribute("{{PREFIX}}.authz.reseller_shortcut", resellerShortcutApplied);  // an override that bypasses the check for a category of accounts

// DB ownership / in-memory checks — account-ownership validator, requested ⊆ permitted, record-id validation
span.setAttribute("{{PREFIX}}.authz.provider", "db");  span.setAttribute("{{PREFIX}}.authz.policy", "account_ownership");  span.setAttribute("{{PREFIX}}.authz.decision", owns ? "allow" : "deny");  span.setAttribute("{{PREFIX}}.authz.reason", owns ? "-" : "ACCOUNT_NOT_BELONGS_TO_USER");

// role gate — role aspect / @RoleAllowed / hasRole
span.setAttribute("{{PREFIX}}.authz.provider", "role");  span.setAttribute("{{PREFIX}}.authz.policy", String.join(",", roleAllowed.value()));  span.setAttribute("{{PREFIX}}.authz.decision", ok ? "allow" : "deny");
```

* **Key rule when engines can vary per request** (reference C: the identity check only when the hierarchy flag is on, the sidecar only when enabled, then ownership): every engine writes its own set `{{PREFIX}}.authz.<provider>.decision|policy|reason` (`{{PREFIX}}.authz.policy-engine.decision`, `{{PREFIX}}.authz.identity.decision`, `{{PREFIX}}.authz.policy-sidecar.decision`, `{{PREFIX}}.authz.db.decision`, `{{PREFIX}}.authz.role.decision`); the **plain** keys `{{PREFIX}}.authz.decision` / `{{PREFIX}}.authz.reason` are the **overall** outcome (`deny` if any engine denied, `error` if any failed closed, else `allow`) and `{{PREFIX}}.authz.decided_by` names the engine that produced a deny/error. A service with exactly one engine may write only the plain keys plus `{{PREFIX}}.authz.provider`. The snippets above show the plain form for brevity; apply this rule literally.
* `{{PREFIX}}.authz.reason` is the **catalogue key** of the code the deny maps to (`NOT_ENOUGH_PERMISSIONS`, `ACCOUNT_NOT_BELONGS_TO_USER`, `UNAUTHORIZED_RESOURCE_ACCESS`, `ACT_ON_BEHALF_DENIED`) — never a free-text reason; `{{PREFIX}}.error_code` (03) carries exactly what the response body carries (if a service today derives the code from the HTTP status instead of the catalogue, register it as is — the mismatch is what the attribute reveals; 02 §7.1).
* Every deny/error is also an outcome (03): `{{PREFIX}}.error_code`, `error.type` = the exception class or the code; status `ERROR` only for 5xx-class failures (provider unreachable), **not** for a 403 — a deny is a correct answer.
* The authorisation call itself is a CLIENT span (agent) under the SERVER span with `peer.service=policy-engine|identity|policy-sidecar|permission-api` (04 §3) — its duration is the answer to "is the policy engine slow"; the decision is on the parent.
* Never register the policy input document, the token, resource names that embed user data, or the `permissions[]` list. Counts and booleans only.

### 4.2a Standard Spring Security (projects without in-house aspects)

The four reference chains are examples; **run §2 on every project**. Where a project uses plain Spring Security instead of an in-house library/aspects — detectors: `SecurityFilterChain`, `oauth2ResourceServer()`, `JwtDecoder`/`ReactiveJwtDecoder`, `@EnableMethodSecurity`, `@PreAuthorize`/`@PostAuthorize`/`@PostFilter`, `@Secured`, `@RolesAllowed`, `hasRole`/`hasAuthority`, `AuthorizationManager`/`ReactiveAuthorizationManager`, gRPC `ServerInterceptor` — register with the framework's own hooks, once per service:
```java
@Component class AuthTracing {
  @EventListener void ok(AuthenticationSuccessEvent e)            { Span s = RequestSpan.root(); s.setAttribute("{{PREFIX}}.authn.source", "jwt"); s.setAttribute("{{PREFIX}}.authn.result", "ok"); s.setAttribute(AttributeKey.stringArrayKey("user.roles"), roles(e.getAuthentication())); }
  @EventListener void ko(AbstractAuthenticationFailureEvent e)    { Span s = RequestSpan.root(); s.setAttribute("{{PREFIX}}.authn.result", kind(e.getException())); }   // missing|expired|invalid
  @EventListener void denied(AuthorizationDeniedEvent<?> e)       { Span s = RequestSpan.root(); s.setAttribute("{{PREFIX}}.authz.provider", "spring-security"); s.setAttribute("{{PREFIX}}.authz.policy", String.valueOf(e.getAuthorizationResult())); s.setAttribute("{{PREFIX}}.authz.decision", "deny"); }
  @Bean AuthorizationEventPublisher publisher(ApplicationEventPublisher p) { return new SpringAuthorizationEventPublisher(p); }   // required for AuthorizationDeniedEvent
}
// AuthenticationEntryPoint / AccessDeniedHandler (MVC) or ServerAuthenticationEntryPoint / ServerAccessDeniedHandler (WebFlux): SpanOutcome.record("{{ERR_PREFIX}}-<module>005", "NOT_ENOUGH_PERMISSIONS", 401|403, null) — the ControllerAdvice never sees these
```
Method-security expressions (`@PreAuthorize("hasRole('ADMIN')")`) register `{{PREFIX}}.authz.policy` = the expression text (low cardinality, it is source code) through the `AuthorizationDeniedEvent`; a successful check registers nothing extra beyond `user.roles`.

### 4.3 Grants — what the user was given for this request

```java
// user-context interceptor/service (reference C), contact + permitted-accounts services (references A, B)
span.setAttribute("{{PREFIX}}.user.context_source", fromCache ? "cache" : "identity");   // cache hit or identity-service profile call
span.setAttribute("{{PREFIX}}.user.accounts_source", "permissions_view");                // permissions_view | org_accounts(admin API) | accounts_by_org(JPA) | hierarchy
span.setAttribute("{{PREFIX}}.user.accounts_count", permitted.size());                   // 0 is the interesting value
span.setAttribute("{{PREFIX}}.user.assets_count", assets.size());
span.setAttribute("{{PREFIX}}.user.hierarchy_enabled", userContext.isHierarchyEnabled());
span.setAttribute("{{PREFIX}}.user.is_internal", contact.isInternalUser());
span.setAttribute("{{PREFIX}}.user.accounts_fallback", true);                             // when "lookup failed → defaulting to no permitted accounts"
```

### 4.4 Impersonation, support-org mode, channel

```java
// actor context (reference C), isSupportOrgMode (reference A), channel == internal portal (reference B), internal-org guard
span.setAttribute("{{PREFIX}}.impersonation", "none");     // default at the door; overwrite: user ({{ACTOR_USER_HEADER}}) | org ({{ACTOR_ORG_HEADER}} only)
span.setAttribute("{{PREFIX}}.support_org_mode", supportOrgMode);
span.setAttribute("{{PREFIX}}.internal_guard", "ok");      // ok | missing_actor (401 for the internal org without actor headers)
span.setAttribute("{{PREFIX}}.effective.org_id", effectiveOrgId);                          // the org the query actually used (may differ from {{PREFIX}}.org_id)
span.setAttribute("{{PREFIX}}.effective.user_name", mask(effectiveUser));                  // actor when impersonating
```

### 4.5 Read-only mode and other request-level gates

```java
// read-only-mode interceptor (feature flag), environment interceptor (env header validation)
span.setAttribute("feature_flag.key", "applicationReadOnlyMode");
span.setAttribute("feature_flag.result.variant", readOnly ? "on" : "off");
span.setAttribute("feature_flag.provider.name", "<your flag provider>");
span.setAttribute("{{PREFIX}}.gate.readonly", readOnly && isWrite ? "blocked" : "pass");
span.setAttribute("{{PREFIX}}.env", envHeader);
```

---

## 5. R-DECIDE — register every decision on the path (controller and service)

The controller adds the **request's business identifiers** before calling the service; the service adds **decisions** where they are made. No new spans for that.

```java
@PostMapping("/accounts/billing/search")
@Policy(key = "billing/get")
public ResponseEntity<BillingAccountSearchResponse> search(@RequestBody BillingAccountSearchRequest req, RequestContext ctx) {
  Span span = Span.current();
  span.setAttribute("{{PREFIX}}.customer_account", req.getCustomerAccountNumber());     // as soon as known, before any call
  span.setAttribute("{{PREFIX}}.page_size", req.getLimit());  span.setAttribute("{{PREFIX}}.page_offset", req.getOffset());
  span.setAttribute("{{PREFIX}}.search_scope", req.getSearchScope());      // enum
  return ResponseEntity.ok(service.searchBillingAccounts(ctx, req).join());
}

@WithSpan("AccountService.searchBillingAccounts")      // only because it fans out to two upstreams + DB
public CompletableFuture<BillingAccountSearchResponse> searchBillingAccounts(RequestContext ctx, BillingAccountSearchRequest req) {
  RequestSpan.set("{{PREFIX}}.account.filter", "permitted");             // changes the response → ROOT span (default; overwritten below)
  if (commonService.isSupportOrgMode(ctx)) RequestSpan.set("{{PREFIX}}.account.filter", "org");
  RequestSpan.here("{{PREFIX}}.cache_hit", "false");                      // local to this unit of work → the @WithSpan span (default before the lookup)
  ...
  RequestSpan.set("{{PREFIX}}.billing-hub.version", "v3");                // which upstream contract was chosen → root
  RequestSpan.here("{{PREFIX}}.billing-hub.batches", String.valueOf(batches.size()));   // fan-out shape → this span
  RequestSpan.set("{{PREFIX}}.result_count", result.size());              // root
  RequestSpan.set("{{PREFIX}}.no_record", result.isEmpty());              // root — "empty 200" advices make this invisible otherwise
  return ...;
}
```
**Root or here?** One test: *does it change the response, the access decision or the outcome the client sees?* → `RequestSpan.set` (root). Otherwise (timing shape, cache hit of a helper, batch sizes) → `RequestSpan.here` (the innermost span). The TraceQL in §9 and 03 §6 query these keys on `kind = server`.

Decision catalogue — register these whenever they exist (names in §7):

| Decision type | Attribute(s) | Typical places |
|---|---|---|
| Feature flag evaluated | `{{PREFIX}}.flag.<key>=<variant>` for **every** flag evaluated on the request (one key per flag — a read-only-mode flag is often evaluated on every request, so most requests see more than one flag); additionally the standard `feature_flag.key` / `feature_flag.result.variant` / `feature_flag.provider.name` for the single flag that decided the code path of *this* span, if there is one | flag-provider gates (read-only mode, listener enable flags, migration toggles); `@ConditionalOnProperty` toggles are startup facts — one INFO line at startup, nothing per request |
| Cache hit / miss | `{{PREFIX}}.cache_hit` (+ `{{PREFIX}}.cache.name`) | Redis user-context cache, OAuth token cache, in-memory principal cache, a Node help-topics cache fallback |
| Fallback taken | `{{PREFIX}}.fallback=<what>` (`no_permitted_accounts`, `empty_contact`, `default_project_null`, `help_topics_cache`) | "lookup failed → proceed without filter" paths, a timeout → empty object, a hierarchy lookup → null |
| Filter / skip | `{{PREFIX}}.skipped=true`, `{{PREFIX}}.skip_reason=<enum>` | listener filters (05), no-record → empty 200 advices |
| Version / route chosen | `{{PREFIX}}.<dep>.version` (`v1\|v2\|v3`), `{{PREFIX}}.strategy` | upstream v1/v2/v3 contracts, search-by-company vs search-by-account |
| Fan-out shape | `{{PREFIX}}.<dep>.batches`, `{{PREFIX}}.batch_size`, `{{PREFIX}}.parallelism` | enrichment in batches of N, `CompletableFuture` joins |
| Result | `{{PREFIX}}.result_count`, `{{PREFIX}}.no_record`, `{{PREFIX}}.truncated` (hard caps) | every service method returning a list/page |
| Validation | `{{PREFIX}}.validation.failed=true`, `{{PREFIX}}.validation.field_count` | custom validators, `MethodArgumentNotValidException` handler |
| Retry | `{{PREFIX}}.retry.attempts`, `{{PREFIX}}.retry.exhausted` | `@Retryable`/`@Recover`, policy-engine retry ×3 |
| Timeout policy | `{{PREFIX}}.timeout_ms` when it is a per-call decision | `CompletableFuture.get(20, SECONDS)`, a long read timeout for an LLM proxy |

Rules:
* **Defaults first.** Write the default value at the top of the method, overwrite on the branch. Every span then has the key and TraceQL `= false` works.
* **Counts, not lists; enums, not text.** `{{PREFIX}}.user.accounts_count=0` not the list; `{{PREFIX}}.skip_reason=EMPTY_ACCOUNT_NUMBER` not the log sentence.
* **Never in a loop.** For a fan-out over N items register `N`, the number failed, and (only if there is exactly one that matters) its id.
* **`@WithSpan` only** on a public use-case method that fans out to ≥2 downstreams, runs async, or is a known hotspot; name = `Class.method`; never on getters, mappers, validators, DAOs (the agent's JDBC spans already exist).

---

## 6. R-ROOT helper — pin decisions to the request span from anywhere

A decision made inside a `@WithSpan` method, a Reactor operator or a worker thread must still be searchable on the **root** span. Add one small helper per service (no framework, no new dependency):

```java
// tracing/RequestSpan.java  ➕
public final class RequestSpan {
  private static final ContextKey<Span> ROOT = ContextKey.named("{{PREFIX}}-root-span");
  /** call in the door filter: makes the SERVER span reachable from every child scope on this request */
  public static Scope open(Span root) { return Context.current().with(ROOT, root).with(WRITTEN, ConcurrentHashMap.newKeySet()).makeCurrent(); }
  public static Span root() { Span s = Context.current().get(ROOT); return s != null ? s : Span.current(); }
  public static void set(String key, String v) { if (v != null) root().setAttribute(key, v); }
  public static void set(String key, long v)   { root().setAttribute(key, v); }
  public static void set(String key, boolean v){ root().setAttribute(key, v); }
  public static void here(String key, String v){ Span.current().setAttribute(key, v); }   // on the innermost span
  /** first writer wins (the OTel API cannot read attributes back): a per-request Set<String> of written keys travels in the same Context */
  public static void setIfAbsent(String key, Object v) { Set<String> w = Context.current().get(WRITTEN); if (w == null || w.add(key)) setAny(root(), key, v); }
  public static final String KEY = "{{PREFIX}}-root-span";                                        // WebFlux: Reactor context key (§3)
  private static final ContextKey<Set<String>> WRITTEN = ContextKey.named("{{PREFIX}}-written");
}
```
`open()` is called **once**, in the door filter (§3), inside a try-with-resources around `chain.doFilter`; nothing else opens it. Arrays (`user.roles`) use `root().setAttribute(AttributeKey.stringArrayKey(...), list)`.
* The agent propagates `Context` through executors, `CompletableFuture`, WebClient/Reactor and Kafka consumers, so `RequestSpan.root()` works on worker threads and inside operators (⚠ verify once in a non-production environment with the agent on: a `RequestSpan.set` from an `@Async` method appears on the SERVER span).
* Authorisation, grants, impersonation and outcome attributes go on the **root** (`RequestSpan.set`); decisions local to a fan-out unit go on the **current** span (`RequestSpan.here`) — and, if they change the response, also on the root.
* **MDC is not `Context`.** Use the MDC-aware executors that usually already exist (an MDC-copying `ThreadPoolTaskExecutor`, a `TaskDecorator`, a Reactor MDC hook) for any traced work off the request thread; never `CompletableFuture.runAsync(task)` on the common pool (a typical defect: an async e-mail dispatch whose log lines have no correlation id).

---

## 7. Vocabulary (normative — do not invent parallel names)

**Standard (set by the agent or by the rules above; keep these names exactly):** `http.request.method`, `http.route`, `http.response.status_code`, `url.path`, `server.address`, `client.address`, `user_agent.original`, `http.request.header.<name>` (captured headers), `db.system`, `db.statement`, `db.operation`, `messaging.*` (05), `peer.service`, `error.type`, `exception.type|message|stacktrace`, `user.name`, `user.id`, `user.roles`, `session.id`, `feature_flag.key`, `feature_flag.result.variant`, `feature_flag.provider.name`, `code.function.name`, `thread.name`.

**Company (`{{PREFIX}}.`, snake_case, string/number/bool):**

| Group | Keys |
|---|---|
| Door | `{{PREFIX}}.correlation_id`, `{{PREFIX}}.correlation_id_generated`, `{{PREFIX}}.org_id`, `{{PREFIX}}.channel_type`, `{{PREFIX}}.source_app`, `{{PREFIX}}.env`, `{{PREFIX}}.actor.present`, `{{PREFIX}}.actor.user_name` (masked), `{{PREFIX}}.actor.org_id` |
| Authn | `{{PREFIX}}.authn.source`, `{{PREFIX}}.authn.result`, `{{PREFIX}}.authn.token_type` |
| Authz | `{{PREFIX}}.authz.provider`, `{{PREFIX}}.authz.policy`, `{{PREFIX}}.authz.decision` (overall), `{{PREFIX}}.authz.reason` (catalogue key), `{{PREFIX}}.authz.decided_by`, `{{PREFIX}}.authz.<provider>.decision\|policy\|reason` (per engine: policy-engine, identity, policy-sidecar, permission-api, db, role, spring-security), `{{PREFIX}}.authz.input.*` (booleans/counts), `{{PREFIX}}.authz.accounts_checked`, `{{PREFIX}}.authz.reseller_shortcut` |
| Grants | `{{PREFIX}}.user.context_source`, `{{PREFIX}}.user.accounts_source`, `{{PREFIX}}.user.accounts_count`, `{{PREFIX}}.user.assets_count`, `{{PREFIX}}.user.hierarchy_enabled`, `{{PREFIX}}.user.is_internal`, `{{PREFIX}}.user.accounts_fallback` |
| Impersonation | `{{PREFIX}}.impersonation` (`none\|user\|org`), `{{PREFIX}}.support_org_mode`, `{{PREFIX}}.internal_guard`, `{{PREFIX}}.effective.org_id`, `{{PREFIX}}.effective.user_name` (masked) |
| Business ids | `{{PREFIX}}.customer_account`, `{{PREFIX}}.billing_account`, `{{PREFIX}}.account_number`, `{{PREFIX}}.invoice_number`, `{{PREFIX}}.order_number`, `{{PREFIX}}.document_id`, `{{PREFIX}}.site_id`, `{{PREFIX}}.project_id`, `{{PREFIX}}.reference_id`, `{{PREFIX}}.agreement_number`, `{{PREFIX}}.ticket_id`, `{{PREFIX}}.master_data_id`, `{{PREFIX}}.audit_id` (row written by a consumer/job) — extend with your own opaque ids, never names |
| Decisions | `{{PREFIX}}.cache_hit`, `{{PREFIX}}.cache.name`, `{{PREFIX}}.fallback`, `{{PREFIX}}.skipped`, `{{PREFIX}}.skip_reason`, `{{PREFIX}}.<dep>.version`, `{{PREFIX}}.strategy`, `{{PREFIX}}.account.filter`, `{{PREFIX}}.gate.readonly`, `{{PREFIX}}.flag.<key>`, `{{PREFIX}}.validation.failed`, `{{PREFIX}}.validation.field_count`, `{{PREFIX}}.retry.attempts`, `{{PREFIX}}.retry.exhausted`, `{{PREFIX}}.timeout_ms` |
| Shape / result | `{{PREFIX}}.page_size`, `{{PREFIX}}.page_offset`, `{{PREFIX}}.sort`, `{{PREFIX}}.search_scope`, `{{PREFIX}}.batch_size`, `{{PREFIX}}.<dep>.batches`, `{{PREFIX}}.parallelism`, `{{PREFIX}}.result_count`, `{{PREFIX}}.no_record`, `{{PREFIX}}.truncated` |
| Outcome (03) | `{{PREFIX}}.error_code` (exactly the response body's code), `{{PREFIX}}.error_key`, `{{PREFIX}}.handled_error_code`, `{{PREFIX}}.upstream.name`, `{{PREFIX}}.upstream.status`, `{{PREFIX}}.upstream.code` (first failing dependency wins), `{{PREFIX}}.handled` (`mapped\|passthrough\|swallowed\|fallback\|treated_as_success\|retried\|fail_closed\|rethrown`) |
| Handover (04) | `{{PREFIX}}.call.purpose`, `{{PREFIX}}.call.version`, `{{PREFIX}}.call.id`, `{{PREFIX}}.call.ids_count`, `{{PREFIX}}.call.flags`, `{{PREFIX}}.call.page_size`, `{{PREFIX}}.call.<purpose>.*` (several calls in one method), `{{PREFIX}}.call.response.id\|count\|status\|code\|correlation_id\|request_id`, `{{PREFIX}}.<dep>.outcome` (per dependency, on the root); auth step: `{{PREFIX}}.auth.provider`, `{{PREFIX}}.auth.grant`, `{{PREFIX}}.auth.token_source`, `{{PREFIX}}.auth.token_ttl_s`, `{{PREFIX}}.auth.result`, `{{PREFIX}}.auth.on_failure` |
| Messaging (05) | `{{PREFIX}}.event.type`, `{{PREFIX}}.event.status`, `{{PREFIX}}.event.source`, `{{PREFIX}}.route` (handler chosen by event type), `{{PREFIX}}.consumer.action`, `{{PREFIX}}.publish.result`, `{{PREFIX}}.publish.attempt`, `{{PREFIX}}.publish.partition`, `{{PREFIX}}.publish.offset`, `{{PREFIX}}.correlation_id_generated` |
| Jobs (06) | `{{PREFIX}}.job.name`, `{{PREFIX}}.job.run_id`, `{{PREFIX}}.job.trigger` (`scheduled\|api\|message\|cronjob\|manual`), `{{PREFIX}}.job.status` (`running\|success\|failed\|partial\|skipped_lock`), `{{PREFIX}}.job.batch_status` (raw Spring Batch status), `{{PREFIX}}.job.exit_code`, `{{PREFIX}}.job.lock`, `{{PREFIX}}.job.params` (names only), `{{PREFIX}}.job.execution_id`, `{{PREFIX}}.job.instance_id`, `{{PREFIX}}.job.step`, `{{PREFIX}}.job.step.execution_id\|status\|read\|write\|skip\|rollback`, `{{PREFIX}}.job.items_total\|processed\|failed\|skipped`, `{{PREFIX}}.check.<system>` (per-system comparison result of a monitoring run) |
| UI (07) | `{{PREFIX}}.screen`, `{{PREFIX}}.mfe`, `{{PREFIX}}.endpoint`, `{{PREFIX}}.error_type`, `{{PREFIX}}.step` (phase of a long flow), `{{PREFIX}}.proxy.target` (BFF) |

Never: tokens, API keys, `Authorization`, OAuth secrets, SASL/cloud credentials, e-mails, names, addresses, phone numbers, request/response bodies, policy input documents, SQL literals (`db-statement-sanitizer` **on** everywhere — §8), lists of accounts.

---

## 8. Runtime wiring, MDC and logs

* Agent flags (startup scripts / ConfigMap): `-javaagent:opentelemetry-javaagent.jar`, `-Dotel.exporter.otlp.endpoint={{COLLECTOR_ENDPOINT}}`, `-Dotel.service.name=<service>`, `-Dotel.metrics.exporter=none`, header capture `-Dotel.instrumentation.http.server.capture-request-headers={{USER_HEADER}},{{CORRELATION_HEADER}},{{ORG_HEADER}},{{SESSION_HEADER}}`; some templates disable `spring-scheduling` and `spring-rabbit` instrumentation (06, 05 K6). ➕ `-Dotel.propagators=tracecontext,baggage,b3,b3multi` everywhere (estates mix `b3`, `tracecontext,b3` and the default) so no hop breaks. ➕ Never run with `-Dotel.instrumentation.common.db-statement-sanitizer.enabled=false` (it puts SQL literals — account numbers, user names — into `db.statement`). ➕ `-Dotel.instrumentation.common.peer-service-mapping=<host>=<name>,…` per environment (04).
* Dependencies to add where missing: `io.opentelemetry:opentelemetry-api`, `io.opentelemetry.instrumentation:opentelemetry-instrumentation-annotations` (versions from `io.opentelemetry:opentelemetry-bom`; ⚠ align with the agent in the image). Never add an SDK/exporter/second APM agent to a service.
* Micrometer bridges: keep until the Brave/Zipkin reporter has no consumer, but ➕ print the agent's ids next to the bridge's: `trace_id:%X{trace_id:-} span_id:%X{span_id:-}` in every logback pattern (JSON encoders: add `"trace_id"`,`"span_id"` fields). Log-to-trace derived fields use `trace_id`.
* Log once at the point of failure and once where the error code is assigned; every line after the door carries `correlationId` (common key) through MDC. Log labels come from your log shipper (typically container/namespace/pod labels, not `app`); services with a bracketed text pattern are searched with `|= "correlationId:<id>"`, services with JSON logs with `| json | correlationId="<id>"`.

---

## 9. Verification (TraceQL) and checklist

```
{ resource.service.name = "<service name of the deployment>" && span.{{PREFIX}}.correlation_id = "<uuid>" }   -- the request and all its hops
{ kind = server && span.{{PREFIX}}.authz.decision = "deny" } | by(span.{{PREFIX}}.authz.provider, span.{{PREFIX}}.authz.policy, span.{{PREFIX}}.authz.reason)
{ kind = server && span.{{PREFIX}}.authz.decision = "error" }                                                  -- policy engine failures (fail-closed denies)
{ kind = server && span.{{PREFIX}}.user.accounts_count = 0 && span.http.response.status_code = 200 }           -- silent empty answers
{ kind = server && span.{{PREFIX}}.impersonation != "none" } | by(span.{{PREFIX}}.channel_type, span.http.route)
{ kind = server && span.feature_flag.key = "applicationReadOnlyMode" && span.{{PREFIX}}.gate.readonly = "blocked" }
{ kind = server && span.{{PREFIX}}.fallback != nil } | by(span.{{PREFIX}}.fallback)
{ kind = server && span.{{PREFIX}}.correlation_id_generated = true } | by(span.http.route)                     -- clients that send no correlation id
{ span.http.route =~ ".*health.*" }                                                                             -- must be empty
```

- [ ] §2 analysis table produced (file:line per decision) before any code
- [ ] Door filter: correlation id read-or-generated once, MDC (existing key + `correlationId`), `{{PREFIX}}.correlation_id`, identity/actor/channel/session attributes, response header echoed; no tokens
- [ ] Authn result registered where validation happens; each authz engine registered where its answer is read (`provider/policy/decision/reason`), overall decision on the root; grants as counts + source; impersonation/support-org/channel switches; read-only/flag gates
- [ ] Controller: business ids before the service call; service: defaults first, decisions/fallbacks/skips/versions/result counts; `@WithSpan` only on fan-out/async/hotspots
- [ ] `RequestSpan` helper in place; async work on MDC-aware executors; no common-pool `runAsync`
- [ ] Outcome per 03 (`error.type`, `{{PREFIX}}.error_code`, upstream status/code), handover per 04, messages per 05
- [ ] Vocabulary §7 only; no PII/secrets/lists; `db-statement-sanitizer` on
- [ ] Health routes dropped (collector filter covers this service's management path); propagators unified
- [ ] Verified with the TraceQL above on one real request in an agent-enabled environment; log line ↔ trace by `trace_id`, log ↔ span ↔ error body by correlation id
