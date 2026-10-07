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
 ├─ door:      correlation_id, caller.service=portal-bff (+ caller.source), user.name (masked), user.id, org_id, actor.*, channel_type, session.id
 ├─ authn:     authn.source=library|jwt|apikey|identity-headers   authn.result=ok|missing|expired|invalid|rejected_actor
 ├─ authz:     authz.provider=policy-engine|identity|policy-sidecar|permission-api|db|role  authz.policy=billing/get  authz.decision=allow|deny|error  authz.reason=NOT_ENOUGH_PERMISSIONS
 │   └─ CLIENT span POST /v1/data/billing/get/allow  (agent)  peer.service=policy-engine
 ├─ grants:    grants.context_source=cache|identity  grants.accounts_count=7  grants.assets_count=3  impersonation=none|user|org  support_org_mode=false
 ├─ decisions: feature_flag.key=…, feature_flag.result.variant=…, cache_hit, account.filter=permitted|org, fallback=…, result_count, no_record
 ├─ INTERNAL span AccountService.searchBillingAccounts  (@WithSpan, only for fan-out/async/hotspot)
 │   ├─ CLIENT POST  peer.service=account-platform   http.response.status_code=200     ← 04: request ids/flags + response ids + decision on outcome, on the caller's span / root
 │   ├─ CLIENT POST  peer.service=billing-hub        http.response.status_code=503  error.type=503
 │   └─ JDBC SELECT contacts.user_v
 └─ outcome:   http.response.status_code, error.type, error_code={{ERR_PREFIX}}-4210460, upstream.name=billing-hub, upstream.status=503   ← 03
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

1. **Door.** Find the first filter that sees the request (servlet `Filter`/`OncePerRequestFilter` with the lowest order, WebFlux `WebFilter` with the lowest `@Order`, or the in-house library's request filter). Note **how the calling service can be identified** (§3.1 — verify, do not assume: a mesh client-certificate header, a gateway-injected app header, a self-declared `{{SOURCE_APP_HEADER}}`, an API key that maps to a named application, a service-account token's client claim — and which of these this service actually reads today, file:line). Note: which headers become identity (`{{USER_HEADER}}`, `{{USER_KEY_HEADER}}`, `{{ORG_HEADER}}`, `{{ACTOR_USER_HEADER}}`, `{{ACTOR_ORG_HEADER}}`, `{{CHANNEL_HEADER}}`, `{{SESSION_HEADER}}`, the `{{SOURCE_APP_HEADER}}` header, `Authorization`, an auth-token header, `{{APIKEY_HEADER}}`, client-id / act-on-behalf headers), where the correlation id is read/generated, which MDC keys are set, what returns 401 before any application code.
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
span.setAttribute("correlation_id", cid);
span.setAttribute("correlation_id_generated", request.getHeader("{{CORRELATION_HEADER}}") == null);   // tells you which client sends none
response.setHeader("{{CORRELATION_HEADER}}", cid);                      // ➕ echo on every response, success too

// identity as received (masked where PII), before any validation:
span.setAttribute("user.name", mask(header("{{USER_HEADER}}")));       // login, masked abc…xyz (first 3 + x… + last 3)
span.setAttribute("user.id", header("{{USER_KEY_HEADER}}"));           // opaque key — only if the service has it
span.setAttribute("org_id", header("{{ORG_HEADER}}"));      // customer org id
span.setAttribute("channel_type", headerOr("{{CHANNEL_HEADER}}", "-"));   // internal portal, customer portal, …
Caller c = CallerIdentity.resolve(request);                            // §3.1: WHO CALLED — the calling service's name, from the strongest source available
span.setAttribute("caller.service", c.name());               // "billing-api" | "portal-bff" | "external" | "unknown" — the name itself, never the transport it arrived through
span.setAttribute("caller.source", c.source());              // mesh-cert | gateway-header | api-key | jwt-client | source-app-header | none — how the name was obtained (§3.1 order)
if (c.declaredDiffers()) span.setAttribute("caller.declared", c.declared());   // the self-declared {{SOURCE_APP_HEADER}} value when a stronger source disagrees with it
span.setAttribute("actor.present", hasActorHeaders);        // {{ACTOR_USER_HEADER}} / {{ACTOR_ORG_HEADER}} present
if (hasActorHeaders) { span.setAttribute("actor.user_name", mask(header("{{ACTOR_USER_HEADER}}"))); span.setAttribute("actor.org_id", header("{{ACTOR_ORG_HEADER}}")); }
span.setAttribute("session.id", header("{{SESSION_HEADER}}"));          // if present
span.setAttribute("authn.result", "not_evaluated");          // default; the validation step (§4.1) overwrites — an exception before it leaves this value, not "ok"
try (Scope root = RequestSpan.open(span)) { chain.doFilter(request, response); }   // §6: makes the SERVER span reachable from every child scope; scope closed in the try-with-resources
finally { MDC.clear(); }
```
WebFlux: the same filter writes the span into the Reactor context instead of a thread scope — `chain.filter(exchange).contextWrite(ctx -> ctx.put(RequestSpan.KEY, span))` and `RequestSpan.root()` reads it through the agent's Reactor context propagation (⚠ verify once). Kotlin coroutines: launch with `MDCContext()` so MDC follows the coroutine; the agent propagates the OTel context.

* MUST read-or-generate the correlation id once, at the door; MUST keep existing MDC keys and ➕ add the common `correlationId`.
* MUST register the **calling service's name** (`caller.service`) on the door span of every service, resolved per §3.1; `external`/`unknown` when nothing identifies it — never an invented name.
* MUST NOT put `Authorization`, auth-token headers, `{{APIKEY_HEADER}}`, client-auth headers or any token on the span.
* The agent's header capture stays as it is — attribute key `http.request.header.<header-in-lower-case>` (agents older than 2.x wrote underscores instead of hyphens ⚠ check one span; quote the key in TraceQL: `span."http.request.header.x-correlation-id"`); `correlation_id` is the one key that works on **every** span kind (Kafka, jobs, browser), which is why it is set explicitly.
* Health/readiness routes are never instrumented: the collector drops `http.route =~ ^/.*actuator/health/.*`; ➕ extend the filter to cover services whose probes live under a management path (`^/(.*actuator|management)/health.*`).

### 3.1 Who called — the calling service's name on the inbound span

`peer.service` on a CLIENT span names the service being called; nothing standard names the service that **called** on the SERVER span, and without it the callee cannot answer "who is sending me these requests" when the trace is broken (agent off upstream, a non-instrumented caller, a gateway in between). So every door span carries **one canonical attribute**, `caller.service`, whose value is the caller's **name** — the same string that is the caller's `service.name` and the callee's `peer.service` entry in the mapping table (04 §3): one naming table per estate, used on both sides.

The thing recorded is the *name*; the transport is only the **mechanism** for obtaining it. Resolve in this order and stop at the first that yields a name (`caller.source` says which one did):

| Order | Source (`caller.source`) | Where the name comes from | Trust |
|---|---|---|---|
| 1 | `mesh-cert` | the sidecar's client-certificate header (Envoy/Istio `X-Forwarded-Client-Cert`: the `URI=spiffe://<trust-domain>/ns/<namespace>/sa/<service-account>` element → service-account → service name via the naming table) | strong **only** when the mesh sets/overwrites the header (`forwardClientCertDetails: SANITIZE_SET`) — otherwise a client can forge it; ⚠ confirm the mesh setting once per cluster |
| 2 | `gateway-header` | a header the API gateway injects after verifying the credential (`{{GATEWAY_CALLER_HEADER}}`: the developer app / client name) | strong when the gateway strips the same header from inbound traffic; ⚠ confirm |
| 3 | `api-key` | the service authenticates callers with `{{APIKEY_HEADER}}` and has (or ➕ gets) a **registry** `key → application name`; register the **name**, never the key | strong |
| 4 | `jwt-client` | a service-account token's client claim (`azp`, `client_id`, `appid`, or `sub` for service accounts) mapped through the registry | strong |
| 5 | `source-app-header` | the calling service **declares itself** in `{{SOURCE_APP_HEADER}}` (04 §5 makes every outbound client send its own `service.name` there) | self-declared — enough for internal traffic, not for authorisation |
| 6 | `none` | nothing identifies a service: `external` if the request came through the public gateway / a browser channel (`{{CHANNEL_HEADER}}` or gateway header present), else `unknown` | — |

```java
// tracing/CallerIdentity.java ➕ — one resolver per service; the door filter calls it once. Registry = a small map in config: apikeys.<name>=<key>, jwt-clients.<client_id>=<name>, service-accounts.<sa>=<name>
public static Caller resolve(HttpServletRequest r) {
  String declared = r.getHeader("{{SOURCE_APP_HEADER}}");
  String fromMesh = MeshCert.serviceName(r.getHeader("X-Forwarded-Client-Cert"));   // parses the URI=spiffe://…/sa/<sa> element → registry; null when absent or the mesh does not sanitise it
  if (fromMesh != null)                          return Caller.of(fromMesh, "mesh-cert", declared);
  String fromGw = r.getHeader("{{GATEWAY_CALLER_HEADER}}");
  if (notBlank(fromGw))                          return Caller.of(fromGw, "gateway-header", declared);
  String fromKey = registry.appOf(r.getHeader("{{APIKEY_HEADER}}"));                // name only — the key value never leaves this method
  if (fromKey != null)                           return Caller.of(fromKey, "api-key", declared);
  String fromJwt = registry.appOfClient(JwtPeek.clientClaim(r));                    // azp / client_id / appid; only for service-account tokens
  if (fromJwt != null)                           return Caller.of(fromJwt, "jwt-client", declared);
  if (notBlank(declared))                        return Caller.of(declared, "source-app-header", null);
  boolean viaGateway = r.getHeader("{{CHANNEL_HEADER}}") != null || r.getHeader("{{GATEWAY_CALLER_HEADER}}") != null;
  return Caller.of(viaGateway ? "external" : "unknown", "none", null);
}
```
Rules:
* **Internal call** → the actual caller service name. **External call** (customer, partner, browser) → `external`; nothing known → `unknown`. Never invent, never fall back to a hostname or an IP (`client.address` is the agent's and stays what it is).
* **One naming table, one name per service everywhere**: a service's `service.name` (resource attribute), the value its callers register as `caller.service`, and its entry in every `peer.service` mapping are the same string. The registry (`apikeys.<name>`, `jwt-clients`, `service-accounts`) and the mesh/gateway mapping resolve **to** that name.
* Values are low-cardinality (one per service in the estate); `{ kind = server } | by(span.caller.service)` is the "who calls me" panel, `{ span.caller.source = "source-app-header" }` shows which callers are only self-declared, `{ span.caller.declared != nil }` shows misconfigured clients (declared ≠ verified).
* Node BFFs and servers apply the same resolver in the door middleware (07 N2); consumers do not need it (the producer is the caller — `messaging.*` + `event_source`, 05 K2).
* The analysis (§2 step 1) must say, per service, which of the six sources exist **today** (file:line) and which registry is missing; a service that reads none of them registers `caller.service=unknown` for all traffic until 04 §5 is applied on its callers — that value is the finding, not a bug.

---

## 4. R-AUTH — register the authentication and authorisation chain

The chain is a sequence of decisions; each is registered **where it is made**, on the current span (the SERVER span — filters, interceptors and aspects run on the request thread before the controller; in WebFlux use the Reactor context's span or `Span.current()` inside the operator).

### 4.1 Authentication (who is calling)

```java
// where the token/header is validated (the JWT aspect, the library's context resolver, after the library filter)
span.setAttribute("authn.source", "jwt");             // library | jwt | apikey | identity-headers | service-account | none
span.setAttribute("authn.result", "ok");              // written where validation CONCLUDES (door default is not_evaluated); on failure one of:
//   missing | expired | invalid | rejected_actor (user/service-account token with actor headers) | not_internal (impersonator outside the corporate domain) | forwarded (this layer only forwards the token — Node BFFs)
span.setAttribute("authn.token_type", jwt.isServiceAccount() ? "service-account" : "user");   // never the token
span.setAttribute(AttributeKey.stringArrayKey("user.roles"), jwtContext.getRoles());   // string[] of role names, low cardinality (RequestSpan.set has no array overload — use the key)
```
* A 401 produced *inside* the service is a traced span with `authn.result != ok` + `error_code` (03). When the 401/403 is produced by Spring Security (no `@ControllerAdvice` runs), register it in the `AuthenticationEntryPoint` / `AccessDeniedHandler` (or the WebFlux `ServerAuthenticationEntryPoint`) with the same `SpanOutcome.record` call (03 §2); when it is produced by an in-house library filter, the only place is the library itself — until it is changed, the door filter in §3 guarantees at least the correlation id and the raw identity are on that span. 401s produced by the API gateway / edge proxy never reach the service and are not spans.

### 4.2 Authorisation decisions (may the caller do this)

One attribute set per decision engine, registered in the handler that reads the answer:

```java
// policy engine (OPA-style) — policy enforcer / result handler (reference A), policy client (reference D)
span.setAttribute("authz.provider", "policy-engine");
span.setAttribute("authz.policy", policy.key());                       // "billing/get" — the policy package path, not the URL
span.setAttribute("authz.decision", allowed ? "allow" : "deny");       // "error" when the engine is unreachable / input empty
span.setAttribute("authz.reason", allowed ? "-" : "NOT_ENOUGH_PERMISSIONS");   // catalogue key of the code the deny maps to
span.setAttribute("authz.input.actor", input.actorUser != null);      // booleans/counts about the input, never the input
span.setAttribute("authz.input.permissions_count", input.permissions.size());

// identity service permission check — permission aspect → identity client isAuthorized (reference C)
span.setAttribute("authz.provider", "identity");
span.setAttribute("authz.policy", permissionCheck.action() + ":" + permissionCheck.resourceType());   // "VIEW:BILLING_ACCOUNT"
span.setAttribute("authz.decision", response.isSuccess() ? "allow" : "deny");

// policy sidecar — sidecar aspect → sidecar client authorize (reference C)
span.setAttribute("authz.provider", "policy-sidecar");
span.setAttribute("authz.policy", actionId);                           // "action:use/getBillingAccount"
span.setAttribute("authz.decision", authorized ? "allow" : (failed ? "error" : "deny"));   // fail-closed: error is also a deny
span.setAttribute("authz.reason", result.getReason());                // only if low-cardinality; otherwise a code
span.setAttribute("authz.accounts_checked", perAccount.size());

// permission API role check — authorizer of the identity system (reference B)
span.setAttribute("authz.provider", "permission-api");  span.setAttribute("authz.policy", "MASTER_ADMIN");  span.setAttribute("authz.decision", isMasterAdmin ? "allow" : "deny");
span.setAttribute("authz.reseller_shortcut", resellerShortcutApplied);  // an override that bypasses the check for a category of accounts

// DB ownership / in-memory checks — account-ownership validator, requested ⊆ permitted, record-id validation
span.setAttribute("authz.provider", "db");  span.setAttribute("authz.policy", "account_ownership");  span.setAttribute("authz.decision", owns ? "allow" : "deny");  span.setAttribute("authz.reason", owns ? "-" : "ACCOUNT_NOT_BELONGS_TO_USER");

// role gate — role aspect / @RoleAllowed / hasRole
span.setAttribute("authz.provider", "role");  span.setAttribute("authz.policy", String.join(",", roleAllowed.value()));  span.setAttribute("authz.decision", ok ? "allow" : "deny");
```

* **Key rule when engines can vary per request** (reference C: the identity check only when the hierarchy flag is on, the sidecar only when enabled, then ownership): every engine writes its own set `authz.<provider>.decision|policy|reason` (`authz.policy_engine.decision`, `authz.identity.decision`, `authz.policy_sidecar.decision`, `authz.db.decision`, `authz.role.decision`); the **plain** keys `authz.decision` / `authz.reason` are the **overall** outcome (`deny` if any engine denied, `error` if any failed closed, else `allow`) and `authz.decided_by` names the engine that produced a deny/error. A service with exactly one engine may write only the plain keys plus `authz.provider`. The snippets above show the plain form for brevity; apply this rule literally.
* `authz.reason` is the **catalogue key** of the code the deny maps to (`NOT_ENOUGH_PERMISSIONS`, `ACCOUNT_NOT_BELONGS_TO_USER`, `UNAUTHORIZED_RESOURCE_ACCESS`, `ACT_ON_BEHALF_DENIED`) — never a free-text reason; `error_code` (03) carries exactly what the response body carries (if a service today derives the code from the HTTP status instead of the catalogue, register it as is — the mismatch is what the attribute reveals; 02 §7.1).
* Every deny/error is also an outcome (03): `error_code`, `error.type` = the exception class or the code; status `ERROR` only for 5xx-class failures (provider unreachable), **not** for a 403 — a deny is a correct answer.
* The authorisation call itself is a CLIENT span (agent) under the SERVER span with `peer.service=policy-engine|identity|policy-sidecar|permission-api` (04 §3) — its duration is the answer to "is the policy engine slow"; the decision is on the parent.
* Never register the policy input document, the token, resource names that embed user data, or the `permissions[]` list. Counts and booleans only.

### 4.2a Standard Spring Security (projects without in-house aspects)

The four reference chains are examples; **run §2 on every project**. Where a project uses plain Spring Security instead of an in-house library/aspects — detectors: `SecurityFilterChain`, `oauth2ResourceServer()`, `JwtDecoder`/`ReactiveJwtDecoder`, `@EnableMethodSecurity`, `@PreAuthorize`/`@PostAuthorize`/`@PostFilter`, `@Secured`, `@RolesAllowed`, `hasRole`/`hasAuthority`, `AuthorizationManager`/`ReactiveAuthorizationManager`, gRPC `ServerInterceptor` — register with the framework's own hooks, once per service:
```java
@Component class AuthTracing {
  @EventListener void ok(AuthenticationSuccessEvent e)            { Span s = RequestSpan.root(); s.setAttribute("authn.source", "jwt"); s.setAttribute("authn.result", "ok"); s.setAttribute(AttributeKey.stringArrayKey("user.roles"), roles(e.getAuthentication())); }
  @EventListener void ko(AbstractAuthenticationFailureEvent e)    { Span s = RequestSpan.root(); s.setAttribute("authn.result", kind(e.getException())); }   // missing|expired|invalid
  @EventListener void denied(AuthorizationDeniedEvent<?> e)       { Span s = RequestSpan.root(); s.setAttribute("authz.provider", "spring-security"); s.setAttribute("authz.policy", String.valueOf(e.getAuthorizationResult())); s.setAttribute("authz.decision", "deny"); }
  @Bean AuthorizationEventPublisher publisher(ApplicationEventPublisher p) { return new SpringAuthorizationEventPublisher(p); }   // required for AuthorizationDeniedEvent
}
// AuthenticationEntryPoint / AccessDeniedHandler (MVC) or ServerAuthenticationEntryPoint / ServerAccessDeniedHandler (WebFlux): SpanOutcome.record("{{ERR_PREFIX}}-<module>005", "NOT_ENOUGH_PERMISSIONS", 401|403, null) — the ControllerAdvice never sees these
```
Method-security expressions (`@PreAuthorize("hasRole('ADMIN')")`) register `authz.policy` = the expression text (low cardinality, it is source code) through the `AuthorizationDeniedEvent`; a successful check registers nothing extra beyond `user.roles`.

### 4.3 Grants — what the user was given for this request

```java
// user-context interceptor/service (reference C), contact + permitted-accounts services (references A, B)
span.setAttribute("grants.context_source", fromCache ? "cache" : "identity");   // cache hit or identity-service profile call
span.setAttribute("grants.accounts_source", "permissions_view");                // permissions_view | org_accounts(admin API) | accounts_by_org(JPA) | hierarchy
span.setAttribute("grants.accounts_count", permitted.size());                   // 0 is the interesting value
span.setAttribute("grants.assets_count", assets.size());
span.setAttribute("grants.hierarchy_enabled", userContext.isHierarchyEnabled());
span.setAttribute("grants.internal_user", contact.isInternalUser());
span.setAttribute("grants.accounts_fallback", true);                             // when "lookup failed → defaulting to no permitted accounts"
```

### 4.4 Impersonation, support-org mode, channel

```java
// actor context (reference C), isSupportOrgMode (reference A), channel == internal portal (reference B), internal-org guard
span.setAttribute("impersonation", "none");     // default at the door; overwrite: user ({{ACTOR_USER_HEADER}}) | org ({{ACTOR_ORG_HEADER}} only)
span.setAttribute("support_org_mode", supportOrgMode);
span.setAttribute("internal_guard", "ok");      // ok | missing_actor (401 for the internal org without actor headers)
span.setAttribute("effective.org_id", effectiveOrgId);                          // the org the query actually used (may differ from org_id)
span.setAttribute("effective.user_name", mask(effectiveUser));                  // actor when impersonating
```

### 4.5 Read-only mode and other request-level gates

```java
// read-only-mode interceptor (feature flag), environment interceptor (env header validation)
span.setAttribute("feature_flag.key", "applicationReadOnlyMode");
span.setAttribute("feature_flag.result.variant", readOnly ? "on" : "off");
span.setAttribute("feature_flag.provider.name", "<your flag provider>");
span.setAttribute("gate.readonly", readOnly && isWrite ? "blocked" : "pass");
span.setAttribute("env", envHeader);
```

---

## 5. R-DECIDE — register every decision on the path (controller and service)

The controller adds the **request's business identifiers** before calling the service; the service adds **decisions** where they are made. No new spans for that.

```java
@PostMapping("/accounts/billing/search")
@Policy(key = "billing/get")
public ResponseEntity<BillingAccountSearchResponse> search(@RequestBody BillingAccountSearchRequest req, RequestContext ctx) {
  Span span = Span.current();
  span.setAttribute("customer_account", req.getCustomerAccountNumber());     // as soon as known, before any call
  span.setAttribute("page_size", req.getLimit());  span.setAttribute("page_offset", req.getOffset());
  span.setAttribute("search_scope", req.getSearchScope());      // enum
  return ResponseEntity.ok(service.searchBillingAccounts(ctx, req).join());
}

@WithSpan("AccountService.searchBillingAccounts")      // only because it fans out to two upstreams + DB
public CompletableFuture<BillingAccountSearchResponse> searchBillingAccounts(RequestContext ctx, BillingAccountSearchRequest req) {
  RequestSpan.set("account.filter", "permitted");             // changes the response → ROOT span (default; overwritten below)
  if (commonService.isSupportOrgMode(ctx)) RequestSpan.set("account.filter", "org");
  RequestSpan.here("cache_hit", "false");                      // local to this unit of work → the @WithSpan span (default before the lookup)
  ...
  RequestSpan.set("billing_hub.version", "v3");                // which upstream contract was chosen → root
  RequestSpan.here("billing_hub.batches", String.valueOf(batches.size()));   // fan-out shape → this span
  RequestSpan.set("result_count", result.size());              // root
  RequestSpan.set("no_record", result.isEmpty());              // root — "empty 200" advices make this invisible otherwise
  return ...;
}
```
**Root or here?** One test: *does it change the response, the access decision or the outcome the client sees?* → `RequestSpan.set` (root). Otherwise (timing shape, cache hit of a helper, batch sizes) → `RequestSpan.here` (the innermost span). The TraceQL in §9 and 03 §6 query these keys on `kind = server`.

Decision catalogue — register these whenever they exist (names in §7):

| Decision type | Attribute(s) | Typical places |
|---|---|---|
| Feature flag evaluated | `flag.<key>=<variant>` for **every** flag evaluated on the request (one key per flag — a read-only-mode flag is often evaluated on every request, so most requests see more than one flag); additionally the standard `feature_flag.key` / `feature_flag.result.variant` / `feature_flag.provider.name` for the single flag that decided the code path of *this* span, if there is one | flag-provider gates (read-only mode, listener enable flags, migration toggles); `@ConditionalOnProperty` toggles are startup facts — one INFO line at startup, nothing per request |
| Cache hit / miss | `cache_hit` (+ `cache.name`) | Redis user-context cache, OAuth token cache, in-memory principal cache, a Node help-topics cache fallback |
| Fallback taken | `fallback=<what>` (`no_permitted_accounts`, `empty_contact`, `default_project_null`, `help_topics_cache`) | "lookup failed → proceed without filter" paths, a timeout → empty object, a hierarchy lookup → null |
| Filter / skip | `skipped=true`, `skip_reason=<enum>` (request decisions); in a listener `<system>.kafka.<subject>.skip_reason` (05, 00 §6) | listener filters (05), no-record → empty 200 advices |
| Version / route chosen | `<dep>.version` (`v1\|v2\|v3`), `strategy` | upstream v1/v2/v3 contracts, search-by-company vs search-by-account |
| Fan-out shape | `<dep>.batches`, `batch_size`, `parallelism` | enrichment in batches of N, `CompletableFuture` joins |
| Result | `result_count`, `no_record`, `truncated` (hard caps) | every service method returning a list/page |
| Validation | `validation.failed=true`, `validation.field_count` | custom validators, `MethodArgumentNotValidException` handler |
| Retry | its own per-purpose span `<dependency> <purpose> retry` with `retry.count`, `retry.max`, `retry.outcome` (04 §4.4) — a count, never a span or event per attempt, never an error code | `@Retryable`/`@Recover`, policy-engine retry ×3 |
| Non-fatal error | `<dependency>.<action>.failure` = short reason, one key per failed call (`SpanOutcome.nonFatal`), + `failure_count` + the `<dep>.outcome` facet — **no** `error_code` (03 §2.1) | catch-all around a notification, `onErrorReturn`, `.exceptionally` |
| Timeout policy | `timeout_ms` when it is a per-call decision | `CompletableFuture.get(20, SECONDS)`, a long read timeout for an LLM proxy |

Rules:
* **Defaults first.** Write the default value at the top of the method, overwrite on the branch. Every span then has the key and TraceQL `= false` works.
* **Counts, not lists; enums, not text.** `grants.accounts_count=0` not the list; `skip_reason=EMPTY_ACCOUNT_NUMBER` not the log sentence.
* **Never in a loop.** For a fan-out over N items register `N`, the number failed, and (only if there is exactly one that matters) its id.
* **`@WithSpan` only** on a public use-case method that fans out to ≥2 downstreams, runs async, or is a known hotspot; name = `Class.method`; never on getters, mappers, validators, DAOs (the agent's JDBC spans already exist).

---

## 6. R-ROOT helper — pin decisions to the request span from anywhere

A decision made inside a `@WithSpan` method, a Reactor operator or a worker thread must still be searchable on the **root** span. Add one small helper per service (no framework, no new dependency):

```java
// tracing/RequestSpan.java  ➕
public final class RequestSpan {
  private static final ContextKey<Span> ROOT = ContextKey.named("request-root-span");
  /** call in the door filter: makes the SERVER span reachable from every child scope on this request */
  public static Scope open(Span root) { return Context.current().with(ROOT, root).with(WRITTEN, ConcurrentHashMap.newKeySet()).with(FAILURES, new AtomicInteger()).makeCurrent(); }
  public static Span root() { Span s = Context.current().get(ROOT); return s != null ? s : Span.current(); }
  public static void set(String key, String v) { if (v != null) root().setAttribute(key, v); }
  public static void set(String key, long v)   { root().setAttribute(key, v); }
  public static void set(String key, boolean v){ root().setAttribute(key, v); }
  public static void here(String key, String v){ Span.current().setAttribute(key, v); }   // on the innermost span
  /** first writer wins (the OTel API cannot read attributes back): a per-request Set<String> of written keys travels in the same Context */
  public static void setIfAbsent(String key, Object v) { Set<String> w = Context.current().get(WRITTEN); if (w == null || w.add(key)) setAny(root(), key, v); }
  /** 03 §2.1: a call the request carried on past. Key = <call>.failure with <call> = "<dependency>.<action>" (e.g. "support_case.create"); value = short reason.
   *  First failure of a call wins; failure_count = number of calls that failed on this request (a query cannot match attribute names by pattern). */
  public static void failure(String call, String reason) {
    String key = AttrName.of(call + ".failure");                                                // company prefix added only when the dependency name is an OpenTelemetry namespace (00 §6)
    Set<String> w = Context.current().get(WRITTEN); AtomicInteger n = Context.current().get(FAILURES);
    if (w == null || n == null || !w.add(key)) return;                                         // outside a request/job scope, or this call already recorded — the log line (SpanOutcome) is the record
    root().setAttribute(key, reason);                                                            // "HTTP 503" | "timeout" | "connection refused" | "retries exhausted (3)" | exception simple class name
    root().setAttribute("failure_count", n.incrementAndGet());
  }
  public static final String KEY = "request-root-span";                                        // WebFlux: Reactor context key (§3)
  private static final ContextKey<Set<String>> WRITTEN = ContextKey.named("request-written-keys");
  private static final ContextKey<AtomicInteger> FAILURES = ContextKey.named("request-failure-count");
}
```
```java
// tracing/AttrName.java ➕ — the one place that decides whether a key needs the company prefix (00 §6)
public final class AttrName {
  private static final Set<String> OTEL_NAMESPACES = Set.of("android", "app", "artifact", "aspnetcore", "aws", "azure", "browser", "cassandra", "cicd", "client", "cloud", "cloudevents", "cloudfoundry", "code", "container", "cpu", "cpython", "db", "deployment", "destination", "device", "disk", "dns", "dotnet", "elasticsearch", "enduser", "error", "event", "exception", "faas", "feature_flag", "file", "gcp", "gen_ai", "geo", "go", "graphql", "heroku", "host", "http", "hw", "ios", "jsonrpc", "jvm", "k8s", "linux", "log", "mainframe", "mcp", "messaging", "network", "nfs", "nodejs", "oci", "onc_rpc", "openai", "openshift", "opentracing", "oracle_cloud", "oracledb", "os", "otel", "peer", "pprof", "process", "profile", "rpc", "security_rule", "server", "service", "session", "signalr", "source", "system", "telemetry", "test", "thread", "tls", "url", "user", "user_agent", "v8js", "vcs", "webengine", "zos");   // 00 §6 list; refresh from the registry
  /** "support_case.create.failure" → unchanged; "db.write.failure" → "{{PREFIX}}.db.write.failure" */
  public static String of(String key) {
    key = key.toLowerCase(java.util.Locale.ROOT).replace('-', '_'); // OpenTelemetry key characters: a-z 0-9 _ . (00 §6.1)
    int dot = key.indexOf('.'); String first = dot < 0 ? key : key.substring(0, dot);
    return OTEL_NAMESPACES.contains(first) ? "{{PREFIX}}." + key : key;
  }
}
```
Use `AttrName.of(...)` for every key whose first group comes from a **name** (dependency, topic, job): `failure()` above already does; per-dependency keys do it at the call site (`RequestSpan.set(AttrName.of(dep + ".outcome"), "ignored")`). Fixed keys from §7 are written as they are — they never start with a namespace.

`open()` is called **once per unit of work**, by the boundary that owns it: the door filter (§3) around `chain.doFilter`, `JobRun` (06 J1) around the job body, and the consumer listener (05 K3) around `process(record)` — always in a try-with-resources; nothing else opens it. Outside such a scope `root()` falls back to `Span.current()` and `failure()` has nowhere to write (it returns; the log line in `SpanOutcome` remains the record). Arrays (`user.roles`) use `root().setAttribute(AttributeKey.stringArrayKey(...), list)`.
* The agent propagates `Context` through executors, `CompletableFuture`, WebClient/Reactor and Kafka consumers, so `RequestSpan.root()` works on worker threads and inside operators (⚠ verify once in a non-production environment with the agent on: a `RequestSpan.set` from an `@Async` method appears on the SERVER span).
* Authorisation, grants, impersonation and outcome attributes go on the **root** (`RequestSpan.set`); decisions local to a fan-out unit go on the **current** span (`RequestSpan.here`) — and, if they change the response, also on the root.
* **MDC is not `Context`.** Use the MDC-aware executors that usually already exist (an MDC-copying `ThreadPoolTaskExecutor`, a `TaskDecorator`, a Reactor MDC hook) for any traced work off the request thread; never `CompletableFuture.runAsync(task)` on the common pool (a typical defect: an async e-mail dispatch whose log lines have no correlation id).

---

## 7. Vocabulary (normative — do not invent parallel names)

**Standard (set by the agent or by the rules above; keep these names exactly):** `http.request.method`, `http.route`, `http.response.status_code`, `url.path`, `server.address`, `client.address`, `user_agent.original`, `http.request.header.<name>` (captured headers), `db.system`, `db.statement`, `db.operation`, `messaging.*` (05), `peer.service`, `error.type`, `exception.type|message|stacktrace`, `user.name`, `user.id`, `user.roles`, `session.id`, `feature_flag.key`, `feature_flag.result.variant`, `feature_flag.provider.name`, `code.function.name`, `thread.name`.

**Our own names (no prefix — 00 §6: meaningful, dot-separated groups, snake_case words, never starting with an OpenTelemetry namespace; every fixed key below was checked against the registry; keys that start with a dependency/topic/job name go through `AttrName.of` (§6) and get the company prefix only when that name is a namespace; string/number/bool):**

| Group | Keys |
|---|---|
| Door | `correlation_id`, `correlation_id_generated`, `caller.service` (the calling service's name — `external`/`unknown` when none), `caller.source` (`mesh-cert\|gateway-header\|api-key\|jwt-client\|source-app-header\|none`), `caller.declared` (only when it differs), `org_id`, `channel_type`, `env`, `actor.present`, `actor.user_name` (masked), `actor.org_id` |
| Authn | `authn.source`, `authn.result`, `authn.token_type` |
| Authz | `authz.provider`, `authz.policy`, `authz.decision` (overall), `authz.reason` (catalogue key), `authz.decided_by`, `authz.<provider>.decision\|policy\|reason` (per engine: policy-engine, identity, policy-sidecar, permission-api, db, role, spring-security), `authz.input.*` (booleans/counts), `authz.accounts_checked`, `authz.reseller_shortcut` |
| Grants | `grants.context_source`, `grants.accounts_source`, `grants.accounts_count`, `grants.assets_count`, `grants.hierarchy_enabled`, `grants.internal_user`, `grants.accounts_fallback` |
| Impersonation | `impersonation` (`none\|user\|org`), `support_org_mode`, `internal_guard`, `effective.org_id`, `effective.user_name` (masked) |
| Business ids | `customer_account`, `billing_account`, `account_number`, `invoice_number`, `order_number`, `document_id`, `site_id`, `project_id`, `reference_id`, `agreement_number`, `ticket_id`, `master_data_id`, `audit_id` (row written by a consumer/job) — extend with your own opaque ids, never names |
| Decisions | `cache_hit`, `cache.name`, `fallback`, `skipped`, `skip_reason`, `<dep>.version`, `strategy`, `account.filter`, `gate.readonly`, `flag.<key>`, `validation.failed`, `validation.field_count`, `retry.count`, `retry.max`, `retry.outcome` (`succeeded\|exhausted\|aborted`, on the per-purpose retry span — 04 §4.4), `timeout_ms` |
| Shape / result | `page_size`, `page_offset`, `sort`, `search_scope`, `batch_size`, `<dep>.batches`, `parallelism`, `result_count`, `no_record`, `truncated` |
| Outcome (03) | `error_code` (exactly the response body's code — **hard failures only**, 03 §2.1), `error_key`, `<dependency>.<action>.failure` (one key per call the request carried on past; value = short reason `HTTP 503\|timeout\|connection refused\|retries exhausted (n)\|<ExceptionClass>`), `failure_count` (how many such keys), `upstream.name`, `upstream.status`, `upstream.code` (first failing dependency wins), `handled` (`mapped\|passthrough\|ignored\|fallback\|treated_as_success\|fail_closed\|rethrown`) |
| Handover (04) | keys named after the call (00 §6): `<dep>.<action>.` + `version`, `id`, `ids_count`, `flags`, `page_size`, `<flag name>`, `response.id\|count\|status\|code\|correlation_id\|request_id` (e.g. `pbas.company_account_search.page_size`); `<dep>.outcome` (per dependency, on the root); `call.purpose` only on a span that exists for one call (04 §4.4 retry span, a `@WithSpan` per call); auth step (common to every service): `auth.provider`, `auth.grant`, `auth.token_source`, `auth.token_ttl_s`, `auth.result`, `auth.on_failure` |
| Messaging (05) | keys named after the message (00 §6): `<system>.kafka.<subject>.` or `<system>.rabbitmq.<subject>.` + `message.id`, `conversation_id`, `event_type`, `event_status`, `event_source`, `route` (handler chosen by event type), `consumer.action`, `delivery_attempt` (redeliveries of this record before this one), `skip_reason`, `publish.result`, `publish.partition`, `publish.offset` (e.g. `dih.kafka.userdetails.message.id`, `fabric.kafka.account.publish.result`); `correlation_id_generated` (common) |
| Jobs (06) | `job.name`, `job.run_id`, `job.trigger` (`scheduled\|api\|message\|cronjob\|manual`), `job.status` (`running\|success\|failed\|partial\|skipped_lock`), `job.batch_status` (raw Spring Batch status), `job.exit_code`, `job.lock`, `job.params` (names only), `job.execution_id`, `job.instance_id`, `job.step`, `job.step.execution_id\|status\|read\|write\|skip\|rollback`, `job.items_total\|processed\|failed\|skipped`, `check.<system>` (per-system comparison result of a monitoring run) |
| UI (07) | `screen`, `mfe`, `endpoint`, `error_type`, `step` (phase of a long flow), `proxy.target` (BFF) |

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
{ resource.service.name = "<service name of the deployment>" && span.correlation_id = "<uuid>" }   -- the request and all its hops
{ kind = server && span.authz.decision = "deny" } | by(span.authz.provider, span.authz.policy, span.authz.reason)
{ kind = server && span.authz.decision = "error" }                                                  -- policy engine failures (fail-closed denies)
{ kind = server && span.grants.accounts_count = 0 && span.http.response.status_code = 200 }           -- silent empty answers
{ kind = server && span.impersonation != "none" } | by(span.channel_type, span.http.route)
{ kind = server && span.feature_flag.key = "applicationReadOnlyMode" && span.gate.readonly = "blocked" }
{ kind = server && span.fallback != nil } | by(span.fallback)
{ kind = server && span.correlation_id_generated = true } | by(span.http.route)                     -- clients that send no correlation id
{ span.http.route =~ ".*health.*" }                                                                             -- must be empty
```

- [ ] §2 analysis table produced (file:line per decision) before any code
- [ ] Every attribute key the code writes is a registry attribute, a §7 key, or a new plain name that passes the 00 §6 check; keys built from a dependency/topic/job name go through `AttrName.of` (company prefix only when that name is an OpenTelemetry namespace)
- [ ] Door filter: correlation id read-or-generated once, MDC (existing key + `correlationId`), `correlation_id`, `caller.service` + `caller.source` resolved per §3.1 (sources present today listed with file:line), identity/actor/channel/session attributes, response header echoed; no tokens
- [ ] Authn result registered where validation happens; each authz engine registered where its answer is read (`provider/policy/decision/reason`), overall decision on the root; grants as counts + source; impersonation/support-org/channel switches; read-only/flag gates
- [ ] Controller: business ids before the service call; service: defaults first, decisions/fallbacks/skips/versions/result counts; `@WithSpan` only on fan-out/async/hotspots
- [ ] `RequestSpan` helper in place; async work on MDC-aware executors; no common-pool `runAsync`
- [ ] Outcome per 03 (`error.type`, `error_code`, upstream status/code), handover per 04, messages per 05
- [ ] Vocabulary §7 only; no PII/secrets/lists; `db-statement-sanitizer` on
- [ ] Health routes dropped (collector filter covers this service's management path); propagators unified
- [ ] Verified with the TraceQL above on one real request in an agent-enabled environment; log line ↔ trace by `trace_id`, log ↔ span ↔ error body by correlation id
