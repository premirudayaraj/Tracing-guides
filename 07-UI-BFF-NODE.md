# 07 — Browser (React MFEs / apps), Node BFFs and Node API servers

> **Roles:** WEB-UI (React micro-frontends and standalone apps), BFF / NODE-SERVER (Koa, Express, Fastify — proxies and API servers), NODE CRONJOB (see 06 §J5). The same door/decision/handover rules as the Java guides, applied with the OpenTelemetry **web SDK** in the browser and the **Node SDK** on the server. Placeholders (`{{PREFIX}}`, `{{CORRELATION_HEADER}}`, `{{USER_HEADER}}`, `{{ORG_HEADER}}`, `{{CHANNEL_HEADER}}`, `{{SESSION_HEADER}}`, `{{ACTOR_USER_HEADER}}`, `{{ACTOR_ORG_HEADER}}`, `{{TRACE_BACKEND}}`…) are defined in `README.md §0`.
>
> **How to use with an AI assistant:** *"Trace `<mfe|bff>` per 07-UI-BFF-NODE.md: §2 inventory first, then U1–U7 / N1–N6."*
>
> Legend: **MUST** = rule; **already** = usually in current code; ➕ = add; ⚠ = confirm at runtime.

---

## 0. Reference patterns (what this is generalised from)

| Tier | Typical occurrence | What existed before tracing |
|---|---|---|
| React MFEs | A dozen micro-frontends whose HTTP calls go through **one shared fetch layer** published as a private package (a `fetchInstance` / `FetchInstanceFactory` wrapper); a `getHeadersForInternalPortal()` utility adds `{{ORG_HEADER}}`-style actor headers, a masked actor user name and `{{CHANNEL_HEADER}}: <internal-portal>` when `window.IS_INTERNAL_PORTAL` | no OTel/APM; no `traceparent`/`{{CORRELATION_HEADER}}` sent; `correlationId` only *received* in error bodies; the private fetch package is where interceptors live |
| The browser reference implementation | One standalone React app with `src/utils/OpenTelemetry.js`: `WebTracerProvider` (resource `service.name`, `service.version`, session id from a cookie), `OTLPTraceExporter` (HTTP) to a collector URL given at init, `BatchSpanProcessor`, `ZoneContextManager`, `CompositePropagator(W3CBaggage, W3CTraceContext)`, `getWebAutoInstrumentations` (`xml-http-request` `clearTimingResources`, `document-load`, `user-interaction` on `submit`), a `CustomSpanProcessor` adding masked `user.name`, `user.email`, `user.status`, `user.error`, `selected.user`; a RUM/EUM script in `index.html` | **the pattern to copy**; `propagateTraceHeaderCorsUrls` not set ⚠ |
| Koa BFF | A portal BFF: APM agent profiled at startup (one tier per portal), a `serviceManager.prepareApiConfig` that proxies **all** inbound headers upstream and echoes `{{CORRELATION_HEADER}}` back as `correlation-id` in success and error JSON; loggers `bunyan` + `winston`; an unused `opentracing` dependency | APM only; the BFF hop invisible in `{{TRACE_BACKEND}}`; no correlation id generated when the browser sends none |
| Express API server | A case-management API: `injectResponseHeaders` → `express.json` → `cookieParser` → `/management` → router with `injectHeaders` (rewrites `Authorization`, `{{USER_HEADER}}`, `{{ORG_HEADER}}`, `{{APIKEY_HEADER}}` from a vault), `setIncomingReqMeta`, sub-routers, `http-proxy-middleware` for `/case` → a support-case system, `handleErrorMiddleware`; correlation id read from a product-specific header or generated (15-char hex), stored as `{{CORRELATION_HEADER}}` in `req.headers`, spread onto every axios call; `pino` logger fields `correlationId, user, sessionId, label(…), method, requestedAt, responseTime, status, error, success, target, reason`; axios calls to users/organisations/parse endpoints, a source-control API for help topics (Bearer, cache fallback), a chat webhook for error notification (silently caught); no JWT validation in this layer | structured logs with correlation id; no OTel/APM |
| Other Node servers | proxies, alliance/partner APIs, MCP servers, console servers | per §2 inventory |
| Backend contract | `{{CORRELATION_HEADER}}` accepted/echoed by every Java service (01 §3); error body `ErrorDetails[]` with real HTTP status (02); the gateway/edge proxy CORS allow-list for `traceparent`/`tracestate`/`{{CORRELATION_HEADER}}` ⚠ not verified | |

---

## 1. The model

```
 Browser (OTel web SDK, service.name=<portal>)                    Node BFF (OTel Node SDK, service.name=<bff>)             Java service (agent)
 user-interaction span "click submit"  {{PREFIX}}.screen=/billing/invoices/:id, {{PREFIX}}.mfe=billing
   └─ fetch span "POST /api/billing/…"  {{PREFIX}}.endpoint=<template>, {{PREFIX}}.correlation_id=<C>  ──traceparent + {{CORRELATION_HEADER}}──►  SERVER span "POST /api/billing/…"
        attrs after response: http.response.status_code, {{PREFIX}}.error_code (from ErrorDetails[0]), {{PREFIX}}.correlation_id (echoed)   {{PREFIX}}.correlation_id=<C>, user.name, {{PREFIX}}.proxy.target=billing
                                                                                                                                    └─ CLIENT span → gateway → Java SERVER span (01)
```

Five invariants (same as the backend, restated for the web tier):
1. **One SDK per page** (the shell or the standalone app); MFEs use `@opentelemetry/api` only and are no-ops when no provider is registered.
2. **The door is the user action / the inbound request**: route template + MFE name in the browser; method + route template in the BFF. Never concrete URLs with ids as span names.
3. **The correlation id is generated in the browser** per API call (or by the BFF when absent) and sent as `{{CORRELATION_HEADER}}`; `traceparent` goes with it once CORS allows it; the same id appears in the browser span, the BFF span, the Java span, the log lines and the error toast.
4. **Backend outcomes are lifted onto the fetch span**: HTTP status + `errorCode` + `correlationId` from `ErrorDetails[]`; 5xx/transport → ERROR, 4xx → attribute only.
5. **Masked identity only** (`user.name` first 3 + `x…` + last 3, the masking the UIs already apply); no tokens, cookies, e-mails, free text.

---

## 2. The analysis you run before writing code (mandatory)

**Browser app / MFE:** where the app boots (shell `index.tsx` / `startOtelInstrumentation` caller); the **central HTTP layer** (the shared fetch wrapper, `FetchInstanceFactory`, axios instance) and every header it sets; the router (route templates); the top-level `ErrorBoundary`; the business actions (thunks/sagas/hooks) that call APIs; existing RUM and its config; what the app knows about the user (masked name, org id, channel, session cookie).

**Node server:** the entry (`server/app.js`, `app/index.js`), the middleware order (first middleware = door), authentication in this layer (JWT validated? forwarded only?), correlation id handling (which inbound header, generated how, which outbound header), every outbound call (axios/fetch/proxy middleware — 04 §2 inventory: auth, request fields, response fields, failure handling), logging library and fields, existing APM agent, process lifecycle (long-running server vs CronJob).

---

## 3. Browser rules (U)

### U1 — Bootstrap: one provider, resource attributes, masked user
```js
// shell (or standalone app) only — copy the reference OpenTelemetry.js bootstrap
startOtelInstrumentation(otelCollectorUrl /* browser-facing collector for the env ⚠ confirm where it comes from */, { serviceName: '<portal>', serviceVersion: BUILD, env: window.APP_ENV });
// resource: service.name, service.version, deployment.environment, session.id (from the session cookie — same value as {{SESSION_HEADER}}), browser.* (SDK)
// CustomSpanProcessor: user.name (masked), {{PREFIX}}.org_id, {{PREFIX}}.channel_type (internal portal marker), {{PREFIX}}.actor.org_id when set — on every span
registerInstrumentations({ instrumentations: [getWebAutoInstrumentations({
  '@opentelemetry/instrumentation-fetch':            { propagateTraceHeaderCorsUrls: [API_ORIGIN_RE], clearTimingResources: true },
  '@opentelemetry/instrumentation-xml-http-request': { propagateTraceHeaderCorsUrls: [API_ORIGIN_RE], clearTimingResources: true },
  '@opentelemetry/instrumentation-document-load': {},
  '@opentelemetry/instrumentation-user-interaction': { eventNames: ['click', 'submit'] },
})]});
```
* MFEs: `const tracer = trace.getTracer('<mfe-name>')` from `@opentelemetry/api` — no provider, no exporter, no `WebTracerProvider` inside an MFE.
* An existing RUM/EUM script stays where it exists (Core Web Vitals); OTel spans are for correlation with `{{TRACE_BACKEND}}`. Do not send the same custom events to both.

### U2 — Door: name the interaction by route template
```js
// router listener (shell/MFE root)
const span = tracer.startSpan('route ' + routeTemplate(location.pathname));   // "/billing/invoices/:invoiceId", never the concrete path
span.setAttribute('{{PREFIX}}.screen', routeTemplate); span.setAttribute('{{PREFIX}}.mfe', 'billing'); span.end();
// user-interaction spans (SDK) get {{PREFIX}}.screen / {{PREFIX}}.mfe from the CustomSpanProcessor (current route kept in a module variable)
```

### U3 — Central HTTP layer: correlation id out, outcome in (the most important placement)
Where: the one wrapper around the shared fetch package (`src/services/Network/index.ts`, `FetchInstanceFactory`) — never per component. If the shared package can be changed, put it there for all MFEs.
```js
// request (wrapper): the fetch span does not exist yet here, so only the header is set and the call context is remembered for the hook below
const correlationId = uuidv4();                                   // one per API call; the Java door keeps it (01 §3)
headers['{{CORRELATION_HEADER}}'] = correlationId;                // traceparent is added by the fetch/XHR instrumentation for origins in propagateTraceHeaderCorsUrls — never hand-craft it
callContext.set(requestKey, { correlationId, endpointTemplate, screen: currentRoute(), ids });   // WeakMap/Map keyed by the Request/URL+timestamp; read by the hook

// the fetch/XHR span is reachable ONLY through the instrumentation hooks — trace.getActiveSpan() in the wrapper returns the parent (and after `await`, the parent again)
'@opentelemetry/instrumentation-fetch': { propagateTraceHeaderCorsUrls: [API_ORIGIN_RE],
  applyCustomAttributesOnSpan: async (span, request, response) => {                              // called once per fetch with the fetch span
    const c = callContext.take(keyOf(request)) ?? {};
    span.setAttribute('{{PREFIX}}.correlation_id', c.correlationId); span.setAttribute('{{PREFIX}}.endpoint', c.endpointTemplate); span.setAttribute('{{PREFIX}}.screen', c.screen);
    c.ids?.forEach(([k, v]) => span.setAttribute(k, v));                                         // {{PREFIX}}.account_number / {{PREFIX}}.invoice_number / {{PREFIX}}.order_number — opaque ids only
    if (response instanceof Response && response.status >= 400) {
      const body = await safeJson(response.clone()); const first = Array.isArray(body) ? body[0] : body;   // ErrorDetails[] (02): errorCode, errorMessage, correlationId, additionalInfo
      span.setAttribute('{{PREFIX}}.error_code', first?.errorCode ?? '-'); span.setAttribute('{{PREFIX}}.correlation_id', first?.correlationId ?? c.correlationId); span.setAttribute('{{PREFIX}}.error_type', 'api');
      if (response.status >= 500) span.setStatus({ code: SpanStatusCode.ERROR, message: first?.errorCode });   // 4xx: attribute only (03)
    } else if (!(response instanceof Response)) { span.recordException(response); span.setAttribute('{{PREFIX}}.error_type', 'transport'); span.setStatus({ code: SpanStatusCode.ERROR }); }   // fetch rejected: CORS, timeout, network
  } },
'@opentelemetry/instrumentation-xml-http-request': { propagateTraceHeaderCorsUrls: [API_ORIGIN_RE], applyCustomAttributesOnSpan: (span, xhr) => { /* same, from xhr.status / xhr.responseText */ } },
// a BFF's JSON may add `correlation-id`; prefer header/body value when present, else the generated one
```
Show `correlationId` on every error toast — that is what support pastes into `{{TRACE_BACKEND}}`.

### U4 — Uncaught render errors
Top-level `ErrorBoundary.componentDidCatch` → `tracer.startSpan('render.error')` + `recordException(error)`, `{{PREFIX}}.error_type=render`, `{{PREFIX}}.mfe`, `{{PREFIX}}.screen`, first 250 chars of `componentStack`; end immediately.

### U5 — Business events and decisions in the UI
Where: the action/thunk that performs the business step. One short span (`billing.invoice.download`, `order.create`) with ids only (`{{PREFIX}}.invoice_number`, `{{PREFIX}}.order_number`, `{{PREFIX}}.account_number`) and UI decisions that change what is requested: `{{PREFIX}}.channel_type` (internal portal), `{{PREFIX}}.actor.org_id` (masquerade selection), feature toggles from the shell (`feature_flag.key/result.variant`), selected filters as enums (`{{PREFIX}}.search_scope`), page size. Never titles, free text, form contents.

### U6 — Long flows
Wrap upload → validate → submit in one parent span (`order.create.flow`) so the fetch spans group; end in `finally`; `{{PREFIX}}.step` attribute updated per phase.

### U7 — Cross-origin propagation (browser → BFF → Java)
1. `propagateTraceHeaderCorsUrls` must cover the API/BFF origin (verify whether the API is same-origin ⚠).
2. Gateway CORS must allow `traceparent, tracestate, {{CORRELATION_HEADER}}` (`Access-Control-Allow-Headers`) — check the edge proxy route config and the API-gateway CORS policy before relying on it.
3. Until 1–2 are done, `{{CORRELATION_HEADER}}` is the join (works today: every Java service logs it and, after 01 §3, puts it on the span).

---

## 4. Node rules (N) — BFF and API servers

### N1 — SDK bootstrap (first `require` in the entry file, before Koa/Express)
```js
// tracing.js — loaded via `node -r ./tracing.js server.js` or first line of startup.js (before any APM agent require if both run ⚠ order matters, verify once)
const { NodeSDK } = require('@opentelemetry/sdk-node'); const { getNodeAutoInstrumentations } = require('@opentelemetry/auto-instrumentations-node');
const sdk = new NodeSDK({ serviceName: process.env.OTEL_SERVICE_NAME || '<bff>',
  instrumentations: [getNodeAutoInstrumentations({ '@opentelemetry/instrumentation-http': { ignoreIncomingPaths: [/\/health/, /\/management/] } })] });  // OTLP exporter from OTEL_EXPORTER_OTLP_ENDPOINT
sdk.start(); process.on('SIGTERM', () => sdk.shutdown());
```
* Env: `OTEL_EXPORTER_OTLP_ENDPOINT={{COLLECTOR_ENDPOINT}}` (the same collector as the Java services), `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES=deployment.environment=<env>,<any extra resource keys your collector inserts for Java services, so one TraceQL works for both>`, `OTEL_PROPAGATORS=tracecontext,baggage,b3,b3multi`. No second APM SDK for traces; an existing APM agent may stay for its own dashboards.
* Health routes ignored at the SDK, not the collector.

### N2 — Door middleware (first in the chain)
```js
// the SERVER span is reachable in a middleware through the RPC metadata, not through getActiveSpan() (the Koa/Express instrumentation makes each middleware's own span active)
const { getRPCMetadata, RPCType } = require('@opentelemetry/core');
const serverSpan = () => { const m = getRPCMetadata(context.active()); return m?.type === RPCType.HTTP ? m.span : undefined; };   // alternatively: the http instrumentation's requestHook(span, req)
app.use(async (ctx, next) => {                                         // Koa; Express: (req,res,next)
  const span = serverSpan();                                           // the SDK's SERVER span "POST /api/billing/…" (http.route set by the router instrumentation)
  const inbound = ctx.get('{{CORRELATION_HEADER}}') || ctx.get('<product-specific legacy header>');
  const cid = inbound || randomUUID();                                 // generate ONCE here when the browser sent none
  ctx.state.correlationId = cid; ctx.set('{{CORRELATION_HEADER}}', cid);   // echo (a BFF that already echoes it inside the JSON as `correlation-id` keeps both)
  span?.setAttribute('{{PREFIX}}.correlation_id', cid); span?.setAttribute('{{PREFIX}}.correlation_id_generated', !inbound);
  span?.setAttribute('user.name', mask(ctx.get('{{USER_HEADER}}'))); span?.setAttribute('{{PREFIX}}.org_id', ctx.get('{{ORG_HEADER}}')); span?.setAttribute('{{PREFIX}}.channel_type', ctx.get('{{CHANNEL_HEADER}}') || '-');
  span?.setAttribute('{{PREFIX}}.actor.present', !!(ctx.get('{{ACTOR_USER_HEADER}}') || ctx.get('{{ACTOR_ORG_HEADER}}'))); span?.setAttribute('session.id', ctx.get('{{SESSION_HEADER}}'));
  await context.with(context.active().setValue(CID_KEY, cid), next);   // AsyncLocalStorage-backed: available to the logger and outbound interceptors
});
```
* Logger: a child logger per request with `correlationId`, `trace_id`, `span_id` (from `trace.getActiveSpan().spanContext()`) so `{{LOG_BACKEND}}` lines open in `{{TRACE_BACKEND}}`; a logger that already has `correlationId` gets the two ids added.
* Authentication in this layer (if any — a header-rewriting `injectHeaders` only rewrites, a `serviceManager` forwards): register `{{PREFIX}}.authn.source=forwarded` + `{{PREFIX}}.authn.result=forwarded` when the token is only passed on, and the real `{{PREFIX}}.authn.result` (01 §4.1) when a token is parsed/validated here. Vault-injected API keys are never on spans.

### N3 — Decisions in the BFF
Route/feature switches (portal tier from an env variable, help-topics cache fallback, local-dev token), proxy target selection (`{{PREFIX}}.proxy.target=support-case|billing|orders`), response shaping (pagination, filtering) → attributes on the SERVER span per 01 §5 (`{{PREFIX}}.fallback`, `{{PREFIX}}.cache_hit`, `feature_flag.*`).

### N4 — Outbound calls (handover)
The SDK creates a CLIENT span per axios/`http`/proxy request. Apply 04: `peer.service` from a host table (`SpanProcessor.onStart` or `http` instrumentation `applyCustomAttributesOnSpan`), `{{CORRELATION_HEADER}}` from the request context on every outbound request (a `prepareApiConfig` that already spreads all inbound headers is kept, but the header is also set explicitly when generated in N2; an Express server that spreads `req.headers` likewise), `{{PREFIX}}.call.purpose`, decisive ids/flags before, response ids/status after, and `{{PREFIX}}.upstream.*` + `{{PREFIX}}.handled` (`swallowed` for a chat notification and a source-control cache fallback, `passthrough` for proxied 401–500). `http-proxy-middleware`: `onProxyReq` sets the header, `onProxyRes` registers `{{PREFIX}}.upstream.status` and the proxied `correlation-id`.

### N5 — Errors
Global error middleware (`handleErrorMiddleware`, the BFF error JSON): `error.type`, `{{PREFIX}}.error_code` when the upstream body has one, `recordException` once, ERROR only for 5xx/transport; the JSON keeps `correlation-id` (02/03 contract for the browser).

### N6 — Node CronJobs
06 §J5.

---

## 5. Attribute contract shared with the backend

| Group | Attributes |
|---|---|
| Resource | `service.name` (`<portal>`, `<standalone-ui>`, `<bff>`, `<api-server>`), `service.version`, `deployment.environment`, `session.id`, `browser.*` |
| Navigation | `{{PREFIX}}.screen` (route template), `{{PREFIX}}.mfe`, `{{PREFIX}}.channel_type` |
| API call | `{{PREFIX}}.endpoint` (template), `http.request.method`/`http.response.status_code` (SDK), `{{PREFIX}}.correlation_id` |
| Errors | `{{PREFIX}}.error_code` (`{{ERR_PREFIX}}-…`), `{{PREFIX}}.error_type` (`api\|transport\|render`), `error.type`, `exception.*` |
| Business ids | `{{PREFIX}}.account_number`, `{{PREFIX}}.billing_account`, `{{PREFIX}}.invoice_number`, `{{PREFIX}}.order_number`, `{{PREFIX}}.document_id` |
| Identity (masked) | `user.name`, `{{PREFIX}}.org_id`, `{{PREFIX}}.actor.org_id`, `{{PREFIX}}.actor.present` |
| BFF | `{{PREFIX}}.proxy.target`, `{{PREFIX}}.upstream.*`, `{{PREFIX}}.handled`, `peer.service` |

Join keys: UI `{{PREFIX}}.correlation_id` == header `{{CORRELATION_HEADER}}` == BFF span == Java MDC/span == `ErrorDetails.correlationId`; UI `{{PREFIX}}.error_code` == backend `{{PREFIX}}.error_code`; `traceparent` links the tiers into one trace once U7 is done.

---

## 6. What must not be traced from the web tier
Tokens, cookies, `Authorization`, e-mails, names, free-text search strings, response bodies (only `errorCode`/`correlationId` are lifted), concrete URLs as span names, anything from the identity headers beyond the masked user and org id.

---

## 7. Verification (TraceQL)
```
{ resource.service.name = "<portal>" } | by(span.{{PREFIX}}.screen)                                              -- templates only, low cardinality
{ resource.service.name = "<portal>" && span.http.response.status_code >= 400 } | by(span.{{PREFIX}}.endpoint, span.{{PREFIX}}.error_code)
{ span.{{PREFIX}}.correlation_id = "<C>" }                                                                     -- browser span + BFF span + Java spans (one trace after U7; before it, several traces sharing the id)
{ resource.service.name = "<bff>" && span.{{PREFIX}}.correlation_id_generated = true } | by(span.http.route)    -- MFEs that still send no id
{ resource.service.name = "<bff>" && kind = client } | by(span.peer.service, span.http.response.status_code)
{ resource.service.name = "<portal>" && name = "render.error" } | by(span.{{PREFIX}}.screen)
```

---

## 8. Checklist — one UI app, one Node server
- [ ] UI: one provider per page (shell/standalone); MFEs on `@opentelemetry/api`; resource + masked user via `CustomSpanProcessor`; router spans by template; central HTTP layer sends `{{CORRELATION_HEADER}}` and the fetch/XHR `applyCustomAttributesOnSpan` hook lifts `errorCode`/`correlationId`/endpoint/screen onto the fetch span, 5xx/transport → ERROR; `ErrorBoundary` → `render.error`; business spans with ids only; `propagateTraceHeaderCorsUrls` + gateway CORS verified
- [ ] Node: SDK first in the entry; health ignored; door middleware (correlation id read-or-generate once, echo, identity/channel/session attributes, request-scoped logger with `trace_id`); decisions as attributes; outbound per 04 with `peer.service` and `{{PREFIX}}.handled`; global error middleware sets `error.type`/`{{PREFIX}}.error_code`; `sdk.shutdown()` on exit
- [ ] No tokens/PII; no concrete ids in names
- [ ] Verified: `{ span.{{PREFIX}}.correlation_id = "<C>" }` returns browser, BFF and Java spans; a forced 5xx shows `{{PREFIX}}.error_code` on the browser span and the same code on the Java span
