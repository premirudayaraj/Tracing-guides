# Tracing Guides — OpenTelemetry placement rules for any service estate

Guides for placing distributed tracing in **any** project of a company's estate — Spring Boot / Kotlin services, Kafka and RabbitMQ producers and consumers, scheduled and batch jobs, Kubernetes CronJobs, Node BFFs and API servers, React micro-frontends — so that the trace in the trace backend, the log lines in the log backend and the error body the client received are joined by one identifier, the **correlation id**, and so that the request span tells the whole story of the request: who called, what was allowed, every decision made on the way, every handover to another system and its outcome, every message published (with its acknowledgement) and consumed (with its receipt), and the final error code.

They are written to be handed to an AI assistant that must instrument a project **exactly where it is necessary and nowhere else**. The method: **classify the project by what it does** (00), **run the analysis** the guide for that role prescribes (a table of decision points / call sites / listeners with file:line), then apply the placement rules and tick the checklist.

---

## 0. Adapt to your company — the only place you change

Every guide uses the placeholders below. **Fill this table once**; an assistant reading the guides substitutes every `{{TOKEN}}` with the value here, and a shell one-liner instantiates the files for your repository. Nothing else in the guides is company-specific.

| Placeholder | Meaning | Example value |
|---|---|---|
| `{{COMPANY}}` | Company / platform name used in prose | `Acme` |
| `{{PREFIX}}` | Namespace of custom span attributes (lower-case, short); standard OpenTelemetry names are used wherever one exists, this prefix only for company concepts | `acme` → `acme.correlation_id` |
| `{{CORRELATION_HEADER}}` | The HTTP header / message header carrying the correlation id everywhere | `X-CORRELATION-ID` |
| `{{ERR_PREFIX}}` | Prefix of client-facing error codes, format `{{ERR_PREFIX}}-<module><code>` | `AC` → `AC-4210465` |
| `{{USER_HEADER}}` | Header carrying the authenticated user's login (masked on spans) | `X-AUTH-USER-NAME` |
| `{{USER_KEY_HEADER}}` | Header carrying the user's opaque key/id | `X-AUTH-USER-KEY` |
| `{{ORG_HEADER}}` | Header carrying the customer organisation id | `X-AUTH-ORG-ID` |
| `{{ACTOR_USER_HEADER}}` | Header carrying the impersonating (acting) user when acting on behalf of a customer | `X-ACTOR-USER-NAME` |
| `{{ACTOR_ORG_HEADER}}` | Header carrying the organisation acted on behalf of | `X-ACTOR-ORG-ID` |
| `{{CHANNEL_HEADER}}` | Header naming the channel / portal the request came through | `X-CHANNEL-TYPE` |
| `{{SESSION_HEADER}}` | Header carrying the browser session id | `X-SESSION-ID` |
| `{{APIKEY_HEADER}}` | Service-to-service API key header (never on spans) | `X-AUTH-APIKEY` |
| `{{SOURCE_APP_HEADER}}` | Header naming the calling application (service-to-service) | `X-SOURCE-APP` |
| `{{TRACE_BACKEND}}` | Where traces are queried | `Grafana Tempo (TraceQL)` |
| `{{LOG_BACKEND}}` | Where logs are queried | `Grafana Loki (LogQL)` |
| `{{COLLECTOR_ENDPOINT}}` | OTLP endpoint the agents/SDKs export to | `http://otel-collector:4318` |
| `{{ESTATE_ROOT}}` | Local path under which every repository is checked out | `~/work/repos/` |
| `{{GUIDES_DIR}}` | Where these guides live relative to the estate | `tracing-guides/` |

Instantiate for a repository (optional — the assistant can also read the placeholders as they are):
```sh
cd <the folder holding these guides> && for f in *.md; do sed -i.bak \
  -e 's/{{COMPANY}}/Acme/g' -e 's/{{PREFIX}}/acme/g' -e 's/{{CORRELATION_HEADER}}/X-CORRELATION-ID/g' -e 's/{{ERR_PREFIX}}/AC/g' \
  -e 's/{{USER_HEADER}}/X-AUTH-USER-NAME/g' -e 's/{{USER_KEY_HEADER}}/X-AUTH-USER-KEY/g' -e 's/{{ORG_HEADER}}/X-AUTH-ORG-ID/g' \
  -e 's/{{ACTOR_USER_HEADER}}/X-ACTOR-USER-NAME/g' -e 's/{{ACTOR_ORG_HEADER}}/X-ACTOR-ORG-ID/g' -e 's/{{CHANNEL_HEADER}}/X-CHANNEL-TYPE/g' \
  -e 's/{{SESSION_HEADER}}/X-SESSION-ID/g' -e 's/{{APIKEY_HEADER}}/X-AUTH-APIKEY/g' -e 's/{{SOURCE_APP_HEADER}}/X-SOURCE-APP/g' -e 's|{{TRACE_BACKEND}}|Grafana Tempo (TraceQL)|g' \
  -e 's|{{LOG_BACKEND}}|Grafana Loki (LogQL)|g' -e 's|{{COLLECTOR_ENDPOINT}}|http://otel-collector:4318|g' -e 's|{{ESTATE_ROOT}}|~/work/repos/|g' -e 's|{{GUIDES_DIR}}|tracing-guides/|g' "$f"; done
```
The default stack the rules assume: OpenTelemetry (Java agent, web SDK, Node SDK) → OTLP → OTel Collector → `{{TRACE_BACKEND}}`; application logs → `{{LOG_BACKEND}}`. TraceQL/LogQL examples are given; translate them if your backends differ.

---

## 1. The guides

| File | Role(s) | Give it to the assistant when… |
|---|---|---|
| [00-QUALIFICATION-AND-DISCOVERY.md](00-QUALIFICATION-AND-DISCOVERY.md) | all | **always first** — the test for what belongs on a span, the discovery procedure that determines a project's roles, exclusions, standard-vs-`{{PREFIX}}.*` naming, libraries |
| [01-REQUEST-SPAN-AND-AUTH-CHAIN.md](01-REQUEST-SPAN-AND-AUTH-CHAIN.md) | API (Spring MVC / WebFlux / gRPC) | the request span: registration at the door, the authentication / authorisation chain (policy engine, identity service, permission API, DB ownership, roles), grants, impersonation, every decision on the path, the `RequestSpan` helper, the attribute vocabulary |
| [02-ERROR-CODE-DESIGN.md](02-ERROR-CODE-DESIGN.md) | API | defining error codes (`{{ERR_PREFIX}}-<module><code>`), catalogue, exception classes, response body, status policy |
| [03-ERROR-CODE-TRACING.md](03-ERROR-CODE-TRACING.md) | all | the outcome on the span / log / response: `error.type`, `{{PREFIX}}.error_code`, upstream status/code, `{{PREFIX}}.handled`, hidden-200 outcomes |
| [04-OUTBOUND-HANDOVER.md](04-OUTBOUND-HANDOVER.md) | HTTP client (any role) | every call to another service or third party: the authentication step (token endpoints, keys), decisive request fields, response identifiers, and the decision on the outcome (mapped / swallowed / fallback / retried / treated-as-success); `peer.service` |
| [05-KAFKA-RABBIT-AND-ASYNC.md](05-KAFKA-RABBIT-AND-ASYNC.md) | PRODUCER, CONSUMER, ASYNC, OUTBOX | producer acknowledgements (partition/offset, message id, retry exhausted), consumer receipts (message id, offset, key, group, restored correlation id), skipped/swallowed outcomes, RabbitMQ, thread hops, deferred work |
| [06-SCHEDULED-BATCH-CRONJOB.md](06-SCHEDULED-BATCH-CRONJOB.md) | SCHEDULED, BATCH, CRONJOB | one root span per run with a run id as correlation id; Spring Batch job/step listeners; Kubernetes CronJob processes; refreshers vs business jobs |
| [07-UI-BFF-NODE.md](07-UI-BFF-NODE.md) | WEB-UI, BFF, NODE-SERVER | browser spans by route template, correlation id from the browser, backend error codes lifted onto fetch spans, Node SDK bootstrap, door middleware, proxy handover |
| [08-ROLE-TABLE.md](08-ROLE-TABLE.md) | all | how to produce the estate's project → role table (commands, columns, example), which the assistant builds once per estate and keeps next to these guides |

## 2. Prompt template

```
You are instrumenting <project> (path: {{ESTATE_ROOT}}<project>). Follow {{GUIDES_DIR}} exactly; the placeholders are defined in README §0.
1. Read 00 and run its §3 discovery on the project; state the roles you found ("Roles: API, PRODUCER, SCHEDULED").
2. For each role, run the mandatory analysis section (01 §2 / 04 §2 / 05 §2 / 06 §2 / 07 §2) and print the table (file:line | decision or call or listener | inputs that matter | output | attributes to set) BEFORE writing code.
3. Apply the placement rules for those roles only. Use the vocabulary in 01 §7 and the standard names in 00 §6; never invent parallel attribute names; never put tokens, PII, payloads or lists on spans.
4. Items marked ➕ are to be added; "already" items are kept; ⚠ items are verified on one real span before being relied on.
5. Finish with the checklist of each guide you applied, ticked against the code you wrote, and the TraceQL queries that prove it.
Do not add a second tracing stack (SDK/exporter/other APM agent) to a service; the OpenTelemetry agent or SDK already present is the stack.
```

## 3. Facts the rules are built on (from a real estate of several hundred repositories, anonymised)

* Three ways a project gets OpenTelemetry (00 §3.5): the **Java agent** attached at container start by a shared startup script or baked into the base image; **Micrometer Tracing** inside the app (a Brave/Zipkin bridge or an OTel bridge) — which fills `%X{traceId}` in the logs but is not the agent's trace unless it exports OTLP; or **nothing** (typical for Node servers, CronJobs, most micro-frontends, old batch jobs). The rules stay harmless with the agent off.
* **The correlation id header is the same on the wire everywhere**, but MDC keys and outbound header spellings drift per service; the guides keep existing keys and add the common ones.
* Typical gaps found: no service registers a custom span attribute; no producer writes the correlation id into message headers; almost nobody looks at the broker acknowledgement; consumers generate a fresh correlation id per message; consumer error handlers skip poison messages silently; several outbound failures are swallowed or reported as success. These are the gaps the rules close.
* **Error codes**: `{{ERR_PREFIX}}-<module><code>` with a fixed module id per service and number bands (permissions, validation, unknown, upstream, datastore, messaging); a second, name-based family may exist in another product line — keep it, register it the same way.
* Health probes are dropped by a collector filter on `http.route`; services that expose probes on a management port/path need the filter extended.
* Standard OpenTelemetry attribute names are used wherever one exists (`http.*`, `messaging.*`, `peer.service`, `error.type`, `user.*`, `feature_flag.*`, `session.id`); `{{PREFIX}}.*` only for company concepts (00 §6, 01 §7).

## 4. Evidence and provenance

The rules were derived by reading, end to end, the request chains, outbound clients, message producers/consumers, jobs, Node servers and UIs of a real estate (three REST API services with three different authorisation designs, a WebFlux authorisation service, a Kotlin Kafka worker, Spring Batch jobs, a Node CronJob, a Koa BFF, an Express API and React micro-frontends) and comparing them with the OpenTelemetry semantic conventions (HTTP, messaging, Kafka, RabbitMQ, error recording, feature flags, user/enduser, browser). Each guide's reference-patterns section (§0 in 01–07, §5 in 08) summarises the anonymised patterns it generalises from.
