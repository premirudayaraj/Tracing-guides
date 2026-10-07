# 00 — What Qualifies for a Trace, and How to Discover a Project's Role

> **Read this first, for every project.** It gives (1) the test that decides whether a value or a decision belongs on a span, (2) the discovery procedure that tells you which *roles* a project plays (API, Kafka consumer, producer, scheduled job, batch, BFF, web UI, library …), and (3) which guide to apply for each role. An estate has hundreds of repositories and they do **not** all follow one pattern; every project is classified by what it does, and the rules are applied per role — never "one representative per family".
>
> Placeholders (`{{COMPANY}}`, `{{PREFIX}}`, `{{CORRELATION_HEADER}}` …) are defined once in `README.md §0`.
>
> Stack: Spring Boot / Kotlin / Node / React → OpenTelemetry (Java agent, OTel web SDK, OTel Node SDK) → OTLP → OTel Collector → **{{TRACE_BACKEND}}**; logs → **{{LOG_BACKEND}}**. The join key between traces, logs and error bodies is **`{{CORRELATION_HEADER}}`**.
>
> Legend used in all guides: **MUST** = the rule; **already** = usually present in current code and to be kept; ➕ = to be added (usually not in current code); ⚠ = not verified at runtime, confirm on one real span or log line.

---

## 1. The methodology in one paragraph

A request (or a message, or a job run) is **registered at the door**: the boundary span gets the identity of the request — correlation id, who is calling, on whose behalf, through which channel. Then, **wherever the flow goes, every decision that changes what the request does or returns is registered on that same span as an attribute the moment it is made** — authentication outcome, authorisation decision and what it was based on, which permitted accounts / assets the user got, feature flags, cache hits, fallbacks, filters that skipped work, which upstream was called and what it answered, the error code. Child spans (HTTP client, JDBC, Kafka, one span per fan-out method) hold *timing and transport*; the request span holds *the story*. In the trace backend one query on the root span then answers "what happened to this request and why", and the same correlation id finds the log lines and the error body the client saw.

The rules are the same for a Kafka consumer (the record is the request), a scheduled or batch run (the trigger is the request), a Node BFF (the inbound HTTP call is the request) and a browser (the user action is the request). Only the *door* differs.

---

## 2. Qualification test — does this value belong on the span?

Put a value on the current span when **all** of the following hold:

| # | Test | Passes | Fails |
|---|---|---|---|
| Q1 | **It decides or explains.** The value changes the code path, the upstream that is called, the data returned, or the status/error the caller gets — or it is the identifier someone will search for when that request is questioned. | `authz.decision=deny`, `support_org_mode=true`, `permitted accounts=0`, `upstream status=503`, `feature flag variant`, `account number`, `order number`, `Kafka offset` | a loop counter, a formatted message, an intermediate DTO |
| Q2 | **It is known at a point in the code, once.** There is one place where the value becomes final (after the policy call, after the cache lookup, after the send acknowledgement). | the policy response handler, the send callback, the `@Recover` method | values that are recomputed in five places |
| Q3 | **It is low-cardinality or an opaque id.** Enums, booleans, counts, status codes, error codes, and business identifiers (account, invoice, order, document, message id, offset). | `authz.reason=NOT_ENOUGH_PERMISSIONS`, `result_count=12`, `account_number=1234567` | free text, names, e-mails, addresses, payloads, SQL with literals |
| Q4 | **It is not a secret and not PII.** | masked user (`abc…xyz`), org id, user key | JWT, API key, `Authorization`, OAuth tokens, passwords, SASL config, e-mail, full name, phone |
| Q5 | **It would be wanted in an alert or a support ticket.** "Show me every request today where the policy decision was deny for policy billing/get", "every consumer record skipped because the account number was empty", "every send whose retry was exhausted". | | values only a debugger needs |

If Q1–Q5 pass → **attribute on the current span**, set *as soon as the value is known*, with a *default written before the branch* so the key exists on every span (`cache_hit=false` then `true`).

Do **not** create a new span to hold a decision. Spans are for **units of work with duration** (an HTTP call, a query, a message hop, a fan-out method, a job run); decisions are attributes on the span that made them. Do not use span *events* for decisions either — the OpenTelemetry project is deprecating the span-events API in favour of logs; keep decisions as attributes and exceptions as `recordException` (still supported) or a log line carrying `trace_id`.

### 2.1 What is always registered (the non-negotiable set)

| Where | Registered |
|---|---|
| Door of every root span (HTTP server, Kafka/Rabbit consumer, scheduled/batch/CronJob run, browser action) | `correlation_id`; identity of the caller / actor / channel (01 §3); for messages the message id, key, partition, offset, consumer group (05); for jobs the job name, trigger and run id (06) |
| Authentication + authorisation chain (filters, interceptors, aspects, policy engine / identity service / permission API / DB ownership checks) | source of identity, decision (`allow`/`deny`/`error`), policy or action evaluated, reason code, what the user was granted (counts of permitted accounts / assets / roles), impersonation mode (01 §4) |
| Every decision on the path | feature flag variant, cache hit/miss, fallback taken, filter that skipped work, chosen upstream/version, result count, "no record" outcomes (01 §5) |
| Every outbound call (HTTP to another internal service or a third party, incl. token/auth calls) | the dependency name, the request's key identifiers/flags, the response's identifiers and status, and the **decision made on the outcome** (mapped error code, ignored → a `<dependency>.<action>.failure` key, fallback) (04); a retried call has its own per-purpose span with a retry **count** (04 §4.4) |
| Every message published | topic/exchange, key, message id, the **acknowledgement** (partition + offset from the broker, or failure/retry-exhausted) (05) |
| Every message consumed | topic, partition, offset, key, consumer group, message id from the payload/header, the correlation id restored from the record, the processing outcome incl. **skipped / ignored** (05) |
| Every failure | **hard failure** (the program failed the operation): `error.type` (standard) + `error_code` (`{{ERR_PREFIX}}-<module><code>`), `recordException` once, status `ERROR` only when the operation really failed; **non-fatal error** (caught, the request carried on): one key per failed call, `<dependency>.<action>.failure` = short reason, + `failure_count`, no code (03 §2.1) |

---

## 3. Discovery procedure — determine the project's roles before touching code

Run this in the project root. Each hit maps to a role; a project usually has several roles. Record the result at the top of your change ("Roles: API, PRODUCER, SCHEDULED") — the checklists are per role.

### 3.1 Exclude first (never instrumented)

```sh
# CI/CD, IaC, images, gateway proxies, test automation, docs: out of scope
ls main.tf */main.tf 2>/dev/null; ls -d apiproxy */apiproxy 2>/dev/null
ls .github/workflows 2>/dev/null | head        # only a workflow repo if there is no src/
case "$PWD" in *automation*|*-test*|*e2e*|*selenium*|*cypress*|*cicd*|*workflow*|*helm*|*kustomize*|*puppet*|*terraform*|*apiproxy*|*hello-*|*petclinic*|*allure*) echo EXCLUDED;; esac
```
`INFRA`, `TEST`, `DOCS` projects get no tracing code. (They may *configure* it — the startup scripts and deployment overlays that attach the agent, the collector manifests, the platform ConfigMap live in such repositories; note where they are in your estate's 08 table.)

### 3.2 Language and framework

```sh
ls pom.xml build.gradle build.gradle.kts settings.gradle* package.json pyproject.toml requirements.txt 2>/dev/null   # anything else (other languages) → §4 last row
grep -l "spring-boot" pom.xml build.gradle* 2>/dev/null; grep -l "kotlin" build.gradle* 2>/dev/null
node -e 'const p=require("./package.json");const d={...p.dependencies,...p.devDependencies};console.log(Object.keys(d).filter(k=>/^(react|next|@angular|express|koa|fastify|kafkajs|node-cron|axios|node-fetch|@opentelemetry|appdynamics|dd-trace|elastic-apm-node|winston|pino|bunyan)/.test(k)).join(" "))' 2>/dev/null
```

### 3.3 Role detectors (Java / Kotlin)

| Role | Detector (grep -rl … src/main) | Guide |
|---|---|---|
| **API** (HTTP server) | `@RestController`, `@Controller`, `RouterFunction`, `@GrpcService`, `graphql` | 01, 02, 03 |
| **AUTH chain present** | in-house: policy annotations (`@Policy`, `@RequiredJWT`, `@PermissionCheck`, `@RoleAllowed`, `@AllowImpersonation`, `@AllowForReadonly` — whatever your libraries call them), the in-house request-context class, policy-engine URLs (`/v1/data/`, a policy sidecar host); standard: `SecurityFilterChain`, `oauth2ResourceServer`, `JwtDecoder`/`ReactiveJwtDecoder`, `jwks`, `@EnableMethodSecurity`, `@PreAuthorize`/`@PostAuthorize`/`@PostFilter`, `@Secured`, `@RolesAllowed`, `hasRole`/`hasAuthority`, `AuthorizationManager`, gRPC `ServerInterceptor`; generic: `OncePerRequestFilter`, `HandlerInterceptor`, `WebFilter`, `@Aspect` + `@Before/@Around` | 01 §4 (in-house engines) / 01 §4.2a (standard Spring Security) |
| **HTTP CLIENT / RPC** (handover) | `WebClient`, `RestTemplate`, `RestClient`, `@FeignClient`, `HttpClient`, `HttpURLConnection`, `OkHttp`, `WebServiceTemplate`, the in-house WebClient factory; gRPC stubs (`*Grpc.newBlockingStub`, `@GrpcClient`); `JavaMailSender`; workflow-engine clients; cloud SDK clients (STS, schema registry); Redis (`RedisTemplate`, Lettuce) | 04 |
| **CONSUMER** | `@KafkaListener`, `KafkaListenerContainerFactory`, `@RabbitListener`, `@StreamListener`, `Consumer<` (Spring Cloud Stream), `@JmsListener`, `@SqsListener` | 05 |
| **PRODUCER** | `KafkaTemplate`, `kafkaTemplate.send`, `ProducerRecord`, `RabbitTemplate`, `StreamBridge`, `AmqpTemplate`, an in-house notification library that publishes for you | 05 |
| **SCHEDULED** (in-app) | `@Scheduled`, `@EnableScheduling`, `TaskScheduler`, `Quartz`, `ShedLock` — then **read the method body**: an empty keep-alive (`doNothing()`), a token refresh or a config refresh is *not* a business job | 06 |
| **BATCH** | `spring-batch`, `@EnableBatchProcessing`, `JobBuilder`, `Tasklet`, `ItemReader`, `CommandLineRunner` + `JobLauncher` | 06 |
| **ASYNC / FAN-OUT** | `@Async`, `CompletableFuture.runAsync/supplyAsync`, `ExecutorService`, `ThreadPoolTaskExecutor`, `Mono.zip`, `Flux.parallel` | 01 §6, 05 §K4 |
| **OUTBOX / DEFERRED** | a task/outbox entity, "outbox", a stored tracing-context column | 05 §K8 |
| **LIB** | no `@SpringBootApplication` and no `main`; published to an artifact repository | §5 below |

### 3.4 Role detectors (Node / browser)

| Role | Detector | Guide |
|---|---|---|
| **BFF / NODE-SERVER** | `express`, `koa`, `fastify`, `http.createServer`, `http-proxy-middleware` in `dependencies` and a `server/` or `app/` entry | 07 |
| **WEB-UI** | `react`, `react-dom`, `single-spa`, `webpack` module federation, the in-house fetch/API-guide package; no server entry | 07 |
| **CRONJOB** | a Kubernetes `kind: CronJob` manifest in the repo, or `node-cron`/`cron` in dependencies, or a script that runs to completion and exits | 06 §J5 |
| **NODE CONSUMER/PRODUCER** | `kafkajs`, `node-rdkafka`, `amqplib` | 05 (same rules, Node SDK) |
| **NODE-LIB / TOOL** | `"private": false` with `main`/`exports` and no server/UI | §5 |

### 3.5 Runtime wiring to confirm (per Java service)

```sh
grep -rn "OTEL_\|otel\.\|-javaagent\|JAVA_TOOL_OPTIONS\|<your startup script's agent-options variable>" Dockerfile* startup*.sh bin/*.sh kubernetes/ helm/ kustomize/ 2>/dev/null | head
grep -rn "micrometer-tracing\|opentelemetry\|zipkin\|brave\|sleuth" pom.xml build.gradle* 2>/dev/null
grep -rn "management.tracing\|spring.zipkin\|spring.sleuth" src/main/resources config 2>/dev/null
grep -rn "logback\|logstash\|%X{" src/main/resources/logback*.xml 2>/dev/null | head
```
Three ways a project gets OpenTelemetry — know which one applies before adding code:
1. **Java agent attached at container start** by a shared startup script (`-javaagent:opentelemetry-javaagent.jar`, `OTEL_*`/`-Dotel.*` flags, enabled per environment by deployment overlays), through an injected agent-options environment variable read by the startup script, or **baked into the base image** — the agent creates SERVER / CLIENT / JDBC / PRODUCER / CONSUMER spans automatically.
2. **Micrometer Tracing inside the app** — a Brave bridge (`management.tracing.enabled`, a Zipkin reporter, possibly to Kafka) or an OTel bridge (`micrometer-tracing-bridge-otel`, Kafka containers with `isObservationEnabled=true`). This fills `%X{traceId}` in logs; it is **not** the agent's trace unless the bridge exports OTLP to the collector.
3. **Nothing** — typical for Node CronJobs, Express/Koa servers, React MFEs (unless one already runs the OTel web SDK), old batch jobs with only a vendor analytics hook.

The rules in 01–07 are written for case 1 (agent on) and stay harmless when the agent is off (`Span.current()` is a no-op span; `@WithSpan` is inert). **Case 2** (in-app Micrometer/OTel SDK only): the same code works because `Span.current()`/`GlobalOpenTelemetry` resolve to the in-app SDK when the OTel bridge is used; with the **Brave** bridge the OTel API is a no-op — only the MDC/log/header/response parts of the rules take effect there, which is why every rule also writes the correlation id to MDC and to headers. **Case 3** (nothing): the first change is the door/run id + correlation id in logs and headers (01 §3, 06 §J5, 07 §N1); adding the agent/SDK is a deployment change (an environment overlay, a base image with the agent, `@opentelemetry/sdk-node`), not application code — say so in the change instead of adding an SDK to the service.

---

## 4. Role → guide → checklist map

| Role | Door (root span) | Guide | Checklist |
|---|---|---|---|
| API (Spring MVC / WebFlux / gRPC) | agent SERVER span `<METHOD> <route>` | 01 (request span + auth chain), 02, 03 | 01 §9 |
| HTTP client / handover | agent CLIENT span `<METHOD>` under the caller's span | 04 | 04 §7 |
| Kafka / RabbitMQ producer | agent PRODUCER span `<topic> publish` | 05 §K1 | 05 §7 |
| Kafka / RabbitMQ consumer | agent CONSUMER span `<topic> process` | 05 §K2–K3 | 05 §7 |
| Async / fan-out inside a request | no new root; `@WithSpan` child | 01 §6, 05 §K4 | 01 §9 |
| Scheduled (`@Scheduled`) business job | ➕ `@WithSpan("<Job>.run")` root | 06 §J1–J3 | 06 §6 |
| Spring Batch job | ➕ root span per `JobExecution`, child per `StepExecution` | 06 §J4 | 06 §6 |
| Kubernetes CronJob (Node/Python/Java) | ➕ root span per process run (OTel SDK) | 06 §J5 | 06 §6 |
| Node BFF / server | OTel Node SDK SERVER span | 07 §N | 07 §8 |
| React MFE / web app | OTel web SDK (document-load, fetch/XHR, user-interaction) | 07 §U | 07 §8 |
| Library | no spans of its own except at the I/O it owns; exposes hooks | §5 | — |
| **Every role, after the code** | — | 09 (publish the service's trace documentation page to Confluence: repository name = page title, fixed template) | 09 §6 |
| Other languages (Python, others) | OTel SDK of that language, same collector, same vocabulary; the placement guides are written for JVM/Node/browser — apply their rules by analogy, no separate guide | 00 §6 | — |

---

## 5. Libraries (shared code used by many services)

* A library **never** starts a tracer provider, never sets `service.name`, never adds an exporter. It uses `io.opentelemetry:opentelemetry-api` only (`Span.current()`, `@WithSpan` from `opentelemetry-instrumentation-annotations`), which is a no-op unless the host application has the agent or an SDK.
* A library that owns an I/O boundary (a notification library that owns the Kafka producer for e-mail; the in-house REST library that owns the WebClient factory and the request filter; a common library that owns the Kafka config helper and the MDC filter; an outbox library that owns the deferred-task runner; a kafka-support library that owns the listener container factory) is **where the door/handover rules are implemented once** for every host: correlation-id header injection and restoration (05 §K1–K2), `peer.service` naming (04), MDC restoration. Fix the library, not every caller.
* A library must **expose the hook** the host needs to register decisions: e.g. a `sendEmail(...)` that publishes to Kafka should return the send result (topic, partition, offset, message id) so the host can register the acknowledgement (05 §K1) — a library that returns nothing and hides its own errors hides the outcome from every host.
* Libraries used only for tests, CLIs, code generators: nothing.

---

## 6. Naming — standard names first, then plain meaningful names

Use the OpenTelemetry semantic-convention attribute when one exists. For everything the standard does not cover, write a plain, meaningful name — **no company prefix**:
* lower-case; dots separate groups, underscores separate words inside a group (`authz.decision`, `job.run_id`, `correlation_id`); a hyphen in a dependency name becomes `_` in the key (the `peer.service` value `support-case` is the key group `support_case`; §6.1) (`support_case.create.failure`);
* the name says what the value is when read on its own in a trace (`grants.accounts_count`, not `count`);
* **never start with an OpenTelemetry namespace.** The first group must not be a namespace of the attribute registry (https://opentelemetry.io/docs/specs/semconv/registry/attributes/) — the ones most likely to be hit: `http`, `url`, `server`, `client`, `network`, `db`, `messaging`, `rpc`, `user`, `enduser`, `session`, `error`, `exception`, `event`, `code`, `service`, `peer`, `source`, `destination`, `app`, `feature_flag`, `log`, `file`, `process`, `thread`, `host`, `container`, `k8s`, `cloud`, `deployment`, `telemetry`, `otel`, `test`. OpenTelemetry's own naming guidance says not to reuse its namespaces for custom attributes, because a later release or an instrumentation library can define the same key with another meaning. That is why the guides use `grants.accounts_count` (not `user.accounts_count`) and `event_type` (not `event.type`). A single-group name such as `error_code` or `failure_count` is not inside a namespace (`error_code` is not `error.*`) and is fine;
* only when the natural name would start with a namespace and no better word exists — typically a dependency whose name *is* a namespace, e.g. a service literally called `db` or `messaging` — put `{{PREFIX}}.` (README §0) in front of **that key only**: `{{PREFIX}}.db.write.failure`. That is the only use of the company prefix.
* **name the key after what it is about.** A key that describes one call, one message, one topic or one exchange starts with the system and the thing, never with a generic word: `<system>.<action>.<fact>` for an HTTP call (`pbas.company_account_search.page_size`, `pbas.company_account_search.response.correlation_id`), `<system>.kafka.<subject>.<fact>` or `<system>.rabbitmq.<subject>.<fact>` for a message (`dih.kafka.userdetails.message.id`, `fabric.kafka.account.publish.result`, `dih.kafka.account.consumer.action`). `<system>` is the system on the other end — the `peer.service` name for a call, the system the topic feeds for a publish, the system that sent it for a consume; `<subject>` is the short business name of the topic, exchange or action, without environment suffixes (`account`, not `equinix.account.prod`). The key goes through `AttrName.of(...)` like every key that starts with a dependency name. Why: a generic key (`messaging.message.id`, `publish.result`, `consumer.action`, `call.page_size`) on a request span does not say which Kafka or which call it belongs to, and the second publish or call in the same request overwrites the first.
* **keys common to every service keep their common name**: the request's identity and caller (`correlation_id`, `correlation_id_generated`, `caller.*`, `user.*`, `session.id`, `org_id`, `channel_type`, `actor.*`), authentication and authorization (`authn.*`, `authz.*`, `grants.*`, `auth.*`), the request's outcome summary (`error_code`, `error_key`, `error.type`, `failure_count`, `upstream.*`, `handled`), business ids (01 §7), job keys on the job's own span (`job.*`), and every key the OpenTelemetry agent writes itself (`service.name`, `http.*`, `db.*`, and `messaging.*` on its own PRODUCER and CONSUMER spans).
* **shared helper code:** when the line that writes the key is shared by several systems or topics (one Kafka sender for every topic, a record interceptor on every listener, one WebClient filter for every client), build the name from what the helper receives — the topic, exchange or client it is given — through a small map kept next to the helper (`topic → <system>.kafka.<subject>`). When nothing the helper receives names the target, keep the generic key and put this comment on the line: `// TRACE: generic trace — this code does not know which system or topic it serves; the user has to instruct which name to use.` List every such line in the run report.

When the prefix is added, and by whom:

| Kind of key | Example | Prefix? | Who decides |
|---|---|---|---|
| Standard OpenTelemetry key | `http.route`, `user.name`, `error.type`, `messaging.message.id` | never — it *is* the standard | the key exists in the registry |
| Our fixed keys (the vocabulary of 01 §7) | `correlation_id`, `authz.decision`, `grants.accounts_count`, `account_number`, `job.run_id` | no — every fixed key in 01 §7 was checked against the registry | the guides |
| Keys whose first group is a **dependency, topic or job name** | `<dependency>.<action>.failure`, `<dependency>.outcome`, `<dependency>.version`, `<system>.<action>.<fact>`, `<system>.kafka.<subject>.<fact>` (keys like `check.<system>` or `call.<purpose>.*` start with a fixed word and are safe) | **only if** that name is a namespace: `support_case.create.failure` → no prefix; `db.write.failure` → `{{PREFIX}}.db.write.failure` | the `AttrName.of(...)` helper (01 §6, Node: `attrName(...)` in 07 N4) — the code decides at runtime, the assistant never has to remember |
| A new key the assistant invents for this service | `invoice.download_format` | no, unless its first group is a namespace — then rename it first; prefix only if no better word exists | the check below, run before the run finishes |

The namespace list (the registry's namespaces, September 2026 — refresh from the registry link above when it changes): android, app, artifact, aspnetcore, aws, azure, browser, cassandra, cicd, client, cloud, cloudevents, cloudfoundry, code, container, cpu, cpython, db, deployment, destination, device, disk, dns, dotnet, elasticsearch, enduser, error, event, exception, faas, feature_flag, file, gcp, gen_ai, geo, go, graphql, heroku, host, http, hw, ios, jsonrpc, jvm, k8s, linux, log, mainframe, mcp, messaging, network, nfs, nodejs, oci, onc_rpc, openai, openshift, opentracing, oracle_cloud, oracledb, os, otel, peer, pprof, process, profile, rpc, security_rule, server, service, session, signalr, source, system, telemetry, test, thread, tls, url, user, user_agent, v8js, vcs, webengine, zos.

The check the assistant runs on the project before ticking any checklist (it lists every key the code writes whose first group is a namespace; each printed key must be a real registry attribute — anything else is ours and must be renamed or prefixed):
```sh
NS="android|app|artifact|aspnetcore|aws|azure|browser|cassandra|cicd|client|cloud|cloudevents|cloudfoundry|code|container|cpu|cpython|db|deployment|destination|device|disk|dns|dotnet|elasticsearch|enduser|error|event|exception|faas|feature_flag|file|gcp|gen_ai|geo|go|graphql|heroku|host|http|hw|ios|jsonrpc|jvm|k8s|linux|log|mainframe|mcp|messaging|network|nfs|nodejs|oci|onc_rpc|openai|openshift|opentracing|oracle_cloud|oracledb|os|otel|peer|pprof|process|profile|rpc|security_rule|server|service|session|signalr|source|system|telemetry|test|thread|tls|url|user|user_agent|v8js|vcs|webengine|zos"
grep -rhoE "(setAttribute|RequestSpan\.(set|setIfAbsent|here))\(\s*[\"'][a-z_][^\"']*[\"']" src 2>/dev/null | sed -E "s/.*[\"']([^\"']+)[\"']$/\1/" | sort -u | grep -E "^($NS)\."
# example output:  user.name  session.id  → registry attributes, fine;   user.accounts_count  event.type  db.custom → ours: rename (grants.accounts_count, event_type) or prefix
```

The full vocabulary is in 01 §7; the rule of thumb:

| Concept | Use | Not |
|---|---|---|
| HTTP method / route / status | `http.request.method`, `http.route`, `http.response.status_code` (agent) | `status` |
| Dependency name | `peer.service` (agent mapping; deprecated in the conventions in favour of `service.peer.name`, query what the deployed agent writes) | `upstream_host` |
| Header of an outbound call | `http.request.header.<lower-case name>` on the CLIENT span (agent config, never credential headers) | `crh-req-header`, `crh.req.headers` |
| Caller identity (person) | `user.name` (masked login), `user.id` (user key), `user.roles`, `enduser.id` only when the raw id is allowed | `user` |
| Calling **service** (inbound) | `caller.service` (the caller's `service.name`; `external`/`unknown` when none) + `caller.source` (how it was obtained: mesh cert, gateway header, api-key registry, jwt client claim, self-declared header) — no standard key exists for this; `client.address` stays the agent's IP (01 §3.1) | `caller_ip`, `source_app`, a hostname |
| Failure type | `error.type` (exception class or domain code) + `error_code` (`{{ERR_PREFIX}}-…`) on hard failures; `<dependency>.<action>.failure` (short reason) + `failure_count` on non-fatal ones; `retry.count` on per-purpose retry spans | `exception`, `handled_error_code`, `retry.attempt_<n>` |
| Feature flag | `feature_flag.key`, `feature_flag.result.variant`, `feature_flag.provider.name` (one flag per span) or `flag.<key>=<variant>` when several flags decide one request | `<vendor>_flag` |
| Message identity | our code: `<system>.kafka.<subject>.message.id` (`dih.kafka.userdetails.message.id`) on the caller's span and on the consumer's span; the agent: `messaging.message.id`, `messaging.kafka.offset`, `messaging.destination.partition.id`, `messaging.kafka.message.key`, `messaging.consumer.group.name`, `messaging.rabbitmq.destination.routing_key` on its own PRODUCER/CONSUMER spans — never written again by our code | `messaging.message.id` written by our code, `offset` |
| Session | `session.id` (browser / `{{SESSION_HEADER}}`) | `session` |
| Code location of a manual span | `code.function.name` (agent sets it for `@WithSpan`) | — |
| Our business ids and decisions | common: `correlation_id`, `account_number`, `authz.*`, `job.*`; facts about one call or message carry its name: `pbas.company_account_search.page_size`, `fabric.kafka.account.publish.result`, `dih.kafka.account.consumer.action` | ad-hoc names; `publish.result`, `consumer.action`, `call.page_size` without the system |

### 6.1 OpenTelemetry key rules (mandatory, checked by `tools/trace-coverage.py`)

Sources: naming https://opentelemetry.io/docs/specs/semconv/general/naming/ , errors https://opentelemetry.io/docs/specs/semconv/general/recording-errors/ , HTTP spans https://opentelemetry.io/docs/specs/semconv/http/http-spans/

* **Characters:** lower-case `a-z`, digits, `.` between namespace groups, `_` between words inside a group. No hyphen, no capital, no space; starts with a letter, ends with a letter or digit, never two delimiters in a row. Pattern: `^[a-z][a-z0-9]*([._][a-z0-9]+)*$`. A hyphen in a dependency name becomes `_` in the key (`AttrName.of` does it).
* **Short, but whole words:** the words the conventions use (`request`, `response`, `status_code`, `count`, `id`, `duration`), no abbreviations; read alone in a trace the key still says what the value is.
* **Standard key first, our key only for what the standard does not name.** The transport facts of a call are already on the agent's CLIENT span and are never copied under another name: `http.request.method`, `url.full`, `server.address`, `http.response.status_code`, `http.request.resend_count`, `error.type`, `messaging.*`, `db.*`. Our keys carry the business facts: purpose, ids, counts, the decision taken — named after the call or message they describe (§6): `pbas.company_account_search.ids_count`, not `call.ids_count`.
* **Headers:** a header of an outbound call is `http.request.header.<lower-case header name>` (reply: `http.response.header.<name>`) on the CLIENT span, written by the agent when the header is listed in `otel.instrumentation.http.client.capture-request-headers` / `capture-response-headers`. Authorization, api-key, cookie and token headers are never listed.

| Functionality | Key | Rule |
|---|---|---|
| CRH call made externally: a request header sent | `http.request.header.x-correlation-id` on the CLIENT span (agent config) | standard key; not `crh-req-header` |
| CRH call: why it was made | the key prefix `crh.billing_account_get.` on the caller's span; `call.purpose` = `crh.billing_account_get` only on a span that exists for this one call (04 §4.4 retry span) | §6, 04 §4.1 |
| CRH call: which account | `crh.billing_account_get.id` | 04 §4.1 |
| CRH call: HTTP status | `http.response.status_code` on the CLIENT span; `upstream.status` on the root when it fails the request | standard key / 03 |
| CRH call: outcome for this request | `crh.outcome` = `ok` / `mapped` / `fallback` | 04 §4.3 |
| Cache lookup before the CRH call | `crh.cache.hit` = `true` / `false` | decision key |
| DIH address-fields call: fields returned | `dih.address_fields.response.count` | 04 §4.3 |
| Kafka publish result | `fabric.kafka.account.publish.result` = `acked` / `failed` / `exhausted` and `fabric.kafka.account.message.id` (our code, on the caller's span); `messaging.kafka.offset` stays the agent's, on its PRODUCER span | 05 |

* **Where a key goes:** facts about one call on the span of that call's caller (or its own span); only the outcome summary goes on the request's root span. A span keeps at most 128 attributes by default (`OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT`) and drops the rest without warning.
* **Errors:** a failure that ends the operation sets span status `Error` plus `error.type` (exception class or short code) and records the exception once. An error that is retried or handled so the operation still completes does **not** set status `Error`: it is written as the failure key of 03 (`<dependency>.<action>.failure`) and the program carries on. An HTTP 4xx is `Error` on the CLIENT span, never on the SERVER span.
* **`peer.service`** is deprecated in the conventions in favour of `service.peer.name`. Query whichever the deployed agent writes, checked on one CLIENT span before a query or dashboard is written.
* Never the `otel.*` namespace; never a new key inside a convention namespace (§6).

---

## 7. Coverage: every function, every call, every decision (mandatory)

Roles (§3) decide which guides apply. They never allow a class or a function to be left out. A run that leaves an outbound call or a request-path decision without its keys is not finished: "deferred", "left as a gap" or "outside the scope of this pass" are not valid outcomes in the report.

### 7.1 What must be covered

| Item | Where | Keys it must leave |
|---|---|---|
| **Outbound call**: HTTP, gRPC, message publish, token or identity call, file store, e-mail or notification, a cache or database read that decides the answer | every function that performs it, in whatever class (client, service, listener, job) | **before** the call: `call.purpose` and the decisive request fields (04 §4.1); **after**: response id / count / status and `<dependency>.outcome` (04 §4.3); **on failure**: status `Error` + `error.type` when the failure ends the operation, with `upstream.name` / `upstream.status` / `upstream.code` / `handled` on the root; `<dependency>.<action>.failure` when the program carries on (03) |
| **Decision**: `if` / `switch` / ternary, `orElse` / `filter`, a feature flag, a fallback, an early return, a catch that carries on, a validation result, a retry | every function that chooses between outcomes for the request or the job run | the decision and the input that drove it (01 §7: `<area>.decision`, `fallback`, `feature_flag.*`, a count or an id), plus the failure key when the branch swallows an error |

Two things do not count as coverage: the agent's automatic CLIENT span on its own (URL, status and exception, none of the business fields above), and a key set on a path this call does not pass through.

A branch needs no key only when it cannot change what the request returns, where it goes or what it stores: a null or format guard on an internal value, a log statement, a pure mapping. Those functions are still listed in the report (§7.3), never skipped silently.

### 7.2 Work list, before writing code

```sh
python3 {{GUIDES_DIR}}tools/trace-coverage.py <project> > /tmp/trace-coverage-before.txt
```

It prints one `CALL` line per outbound call site (keys found before / after / on failure in the function and its callers, and what is `MISSING`), one `DECISION` line per function that branches but writes no key, one `BADKEY` line per key that breaks §6.1, and a `SUMMARY`. Work through every line, one class at a time.

### 7.3 Done means

* the tool, run again, prints `missing 0` and `badkeys 0`;
* every `DECISION` line still printed is listed in the report with the reason it needs no key (§7.1);
* the report has one row per outbound call: function, dependency, keys before / after / on failure.
