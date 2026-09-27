# 08 — Project → Role Table: how to build it for your estate (template + procedure)

> Every repository of the estate, classified by **what it does**, with the guide(s) to apply. This file is a **template and a procedure**: an assistant builds the table once per estate (from the existing documentation first, then a code scan), keeps it next to these guides as `08-ROLE-TABLE.<estate>.md`, and re-runs `00 §3` on any project before instrumenting it — the table is the starting point, not a substitute for the discovery procedure. Placeholders (`{{ESTATE_ROOT}}`, `{{GUIDES_DIR}}`, `{{CORRELATION_HEADER}}`) are defined in `README.md §0`.
>
> **How to use with an AI assistant:** *"Build the role table for the estate under `{{ESTATE_ROOT}}` per 08-ROLE-TABLE.md: §1 sources, §2 markers, §3 rules, §4 output format. Use the existing documentation before reading code."*

---

## 1. Sources, in this order

1. **Existing documentation** — most estates already have a service catalog, per-service analysis files, architecture overviews, `README`s with a "what this service does" section. Ask the assistant to **search the whole machine** for them first (`*catalog*.md`, `*architecture*`, `*overview*`, `services/*.md`, `docs/`, knowledge-base folders) and read them before opening any code; a documented catalog typically covers the main organisation's repositories but **not** repositories cloned from other organisations or product lines — those come from the code scan.
2. **A build-file and marker scan** of every checked-out project directory under `{{ESTATE_ROOT}}` (§2). Where documentation and code disagree, **the code wins** and the note column says so ("docs say LIB").
3. **The evidence gathered by the other guides** (01 §2, 04 §2, 05 §2, 06 §2, 07 §2 inventories) for the projects already analysed — they refine the roles (a `@Scheduled` that is only a keep-alive is not SCHEDULED; a Kafka producer hidden in a library makes the host "PRODUCER (via library)").

## 2. Markers the scan looks for

| Role | Markers (any language) |
|---|---|
| API | `@RestController`, `@Controller` + `@ResponseBody`, `RouterFunction`, gRPC `*ServiceImplBase`, FastAPI/Flask routers, `spring-boot-starter-web/webflux` in the build |
| CONSUMER | `@KafkaListener`, `@RabbitListener`, `KafkaConsumer`, `Consumer<…>` beans with `spring-cloud-stream`, `kafkajs`/`amqplib` consumers |
| PRODUCER | `KafkaTemplate`, `StreamBridge`, `RabbitTemplate`, `KafkaProducer`, `kafkajs` producer; **also** a dependency on a house library that publishes (notifications/events library) → "PRODUCER (via library)" |
| SCHEDULED | `@Scheduled`, `@EnableScheduling`, Quartz, `node-cron`, `setInterval` at module level — read the body: an empty keep-alive or a token/config refresher is **infrastructure**, not a business job (06 §J2) |
| BATCH | `spring-batch`, `JobBuilder`/`StepBuilder`, `CommandLineRunner` that launches a job |
| CRONJOB | a Kubernetes `CronJob` manifest in the repo; a process with a `main` that runs to completion; Helm/kustomize `kind: CronJob` |
| NODE-SERVER / BFF | `express`, `koa`, `fastify`, `http-proxy-middleware` in `package.json`; BFF when it proxies to backend services for a browser app |
| WEB-UI | `react`, `single-spa`, `webpack.config` with a `remoteEntry`/module federation, `index.html`; an MFE when it is loaded by a shell |
| LIB | no entry point (no `@SpringBootApplication`, no `main`, no server start), published artifact (`maven-publish`, `npm publish`, `private: true` + `main`) |
| INFRA / TEST / DOCS / CLI | CI workflows, actions, Terraform, Helm charts, base images, gateway policies, e2e/perf test suites, documentation-only repos, command-line tools — **not instrumented** (00 §5), but note which INFRA repos hold the **agent attachment scripts, the collector manifests, the gateway CORS policy** (07 §U7) because the other guides refer to them |
| Auth hints (for 01) | `spring-security`, OPA/policy-sidecar clients, JWT libraries, identity-service clients, `@PreAuthorize`, custom auth filters/aspects |
| Tracing already present (for 00 §3.5) | `opentelemetry-javaagent` in build or startup scripts, `micrometer-tracing-bridge-*`, `brave`, `zipkin`, `@opentelemetry/*` in `package.json`, an APM agent |

A scan script that emits one line per project — `name | category | roles | markers found` — is enough; keep it in `{{GUIDES_DIR}}/tools/` and re-run it when repositories are added. Exclude generated code, `node_modules`, `target/`, `build/`, vendored SDKs, sample apps (`petclinic`, `hello-world`) — mark those TEST/excluded.

## 3. Classification rules (place each repository in exactly one family)

Roles are the union of what the markers and the documentation say (refined by the guide inventories); the **family** decides which guides apply and is chosen in this order:

| # | Condition | Family (guides) |
|---|---|---|
| 1 | CRONJOB or BATCH, and no API | Batch / CronJob / scheduled-only (06) |
| 2 | WEB-UI and not NODE-SERVER | Web UIs (07 U) |
| 3 | NODE-SERVER or BFF | Node servers and BFFs (07 N, 04) |
| 4 | API and any of CONSUMER / PRODUCER / SCHEDULED / BATCH / RABBIT | API + messaging / scheduled (01–06) |
| 5 | API only | API services (01, 02, 03, 04) |
| 6 | CONSUMER or PRODUCER, not LIB | Pure message workers (05) |
| 7 | SCHEDULED, not LIB | Batch / CronJob / scheduled-only (06) |
| 8 | LIB | Libraries (00 §5) — instrumented once for every host (interceptors, helpers), never with their own root spans |
| 9 | everything else | Not instrumented (INFRA / TEST / DOCS / CLI / UNKNOWN) |

Multi-module repositories: one row for the repository, with the modules and their roles listed in the note column (`service/{a (API+CONSUMER), b (API+SCHEDULED)}; library/{…}; stub/{…} (API)`), because the assistant is given a repository, not a module. A Python API (FastAPI) is an API row with the note "OTel Python SDK with the same attribute vocabulary (00 §6)".

## 4. Output format

```markdown
# 08 — Project → Role Table — <estate name> (<date>)
> Sources: <catalog file(s)>, <architecture docs>, code scan of <N> directories (<M> classified). Code wins over docs; notes say where they disagree.
> Re-run `00 §3` on any project before instrumenting it.

## API services (01,02,03,04) — <count>
| Repository | Catalog category | Roles (code-verified where noted) | Notes / where the roles live |
|---|---|---|---|
| `<repo>` | <category from the docs> | API | <module path>; <framework / tracing present / error-code module id / notable clients> |

## API + messaging / scheduled (01–06) — <count>
| … | … | API, CONSUMER, PRODUCER, SCHEDULED | <listeners, producers, refreshers vs business jobs, RabbitMQ, async pools> |

## Pure message workers (05) — <count>
## Batch / CronJob / scheduled-only (06) — <count>
## Node servers and BFFs (07 N, 04) — <count>
## Web UIs (07 U) — <count>
## Libraries (00 §5) — <count>
## Not instrumented (INFRA / TEST / DOCS / CLI / UNKNOWN) — <count>
* **CLI** (<n>): <names>
* **DOCS** (<n>): <names>
* **INFRA** (<n>): <names> — annotate the ones the guides refer to: `<repo> (startup scripts that attach the agent; overlays that enable/disable it)`, `<repo> (OTel Collector manifests + config ConfigMap)`, `<repo> (edge proxy / gateway config — where CORS for traceparent/{{CORRELATION_HEADER}} is checked, 07 U7)`
* **TEST** (<n>): <names>
* **UNKNOWN** (<n>): <names> — resolve with 00 §3 before instrumenting
```

The note column is what makes the table useful to an assistant: it should say **where the roles live** (module path), **what tracing is already present** (agent / Micrometer bridge / nothing — 00 §3.5), the **error-code module id** (02), the **house examples** worth copying ("producer interceptor registers acks — house example for 05 K1"; "reads the ack offset"; "deferred-task runner storing W3C context — 05 K8 pattern"; "OTel web SDK reference implementation — 07 U1"), and known deviations ("`@Scheduled` is only a keep-alive", "runAsync on the common pool", "docs say LIB").

## 5. Example rows (anonymised, from a real estate of ~450 directories)

| Repository | Catalog category | Roles | Notes / where the roles live |
|---|---|---|---|
| `billing-api` | Backend/API (in-house REST library) | API | Spring Boot 3 / Java 17, policy-engine sidecar, Brave bridge; module `{{ERR_PREFIX}}-4210`; 6 WebClients (data hub, account platform, document store, admin, cloud identity, file storage); no Kafka/@Scheduled |
| `order-api` | Backend/API (in-house REST library) | API, PRODUCER (via library) | WebClients (data hub, billing UI, account platform, document store, support-case); e-mail via the notifications library's Kafka; `runAsync` on the common pool |
| `account-platform-api` | Backend/API | API, CONSUMER, PRODUCER, SCHEDULED, RABBIT | RestTemplate ×18 dependencies; Kafka consumers on two account topics, producers account/billing-account; RabbitMQ listeners ×4 + one publisher; `@Scheduled` refreshers; `@Async` backfills; Micrometer OTel bridge + agent via startup options |
| `identity-core` | Backend/API | API, CONSUMER, PRODUCER, SCHEDULED, LIB | multi-module: `service/{ingestor (API+CONSUMER+PRODUCER), access-management (API+CONSUMER+PRODUCER+SCHEDULED), authorization (WebFlux API), client-management (gRPC server; the only @Scheduled is a keep-alive)}`; `library/{kafka-support, security-core, events (PRODUCER)}`; `stub/*` (API); `kubernetes-template` = INFRA (collector ConfigMap) |
| `audit-log-service` | Identity | API, PRODUCER, SCHEDULED | produce-only Kafka; `KafkaEventProducer` reads the ack offset (house example) |
| `report-jobs` | Security | API, BATCH, SCHEDULED, CONSUMER, PRODUCER, LIB | `service/{monthly-report-job, quarterly-report-job, cleanup-job, close-ticket-job}` = Spring Batch (06 J4); `poll-ticket-job` = PRODUCER+BATCH; stubs API+SCHEDULED; `library/*` (APM analytics integrations) |
| `connection-provisioning-service` | Backend/API | CONSUMER, PRODUCER | Kotlin worker, no REST; Micrometer OTel bridge; producer interceptor registers acks (house example); `event.id` headers |
| `monitoring-cronjob` | Backend (Node) | CRONJOB | Node ESM, K8s CronJob `*/5`; compares ~10 systems; chat notification + PostgreSQL; no tracing (06 J5) |
| `portal-bff` | Backend (Node) | NODE-SERVER, BFF | Koa BFF for the portal; APM agent; proxies all inbound headers; echoes `{{CORRELATION_HEADER}}` (07 N) |
| `case-management-api` | Backend (Node) | NODE-SERVER, BFF | Express; pino logger with `correlationId`; axios; `http-proxy-middleware` to the support-case system (07 N) |
| `billing-mfe` | Frontend/MFE | WEB-UI | React MFE; shared fetch package; internal-portal headers |
| `account-platform-ui` | Frontend | WEB-UI | React; OTel web SDK reference implementation (07 U1); RUM script |
| `java-lib-common` | Identity | LIB (PRODUCER helper) | `KafkaConfigHelper`, `KafkaEventConsumer` base, `MdcFilter`, `KafkaRetryConfiguration` — apply 05 K1/K2 here once for every host |
| `java-lib-table-outbox` | Identity | LIB, SCHEDULED | deferred-task runner storing W3C context (05 K8 pattern) |
| `ci-workflows` | CI/CD | INFRA | `startup-java-*.sh` attach the agent; kustomize overlays enable/disable `OTEL_ENABLED` |
| `distributed-tracing-config` | K8s | INFRA | OTel Collector manifests per env + tracing ConfigMap (propagators, disabled instrumentations) |
| `edge-proxy-config` | Gateway | INFRA | edge proxy route config — where gateway CORS for `traceparent`/`{{CORRELATION_HEADER}}` is checked (07 U7) |

Typical distribution in such an estate: ~15 API-only, ~17 API + messaging/scheduled, 2 pure workers, 3 batch/CronJob, ~9 Node servers, ~30 web UIs, ~26 libraries, and ~200 repositories that are CI/CD, infrastructure, tests or docs — i.e. **less than a third of the repositories receive tracing code**, which is why the table is built before any instrumentation starts.

## 6. Checklist — the table

- [ ] Existing documentation located and read first (catalog, per-service files, architecture overviews); their coverage stated (which organisations / product lines are missing)
- [ ] Code scan run over every directory under `{{ESTATE_ROOT}}`; exclusions listed; disagreements with the docs noted with "code wins"
- [ ] Every repository in exactly one family; multi-module repos annotated per module; libraries marked with what they must carry for their hosts
- [ ] INFRA rows that the guides depend on annotated (agent attachment, collector config, gateway CORS)
- [ ] House examples named in the notes so the assistant copies them instead of inventing (ack-reading producer, receipt-logging consumer, outbox context storage, web SDK bootstrap)
- [ ] Table saved as `{{GUIDES_DIR}}/08-ROLE-TABLE.<estate>.md`, dated, with counts per family; the scan script kept in `{{GUIDES_DIR}}/tools/`
