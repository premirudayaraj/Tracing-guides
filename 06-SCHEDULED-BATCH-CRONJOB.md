# 06 — Scheduled Jobs, Spring Batch and Kubernetes CronJobs: A Root Span per Run

> **Roles:** SCHEDULED (`@Scheduled` business jobs inside a service), BATCH (Spring Batch), CRONJOB (Kubernetes CronJob — Node, Java or Python process that runs to completion), plus backfills/reconciliations triggered by API or message. Token/config refreshers are covered because they exist in almost every service, but they are traced differently (J2). Placeholders (`{{PREFIX}}`, `{{CORRELATION_HEADER}}`, `{{ERR_PREFIX}}`, `{{TRACE_BACKEND}}`, `{{LOG_BACKEND}}`…) are defined in `README.md §0`.
>
> **How to use with an AI assistant:** *"Trace the jobs of `<service>` per 06-SCHEDULED-BATCH-CRONJOB.md: §2 inventory first, then J1–J5."*
>
> Legend: **MUST** = rule; **already** = usually in current code; ➕ = add; ⚠ = confirm at runtime.

---

## 0. Reference patterns (what this is generalised from)

| Kind | Typical occurrence | What existed before tracing |
|---|---|---|
| `@Scheduled` **infrastructure** refreshers | An admin-token refresher (cron from config), an identity-token refresher (fixed interval), a config refresh, an HTTP idle-connection monitor (10 s); a gRPC keep-alive `doNothing()` (`fixedDelay=3_600_000`, empty body) | no spans where the ConfigMap sets `OTEL_INSTRUMENTATION_SPRING_SCHEDULING_ENABLED=false`; default *on* elsewhere → a span per tick for keep-alives |
| `@Async` / API-triggered **backfills** | Flag backfills and account-discovery services (`@Async("threadPoolTaskExecutor")`) | trace continues from the API request (agent); MDC via the MDC-copying executor; no job attributes |
| **Spring Batch** | A monthly-report job (`Application implements CommandLineRunner` → `commandLineJobLauncher.run(job, {startTimeStamp})`; job → tasklet steps; a `JobExecutionListener.afterJob` that publishes `jobExecution.getJobId()` to an APM analytics API; a `StepExecutionListener.afterStep` logging `kv(JOB_NAME…)`, `kv(JOB_ID…)`; exit code 0/1; no tracing dependency), siblings for cleanup, quarterly reports, ticket polling (+ Kafka producer) | APM analytics events + structured logs; no trace, no correlation id |
| **Kubernetes CronJob** (Node) | A monitoring service (ESM bundle, `CronJob */5 * * * *`) that compares every billing account across ~10 systems (CRM, data hub, ERP, master data…), writes PostgreSQL rows, posts a chat message | no tracing library, no run id |
| Deferred-task runner | A shared table-outbox library: `TaskDispatcher` → `RunWithTracingContext` (05 §K8) | stored W3C context restored per task |
| Agent behaviour | `spring-scheduling` instrumentation: INTERNAL span named `Class.method` per `@Scheduled` invocation (when enabled); Spring Batch: no instrumentation; CronJob process: nothing without an SDK | |

---

## 1. The model

```
 ROOT span  "MonthlyReportJob.run"        kind=INTERNAL (no parent)                     ← one per run; ➕ created by code
   attrs: {{PREFIX}}.job.name=monthly-report, {{PREFIX}}.job.trigger=cronjob|scheduled|api|message|manual, {{PREFIX}}.job.run_id=<uuid>, {{PREFIX}}.correlation_id=<same uuid>,
          {{PREFIX}}.job.execution_id=<JobExecution id>, {{PREFIX}}.job.params=startTimeStamp (names only), {{PREFIX}}.job.status=success|failed|partial,
          {{PREFIX}}.job.items_total/processed/failed/skipped, {{PREFIX}}.job.exit_code, feature_flag.* for gates, {{PREFIX}}.job.lock=acquired|held_by_other (ShedLock/DB lock)
   ├─ INTERNAL "step LockDataForCurrentReport"   {{PREFIX}}.job.step=…, {{PREFIX}}.job.step.status=COMPLETED, read/write/skip counts
   │    ├─ JDBC …                                                    (agent)
   │    └─ CLIENT POST peer.service=ticketing                       (agent; 04 rules apply)
   └─ INTERNAL "step EndMonthlyReport"
 Logs: every line carries correlationId=<run id> (MDC set at start, cleared in finally); the APM/chat/DB outputs carry the same run id.
```

Four invariants:
1. **One root span per run** (not per tick of a keep-alive, not per item). A run that does no work still produces a short span with `{{PREFIX}}.job.items_total=0` — "did it run?" is the first support question.
2. **A run id is a correlation id.** Generate a UUID at the start of the run, put it in MDC (`correlationId`), on the span (`{{PREFIX}}.correlation_id` + `{{PREFIX}}.job.run_id`), into every row/message/notification the run produces (chat text, DB columns, Kafka headers via 05 K1).
3. **Counts, not items.** `items_total/processed/failed/skipped` on the root; the agent's CLIENT/JDBC/PRODUCER children show the per-item work. One `@WithSpan` per *phase/step*, never per item.
4. **Infrastructure refreshers are traced as outcomes, not as work**: a token refresh gets one short span with `{{PREFIX}}.auth.*` (04 §4.2) and `{{PREFIX}}.job.status`; keep-alives get nothing.

---

## 2. The analysis you run before writing code (mandatory)

| # | file:method | trigger (`@Scheduled` expr / `CommandLineRunner` / K8s CronJob schedule / `@Async` from API / message) | **business or infrastructure?** (read the body: empty, token refresh, config refresh, connection monitor = infra) | lock/leader election (ShedLock, DB lock, single replica?) | phases/steps and their identifiers (Job/Step names, `JobExecution`/`StepExecution` ids, batch size, item id type) | outputs (rows written, messages published, notifications, files) | error/skip/retry policy (`skip`, `retry`, `faultTolerant`, exit codes) | what is logged today at start/end/failure | gates (feature flag, `@ConditionalOnProperty`, env) | attributes to set |

---

## 3. Placement rules

### J1 — `@Scheduled` business job (inside a service)

```java
@Scheduled(cron = "${reconcile.cron}")
public void reconcile() { JobRun.run("account-reconcile", "scheduled", this::doReconcile); }     // same body callable from an API/message trigger with trigger=api|message

// ➕ <package>/tracing/JobRun.java — one helper per service (no framework)
public final class JobRun {
  private static final Tracer T = GlobalOpenTelemetry.getTracer("{{PREFIX}}.jobs");             // resolves to the agent / in-app OTel SDK; a no-op tracer without either (then only the MDC + log parts below take effect)
  public static void run(String job, String trigger, Runnable body) { run(job, trigger, () -> { body.run(); return null; }); }
  public static <R> R run(String job, String trigger, Supplier<R> body) {
    boolean scheduled = "scheduled".equals(trigger) || "cronjob".equals(trigger);
    String runId = scheduled ? UUID.randomUUID().toString() : firstNonBlank(MDC.get("correlationId"), UUID.randomUUID().toString());   // api/message triggers keep the request's correlation id
    SpanBuilder b = T.spanBuilder(job + ".run").setSpanKind(SpanKind.INTERNAL);
    if (scheduled) b.setNoParent();                                                             // fresh trace per scheduled run; api/message runs stay INSIDE the triggering trace; stored work → a link (05 K8)
    Span span = b.setAttribute("{{PREFIX}}.job.name", job).setAttribute("{{PREFIX}}.job.trigger", trigger).setAttribute("{{PREFIX}}.job.run_id", runId)
                 .setAttribute("{{PREFIX}}.correlation_id", runId).setAttribute("{{PREFIX}}.job.status", "running").startSpan();
    Map<String, String> savedMdc = MDC.getCopyOfContextMap();                                   // restore, never clear, on a request thread
    MDC.put("correlationId", runId); MDC.put("<this service's existing MDC key>", runId); MDC.put("jobRunId", runId);
    try (Scope s = span.makeCurrent(); Scope r = RequestSpan.open(span)) {
      R out = body.get(); span.setAttribute("{{PREFIX}}.job.status", "success"); return out;
    } catch (Exception e) { span.setAttribute("{{PREFIX}}.job.status", "failed"); SpanOutcome.record("{{ERR_PREFIX}}-<module>030", "JOB_FAILED", 500, e); throw e; }   // 03: recordException once + ERROR + {{PREFIX}}.error_code
    finally { span.end(); if (savedMdc == null) MDC.clear(); else MDC.setContextMap(savedMdc); }
  }
}
```
`{{PREFIX}}.job.status` values everywhere: `running | success | failed | partial | skipped_lock`.
* Inside the body: `RequestSpan.set("{{PREFIX}}.job.items_total", n)`, `…processed`, `…failed`, `…skipped`, `{{PREFIX}}.job.lock=acquired|held_by_other` (and return early with `{{PREFIX}}.job.status=skipped_lock`), gates as `feature_flag.*`, batching as `{{PREFIX}}.batch_size`, and every decision/fallback per 01 §5; outbound calls per 04; publishes per 05 K1.
* When triggered by an API request (`{{PREFIX}}.job.trigger=api`) the body runs **under the request's trace** (the helper does not call `setNoParent` for api/message triggers) and the request span carries `{{PREFIX}}.job.name` + `{{PREFIX}}.job.run_id` (`RequestSpan.set` before calling `JobRun.run`); when it is `@Async`, the agent keeps the parent and MDC comes from the MDC-copying executor (05 K4).
* Set the agent's `spring-scheduling` instrumentation **off** everywhere (`-Dotel.instrumentation.spring-scheduling.enabled=false` / `OTEL_INSTRUMENTATION_SPRING_SCHEDULING_ENABLED=false`) — with it on, every `@Scheduled` tick (keep-alives, refreshers) becomes an empty trace, and business jobs would get **two** root spans (the agent's `Class.method` and `JobRun`'s). `JobRun` is the only root for jobs.

### J2 — Infrastructure refreshers (tokens, config, caches)

One short span per execution **only if** it performs I/O whose failure matters (token endpoint, config server): `JobRun.run("identity-token-refresh", "scheduled", …)` with `{{PREFIX}}.auth.provider`, `{{PREFIX}}.auth.result`, `{{PREFIX}}.auth.token_ttl_s`, `{{PREFIX}}.job.status` (04 §4.2). Failure → ERROR + `{{PREFIX}}.error_code` so "every request is 401 since 03:00" has a cause in `{{TRACE_BACKEND}}`. Keep-alives (`doNothing`), idle-connection monitors: no span.

### J3 — Backfills / reconciliations (API- or message-triggered, long-running)

Same `JobRun` helper with `trigger=api|message`; `@WithSpan("Backfill.phase")` per phase; counts on the root; a stored progress row carries the run id; if the work is queued for later, store the W3C context and the run id with the row (05 K8) and dispatch with a **link** to the triggering request.

### J4 — Spring Batch

Register at the Spring Batch boundaries, using the ids Batch already has:
```java
// JobExecutionListener  (extend the existing job listener; keep any APM/analytics publish, add the span)
public void beforeJob(JobExecution je) {
  Span root = T.spanBuilder(je.getJobInstance().getJobName() + ".run").setSpanKind(SpanKind.INTERNAL).setNoParent()
     .setAttribute("{{PREFIX}}.job.name", je.getJobInstance().getJobName()).setAttribute("{{PREFIX}}.job.trigger", "cronjob")
     .setAttribute("{{PREFIX}}.job.execution_id", je.getId()).setAttribute("{{PREFIX}}.job.instance_id", je.getJobInstance().getInstanceId())
     .setAttribute("{{PREFIX}}.job.run_id", runId).setAttribute("{{PREFIX}}.correlation_id", runId)
     .setAttribute("{{PREFIX}}.job.params", String.join(",", je.getJobParameters().getParameters().keySet()))     // names only, never values that could be PII
     .startSpan();
  RUNS.put(je.getId(), new Run(root, root.makeCurrent()));                                          // static ConcurrentHashMap<Long, Run> in the listener — never the persisted ExecutionContext
  MDC.put("correlationId", runId); MDC.put("jobRunId", runId);
}
public void afterJob(JobExecution je) {
  Run run = RUNS.remove(je.getId()); Span root = run.span();
  root.setAttribute("{{PREFIX}}.job.batch_status", je.getStatus().name());                          // raw Spring Batch status
  root.setAttribute("{{PREFIX}}.job.status", switch (je.getStatus()) { case COMPLETED -> "success"; case FAILED, ABANDONED -> "failed"; case STOPPED, STOPPING -> "partial"; default -> "running"; });
  root.setAttribute("{{PREFIX}}.job.exit_code", je.getExitStatus().getExitCode());
  je.getStepExecutions().forEach(se -> { /* totals */ }); root.setAttribute("{{PREFIX}}.job.items_processed", totalWrite); root.setAttribute("{{PREFIX}}.job.items_skipped", totalSkip); root.setAttribute("{{PREFIX}}.job.items_failed", totalRollback);
  if (je.getStatus().isUnsuccessful()) { root.setStatus(StatusCode.ERROR, je.getExitStatus().getExitCode()); je.getAllFailureExceptions().stream().findFirst().ifPresent(root::recordException); }
  run.scope().close(); root.end(); MDC.clear();                                                      // batch JVM: no request thread to restore
}
// StepExecutionListener (extend the existing step listener): one child span per step
beforeStep: step = T.spanBuilder("step " + se.getStepName()).setAttribute("{{PREFIX}}.job.step", se.getStepName()).setAttribute("{{PREFIX}}.job.step.execution_id", se.getId()).startSpan(); stepScope = step.makeCurrent();
afterStep:  step.setAttribute("{{PREFIX}}.job.step.status", se.getStatus().name()); step.setAttribute("{{PREFIX}}.job.step.read", se.getReadCount()); …write, skip (read/process/write), rollback, commit counts; ERROR + recordException on failure; stepScope.close(); step.end();
```
* Chunk-oriented steps: no span per chunk or item; `ChunkListener` only to update counts periodically for long steps. Tasklets: the step span is enough.
* `CommandLineRunner` jobs exit the JVM: ➕ flush the exporter before exit (`OpenTelemetrySdk.getSdkTracerProvider().forceFlush()` when using the SDK; with the agent, allow the shutdown hook to run — do not `System.exit` before `JobExecution` end + a short wait). ⚠ verify the last span reaches `{{TRACE_BACKEND}}`.
* **Batch jobs often have no agent**: `GlobalOpenTelemetry` is a no-op there until the deployment adds the agent (`OTEL_ENABLED=true` / base image with the agent) — the run id in MDC, in any APM summary strings and in every log line is what the change delivers immediately; the spans appear when the agent is attached, with no code change.
* An existing APM/analytics publish stays (it is the alerting path today); add the run id to its summary strings so the two systems join.

### J5 — Kubernetes CronJob processes (Node / Python / Java) without an agent

* ➕ Minimum (no SDK): generate a run id at start; put it into every log line (`pino`/`winston` child logger), into the chat message text, into every PostgreSQL row the run writes (`run_id` column), and forward it as `{{CORRELATION_HEADER}}` on every HTTP call the run makes — the run becomes searchable in `{{LOG_BACKEND}}` and joinable to the services it called.
* ➕ Full: OTel SDK for the language (`@opentelemetry/sdk-node` + `@opentelemetry/auto-instrumentations-node`; `opentelemetry-sdk` + `opentelemetry-instrument` for Python; the Java agent for JVM jobs) with `OTEL_EXPORTER_OTLP_ENDPOINT={{COLLECTOR_ENDPOINT}}`, `OTEL_SERVICE_NAME=<service>`, `OTEL_RESOURCE_ATTRIBUTES=service.namespace=<domain>,deployment.environment=<env>`; one root span per run (`{{PREFIX}}.job.*` as in J1) around `main()`, auto-spans for HTTP/pg underneath, `peer.service` mapping for every system it compares (04 §3), and **`sdk.shutdown()` before exit** so the last spans are flushed (a CronJob pod is killed right after `main` returns).
* Per-system comparison results are decisions: `{{PREFIX}}.check.<system>=match|mismatch|unreachable` (one key per system, low-cardinality) and `{{PREFIX}}.job.items_failed` = number of accounts with any mismatch; the chat message is a *notification* of the span, not a substitute.

---

## 4. What must not be done

* No span per tick for keep-alives, idle-connection monitors, `doNothing()`.
* No span per item/row/account inside a run; no unbounded attribute keys (`{{PREFIX}}.account_<n>`).
* No job parameters that carry PII as attribute values (names only).
* No new correlation id **inside** a run for each item; the run id is the correlation id for everything the run produces.

---

## 5. Verification (TraceQL / LogQL)

```
{ name =~ ".*\\.run" && kind = internal } | by(span.{{PREFIX}}.job.name, span.{{PREFIX}}.job.status)          -- every job, every outcome
{ span.{{PREFIX}}.job.name = "monthly-report" } | select(span.{{PREFIX}}.job.execution_id, span.{{PREFIX}}.job.items_processed, span.{{PREFIX}}.job.exit_code)
{ span.{{PREFIX}}.job.name = "account-reconcile" && span.{{PREFIX}}.job.status = "skipped_lock" }             -- lock contention
{ span.{{PREFIX}}.job.run_id = "<run>" }                                                                     -- the run and all its CLIENT/JDBC/PRODUCER children
{ resource.service.name = "<monitoring-service>" && span.{{PREFIX}}.check.erp = "unreachable" }
{ name = "identity-token-refresh.run" && status = error }
LogQL: {k8s_container_name="<monitoring-service>"} | json | correlationId="<run>"      -- label per your log shipper (03 §6)
```

---

## 6. Checklist — one job

- [ ] §2 inventory row: trigger, business vs infrastructure, lock, steps/ids, outputs, error policy, current logs, gates
- [ ] Business job: `JobRun` root span per run (fresh trace for scheduled/cronjob, child for api/message) with `{{PREFIX}}.job.name/trigger/run_id`, `{{PREFIX}}.correlation_id` = run id in MDC (MDC restored, not cleared, on request threads), status + counts at the end, `SpanOutcome.record` on failure; same body for every trigger
- [ ] Refreshers: short span with `{{PREFIX}}.auth.*` + status; keep-alives untraced; agent `spring-scheduling` instrumentation off
- [ ] Spring Batch: job listener root span (`execution_id`, `instance_id`, params names, exit code, totals), step listener child spans with read/write/skip counts; exporter flushed before JVM exit
- [ ] CronJob process: run id in logs/rows/messages/outbound headers; OTel SDK with `sdk.shutdown()`; per-system check results as attributes
- [ ] Rows/messages/notifications produced by the run carry the run id; publishes follow 05 K1; outbound calls 04
- [ ] Verified: one `<job>.run` span per run in `{{TRACE_BACKEND}}` with counts; `{ span.{{PREFIX}}.job.run_id = "<run>" }` shows the children; `{{LOG_BACKEND}}` finds the run's lines by `correlationId`
