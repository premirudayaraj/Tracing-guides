# 05 — Kafka, RabbitMQ and Async: Producer Acknowledgements, Consumer Receipts, One Trace Across the Hop

> **Roles:** PRODUCER, CONSUMER (Kafka, RabbitMQ, Spring Cloud Stream), ASYNC/fan-out, OUTBOX/deferred work. Scheduled/batch/CronJob roles are in `06`. Placeholders (`{{PREFIX}}`, `{{CORRELATION_HEADER}}`, `{{ERR_PREFIX}}`, `{{TRACE_BACKEND}}`…) are defined in `README.md §0`.
>
> **How to use with an AI assistant:** *"Trace the producers/consumers of `<service>` per 05-KAFKA-RABBIT-AND-ASYNC.md: first the §2 inventory, then K1–K8."*
>
> Legend: **MUST** = rule; **already** = usually in current code; ➕ = add; ⚠ = confirm at runtime.

---

## 0. Reference patterns (what this is generalised from)

| Pattern | Where it is typically found |
|---|---|
| Kafka consumers, single record, **fresh correlation id per message** | An account-platform API with two listeners: one on an *account-change* topic (schema-registry JSON, `autoStartup=false` behind a feature flag, reads `partition/offset/key` + `@Header(RECEIVED_TOPIC)` and two business ids), one on an *account-event* topic (reads only `record.topic()`; message `id`, `type`, `eventStatus`, first account number, category; writes an audit row; counters `EVENT_RECEIVED` / `EVENT_IGNORED{reason}`). Both put a **new UUID** into MDC per message. |
| Consumer error handlers that **swallow** | The same service's two `CommonErrorHandler`s (`handleOne` logs + returns `true` = skip; deserialization error → seek past `offset+1`); one of them logs the **payload** |
| Consumer receipt logging with broker metadata | A connection-provisioning service: three listeners log `"Start processing event, key: …, offset: …, partition: … by groupId: …, clientId: …"`; a `RecordFilterStrategy` discards records whose `$.event.source` is not the expected producer; routes by `$.event.type`; a `NonRetryableException` → shared error handler + DLT; every container has `isObservationEnabled=true` |
| Consumer base classes in libraries | A shared library `KafkaEventConsumer<T>.consume(ConsumerRecord, offset)` (logs topic/offset/event); an onboarding service with `UserEventConsumer(ConsumerRecord, @Header(OFFSET))`; a `kafka-support` module with `DefaultErrorHandler + FixedBackOff`; retry topics through `RetryTopicConfigurationSupport` (`-RETRY`, `-DLT`, `maxAttempts=1`, `autoStartDltHandler(false)`) |
| Producers, fire-and-forget with retry, **ack not read** | `KafkaSender.sendToKafka` (payload `{referenceId, accountId, accountNumber}`, **no key, no headers**, `@Retryable` 3× with backoff, `@Recover` logs "retry exhausted"); a billing-account creation sender whose payload has `{id, traceparent=<the MDC correlation id, not a W3C traceparent>, source, type, eventstatus, data.*}` |
| Producer that **reads the ack** | An audit-log service: `SendResult r = kafkaTemplate.send(topic, event).get(); flush(); log "Kafka event sent. Topic: {}, offset: {}"` (offset only; logs the full value) |
| Producer interceptor that **registers the ack** | The connection-provisioning service's `ProducerInterceptor`: `onSend` INFO (topic, headers, payload), `onAcknowledgement(RecordMetadata, Exception)` INFO on ack / ERROR on exception; senders use `send(...).get(timeout)`; every record carries headers `event.id`, `event.type`, `event.subtype`, `event.version`, `event.schemaId` and a key |
| Producer inside a library | An order API → a *notifications* library `sendEmail(EmailMessage{correlationId, userId, eventName, sendTo, payload…})` → topics `EMAIL_<suffix>`; all exceptions caught and logged; **no result returned** |
| RabbitMQ in the same services | The account-platform API: several `@RabbitListener` consumers and one publisher; other services use `StreamBridge` with an `x-correlation-id` header (**new UUID**) via a `MessageHeaders{providerEnv, appId, messageId, correlationId, env}` record; **one ConfigMap disables the agent's `spring-rabbit` instrumentation** |
| Correlation id inside the payload | `traceparent` field that is really the correlation id; `correlationId` in the notifications payload; `event.correlationId` + Micrometer baggage remote fields (`Correlation-Id`/`CorrelationId`); `MessageHeaders.correlationId` |
| Trace context stored with deferred work | A shared *table-outbox* library: `TaskEntity implements TracingContextHolder` (W3C context map as a JSON column), `TracingContextExtractor` (`GlobalOpenTelemetry…inject`), `RunWithTracingContext.runWith(...)` (extracts → SERVER span → runs the task) |
| Async hops | An MDC-copying `ThreadPoolTaskExecutor` + an interceptor that sets async attributes; `@Async` backfills; `CompletableFuture.runAsync` on the **common pool** (e-mail dispatch — MDC lost); an MDC `TaskDecorator`; a Reactor MDC hook |
| Agent behaviour | PRODUCER span `<topic> publish` (`messaging.system=kafka`, `messaging.destination.name`, `messaging.kafka.message.key`, `messaging.destination.partition.id` + `messaging.kafka.offset` from `RecordMetadata` when the send completes ⚠), `traceparent` injected into record headers; CONSUMER span `<topic> process` per record (`messaging.consumer.group.name`, offset, partition, key), parented/linked to the producer; batch listener = one span with links; `<topic> receive` only with `otel.instrumentation.messaging.experimental.receive-telemetry.enabled=true`; RabbitMQ `amqp-client` instrumentation independent of the `spring-rabbit` one ⚠ |

Estate-wide findings that shaped the rules: **no repo wrote `{{CORRELATION_HEADER}}` into Kafka record headers** (only payload fields); one repo used a `ProducerInterceptor`; two looked at the acknowledgement; consumers regenerated the correlation id; swallowed consumer failures were green spans.

---

## 1. The model

```
 producer service                                                   consumer service
 SERVER/INTERNAL span (request or job)                              CONSUMER span  "<topic> process"                (agent)
   attrs: {{PREFIX}}.event.type=ACCOUNT_CREATED  {{PREFIX}}.account_number=…   attrs (agent): messaging.system, messaging.destination.name, messaging.destination.partition.id,
   └─ PRODUCER span "<topic> publish"       (agent)                          messaging.kafka.offset, messaging.kafka.message.key, messaging.consumer.group.name
        record headers:  traceparent   ← agent                     attrs (K2): messaging.message.id (payload/header id), messaging.message.conversation_id=<C>,
                         {{CORRELATION_HEADER}}=<C>   ← K1 interceptor       {{PREFIX}}.correlation_id=<C>, {{PREFIX}}.correlation_id_generated=false, {{PREFIX}}.event.type,
                         event.id=<msg id>      ← K1                         {{PREFIX}}.event.status, {{PREFIX}}.consumer.action=processed|skipped|seek_past|dlt|retry, {{PREFIX}}.skip_reason
        attrs (agent, on ack): messaging.destination.partition.id, messaging.kafka.offset            └─ CLIENT spans → other services (04), JDBC
        attrs (K1): messaging.message.id, messaging.message.conversation_id, {{PREFIX}}.publish.attempt
   (parent, K1 whenComplete on the captured span) {{PREFIX}}.publish.result=acked|failed|exhausted, {{PREFIX}}.publish.partition, {{PREFIX}}.publish.offset; {{PREFIX}}.error_code on exhaustion (03)
```

Five invariants:
1. **The trace continues across the hop** (agent `traceparent` header; K1 injects it explicitly where the agent may be off). Never start a new trace in a listener; never `setNoParent()`.
2. **The correlation id travels as a record header** (`{{CORRELATION_HEADER}}`) *and* stays in the payload where a contract already has it; the consumer **restores** it into MDC and onto the span, generating a new one **only when none arrived** — and says so (`{{PREFIX}}.correlation_id_generated=true`).
3. **The acknowledgement is registered.** A send is not done until the broker answers: the agent puts partition + offset on the PRODUCER span when the ack arrives; the code registers the outcome (`acked | failed | exhausted`) plus partition/offset/message id **on the span it captured before the send** (`whenComplete`), and writes one log line with all four ids. Fire-and-forget is allowed only with that callback; if the parent span may already have ended when the ack arrives (request returned before the broker answered), the log line and the metric are the record — say so in the code comment.
4. **The receipt is registered.** A consumer registers what it is processing — message id, key, partition, offset, consumer group, event type — *before* doing anything, so a crash mid-way still leaves a searchable span.
5. **Skipped and swallowed are outcomes.** A filtered-out record, a poison message skipped by the error handler, a DLT publish — each is an attribute (`{{PREFIX}}.consumer.action`, `{{PREFIX}}.skip_reason`) and, when it is a failure, an error on the CONSUMER span.

---

## 2. The analysis you run before writing code (mandatory)

**Producers** — for every `send`/`publish`/`StreamBridge.send`/library call:
| # | file:method | system (kafka/rabbit) | topic / exchange+routing key (config key) | key set? | headers set? (which) | message id field in payload/header | correlation id (header? payload field?) | sync (`.get`) or async (callback / fire-and-forget)? | retry (`@Retryable`, attempts/backoff), `@Recover` | is `RecordMetadata`/`SendResult`/confirm read? | attributes to set |

**Consumers** — for every `@KafkaListener` / `@RabbitListener` / stream `Consumer`:
| # | file:method | topic/queue (config key) | group | container factory (`isObservationEnabled`, batch?, ack mode, `MAX_POLL_RECORDS`) | payload type + **id field(s)** | filters that skip (`RecordFilterStrategy`, `if (...) return`) and their reasons | routing by event type | error handler (`CommonErrorHandler`, `DefaultErrorHandler`, retry topics, DLT, seek-past) — does it **swallow**? | downstream calls / DB writes / metrics / audit rows | MDC/correlation handling today | gates (`autoStartup`, feature flag, `@ConditionalOnProperty`) | attributes to set |

Also list **every thread hop** (`@Async`, `runAsync`, executors, Reactor schedulers) and **every deferred-work table** (outbox, retry tables, backfill queues).

---

## 3. Placement rules

### K1 — Producer: header, key, message id, **acknowledgement**

Two pieces, both once per service, never per call site:

**(a) `ProducerInterceptor` — headers + message id** (registered on every `ProducerFactory`; where a service already has a producer interceptor, extend it rather than adding a second one):
```java
public class TracingProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {
  @Override public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {                  // runs on the calling thread, before the agent's PRODUCER span is created
    String cid = firstNonBlank(MDC.get("correlationId"), MDC.get("<this service's existing MDC key>"));
    if (cid != null && record.headers().lastHeader("{{CORRELATION_HEADER}}") == null) record.headers().add("{{CORRELATION_HEADER}}", cid.getBytes(UTF_8));
    if (record.headers().lastHeader("event.id") == null) record.headers().add("event.id", UUID.randomUUID().toString().getBytes(UTF_8));   // message id when the payload has none
    if (record.headers().lastHeader("traceparent") == null)                                    // the agent injects this on the wire; explicit injection is a no-op without an SDK and harmless with one
      GlobalOpenTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), record.headers(), (h, k, v) -> h.add(k, v.getBytes(UTF_8)));
    Span caller = Span.current();                                                                // the CALLER's span (request/job/@WithSpan) — the PRODUCER span does not exist yet
    caller.setAttribute("messaging.message.conversation_id", cid);
    caller.setAttribute("messaging.message.id", messageIdOf(record));                          // header event.id / payload id
    return record;
  }
  @Override public void onAcknowledgement(RecordMetadata m, Exception e) {                    // producer I/O thread: NO span is current here and the record is not available — log only
    if (e == null) log.info("kafka ack topic={} partition={} offset={}", m.topic(), m.partition(), m.offset());      // correlation id / message id are not reachable here; the whenComplete below has them
    else log.error("kafka send failed topic={} partition={}", m.topic(), m.partition(), e);
  }
}
```
**(b) the send site — register the acknowledgement on the span captured before the send** (one helper used by every sender):
```java
public static CompletableFuture<SendResult<K, V>> sendTraced(KafkaTemplate<K, V> t, ProducerRecord<K, V> r, String businessId) {
  Span caller = Span.current();                                                               // captured on the calling thread; may be the request span (ends when the request returns) or a job span
  caller.setAttribute("{{PREFIX}}.publish.attempt", attempt());                               // RetrySynchronizationManager.getContext()+1 under @Retryable, else 1
  caller.setAttribute("{{PREFIX}}.publish.result", "pending");                                // default — stays "pending" if the ack never arrives before the span ends: that is a finding, not a bug
  return t.send(r).whenComplete((res, ex) -> {                                                // runs on the producer callback thread; the captured span is still writable until it ends
    String mid = headerOf(r, "event.id");  String cid = headerOf(r, "{{CORRELATION_HEADER}}");
    if (ex == null) { RecordMetadata m = res.getRecordMetadata();
      caller.setAttribute("{{PREFIX}}.publish.result", "acked"); caller.setAttribute("{{PREFIX}}.publish.partition", (long) m.partition()); caller.setAttribute("{{PREFIX}}.publish.offset", m.offset());
      log.info("kafka acked topic={} partition={} offset={} messageId={} correlationId={} id={}", m.topic(), m.partition(), m.offset(), mid, cid, businessId);   // the record of the ack, agent on or off
    } else { caller.setAttribute("{{PREFIX}}.publish.result", "failed"); caller.setAttribute("error.type", ex.getClass().getName());
      log.error("kafka send failed topic={} messageId={} correlationId={} id={}", r.topic(), mid, cid, businessId, ex); }               // exception recorded by the exception handler / @Recover (03)
  });
}
// synchronous senders (send(...).get(timeout)): same attributes from the returned SendResult.getRecordMetadata(), on Span.current()
```
The agent's PRODUCER span already gets `messaging.destination.partition.id` / `messaging.kafka.offset` from the ack; (b) makes the **outcome** searchable on the request/job span and writes the one log line that carries topic, partition, offset, message id and correlation id together.
Rules:
* MUST register the **acknowledgement** through (b): `{{PREFIX}}.publish.result`, `{{PREFIX}}.publish.partition`, `{{PREFIX}}.publish.offset` on the captured span and the ack log line (Kafka), or the publisher-confirm result (RabbitMQ `ConfirmCallback` → `{{PREFIX}}.publish.result=acked|nacked` + `messaging.rabbitmq.destination.routing_key`). Where the code already waits (`send(...).get(timeout)`) read `SendResult.getRecordMetadata()` and set the same attributes on `Span.current()`.
* MUST give every record a **message id**: use the payload's id when the contract has one (an `id` field, a `correlationId`+`eventName` pair), otherwise a header `event.id` (UUID) — and register it as `messaging.message.id` on the PRODUCER span (and the consumer does the same on receipt → the two ends join by id even when the trace is broken).
* ➕ MUST send with a **key** (the business id: account number, reference id) — a sender that calls `send(topic, payload)` unkeyed loses both searchability (`messaging.kafka.message.key`) and per-entity ordering.
* Retry (`@Retryable`/`@Recover`): each attempt is its own PRODUCER span (agent). In `@Recover` register `{{PREFIX}}.publish.result=exhausted`, `{{PREFIX}}.retry.attempts` on the root and call `SpanOutcome.recordHandled("{{ERR_PREFIX}}-<module>47x", "<TOPIC>_PUBLISH_FAILED", e)` (03 §2; 02 §4 band 47x = messaging) — the request may still succeed, but the lost event is now searchable. Without this, a "retry exhausted" log line is the only trace of a lost event.
* Fire-and-forget (`send()` without `.get()`/callback) is allowed only through `sendTraced` (b); otherwise the span ends before the broker answers and a failure is invisible. When the request returns before the ack, `{{PREFIX}}.publish.result` stays `pending` on that span — the ack log line (with message id + correlation id) is then the record; `{ span.{{PREFIX}}.publish.result = "pending" }` tells you where the send is fire-and-forget.
* On the **caller's span** register what was published before the send: `{{PREFIX}}.event.type`, the business id (`{{PREFIX}}.account_number` / `{{PREFIX}}.reference_id`), `messaging.message.id`; the callback (b) adds the outcome.
* Library producers (a shared notifications/events library): the library must (a) carry the interceptor, (b) return the ack (`topic, partition, offset, messageId`) or expose a callback so the host can register `{{PREFIX}}.publish.result` — a `void sendX(...)` that swallows gives the host nothing; until the library changes, the host registers `{{PREFIX}}.publish.result=unknown` and `{{PREFIX}}.handled=swallowed` (04 §4.3) after the call.
* Never put payloads, SASL/cloud/schema-registry settings or credentials on spans or in the ack log (an `onSend` that logs the payload is trimmed to ids).

### K2 — Consumer: receipt first, restore the correlation id, register the message id

*Once per container factory* — a `RecordInterceptor` (Kafka) / `MessagePostProcessor`+`@RabbitListener` advice (Rabbit), set with `factory.setRecordInterceptor(...)` on **every** `ConcurrentKafkaListenerContainerFactory` the service (and its shared libraries) defines:

```java
public class TracingRecordInterceptor<K, V> implements RecordInterceptor<K, V> {
  @Override public ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> r, Consumer<K, V> c) {
    Header h = r.headers().lastHeader("{{CORRELATION_HEADER}}");
    String cid = h != null ? new String(h.value(), UTF_8) : firstNonBlank(correlationIdInPayload(r), UUID.randomUUID().toString());   // header → payload field → generate (last resort)
    MDC.put("correlationId", cid); MDC.put("<this service's existing MDC key>", cid);                   // the common key + this service's existing key (01 §3)
    Span s = Span.current();                                                                            // the agent's CONSUMER span "<topic> process"
    s.setAttribute("{{PREFIX}}.correlation_id", cid);
    s.setAttribute("{{PREFIX}}.correlation_id_generated", h == null && correlationIdInPayload(r) == null);
    s.setAttribute("messaging.message.conversation_id", cid);
    s.setAttribute("messaging.message.id", messageIdOf(r));                                             // header event.id or payload id
    s.setAttribute("messaging.kafka.offset", r.offset()); s.setAttribute("messaging.destination.partition.id", String.valueOf(r.partition()));   // agent sets these too — harmless, and present when the agent is off
    s.setAttribute("messaging.kafka.message.key", String.valueOf(r.key()));
    s.setAttribute("messaging.consumer.group.name", c.groupMetadata().groupId()); s.setAttribute("messaging.client.id", clientIdOf(c));
    s.setAttribute("{{PREFIX}}.consumer.action", "received");                                           // default; the listener overwrites (processed | skipped | retry | dlt | rethrown)
    log.info("receipt topic={} partition={} offset={} key={} group={} messageId={} correlationId={}", …);  // the receipt line, without the payload
    return r;
  }
  @Override public void afterRecord(ConsumerRecord<K, V> r, Consumer<K, V> c) { MDC.remove("correlationId"); MDC.remove("<this service's existing MDC key>"); }
}
```
Then the **listener body** registers only business facts and decisions:
```java
s.setAttribute("{{PREFIX}}.event.type", msg.getType()); s.setAttribute("{{PREFIX}}.event.status", msg.getEventStatus()); s.setAttribute("{{PREFIX}}.event.source", src);
s.setAttribute("{{PREFIX}}.account_number", first(msg.accountNumbers())); s.setAttribute("{{PREFIX}}.master_data_id", msg.getMasterId());
if (!isBilling(msg)) { s.setAttribute("{{PREFIX}}.consumer.action", "skipped"); s.setAttribute("{{PREFIX}}.skip_reason", "NOT_BILLING_ACCOUNT"); return; }   // every early return
s.setAttribute("{{PREFIX}}.audit_id", auditRow.getId());                                                // the row the consumer wrote
s.setAttribute("{{PREFIX}}.route", "completeStagedWorkflow");                                            // routing by event type
```
Rules:
* MUST NOT generate a fresh UUID per message when a header/payload correlation id exists; MUST flag generation.
* MUST register the receipt (ids above) **before** parsing/processing — the "Start processing event, key/offset/partition/group/clientId" receipt line is the model; a listener that logs only the message `id` is not enough.
* MUST register every filter/early-return as `{{PREFIX}}.consumer.action=skipped` + `{{PREFIX}}.skip_reason=<enum>` (ignore reasons, null payload, source mismatch in a `RecordFilterStrategy`). Without this they are invisible successes (where an `EVENT_IGNORED{reason}` metric exists — keep it; the span attribute is what joins it to the producer).
* Gates: a listener that is `autoStartup=false` behind a feature flag registers nothing (it does not run). Register the flag where the container is started/stopped (the flag listener / `KafkaListenerEndpointRegistry` call): one INFO line `listener=<id> topic=<t> flag=<key> state=started|stopped` and `feature_flag.key/result.variant` (00 §6) on the job/request span that toggled it; alert on "no consumer spans for topic X in N minutes".
* Batch listeners: one CONSUMER span per batch (agent, with links) + `messaging.batch.message_count`, `{{PREFIX}}.job.items_processed/failed`; no span per record.
* Downstream calls from the listener get the correlation id from MDC through the existing client interceptors (04) — K2 is what makes that header correct.

### K3 — Consumer error handling: swallowed is still recorded

The container's error handler (`CommonErrorHandler.handleOne`, `DefaultErrorHandler`, retry-topic recoverer) runs **after** the listener returned/threw, when the agent has already ended the CONSUMER span ⚠ (the `RecordInterceptor.failure` hook is where the agent's instrumentation ends it). So the outcome is registered **in the listener**, in a `try/catch` that rethrows, and the error handler only logs:
```java
@KafkaListener(...) public void receive(ConsumerRecord<String, AccountMessage> r) {
  Span s = Span.current();                                             // the CONSUMER span
  try { process(r); s.setAttribute("{{PREFIX}}.consumer.action", "processed"); }
  catch (NonRetryableException e) { s.setAttribute("{{PREFIX}}.consumer.action", "skipped"); SpanOutcome.record("{{ERR_PREFIX}}-<module>47x", "EVENT_INVALID", 500, e); throw e; }   // 03: recordException once, ERROR, {{PREFIX}}.error_code
  catch (Exception e)             { s.setAttribute("{{PREFIX}}.consumer.action", willRetry(e) ? "retry" : "dlt"); s.setAttribute("{{PREFIX}}.retry.attempts", attemptOf(r)); SpanOutcome.record("{{ERR_PREFIX}}-<module>47x", "EVENT_PROCESSING_FAILED", 500, e); throw e; }
}
// error handler (handleOne etc.): log topic/partition/offset/key/message id + exception, decide skip/seek/retry — no span code
```
* Values of `{{PREFIX}}.consumer.action`: `processed | skipped (filtered or poison, handler returns true) | retry (backoff / retry topic) | dlt (published to -DLT) | rethrown`. Deserialization failures (`seek past offset+1`) never reach the listener and have **no CONSUMER span** — they are a log line (topic/partition/offset) + a metric (`kafka_deserialization_failures_total{topic}`), not an attribute.
* A DLT/retry-topic publish is a PRODUCER span child of the failing CONSUMER span (K1 carries the correlation id and message id onto the DLT record automatically) → the trace shows where the message went.
* Never log the record value in the handler: topic/partition/offset/key/message id + exception only.

### K4 — Thread hops: the trace is the agent's, MDC is yours

* MUST run traced work on the MDC-aware executors the service already has (an MDC-copying `ThreadPoolTaskExecutor`, an MDC `TaskDecorator`, a Reactor MDC hook); MUST NOT use the common pool (`CompletableFuture.runAsync(task)`) — the fix is `runAsync(task, mdcAwareExecutor)`.
* No extra span for the hop; `@WithSpan("Class.method")` on the async method if its duration matters. Root-span decisions from the worker go through `RequestSpan.set` (01 §6).
* Never touch request-scoped beans off the request thread — pass values.

### K5 — Fan-out inside a consumer or a backfill

One CONSUMER span (or `@WithSpan("Backfill.run")` root) + counts (`{{PREFIX}}.batch_size`, `{{PREFIX}}.job.items_processed`, `{{PREFIX}}.job.items_failed`) + the agent's CLIENT/JDBC children. Never a span per account.

### K6 — RabbitMQ specifics

* Semantics: `messaging.system=rabbitmq`, `messaging.destination.name={exchange}:{routing key}` (producer) / `{exchange}:{routing key}:{queue}` (consumer), `messaging.rabbitmq.destination.routing_key`, `messaging.rabbitmq.message.delivery_tag`; operation names `publish`/`process`.
* Check whether a deployment ConfigMap disables the agent's `spring-rabbit` instrumentation (`OTEL_INSTRUMENTATION_SPRING_RABBIT_ENABLED=false`); the lower-level `rabbitmq` (amqp-client) instrumentation may still create spans ⚠ — check one `{ span.messaging.system = "rabbitmq" }` in `{{TRACE_BACKEND}}`. If nothing appears: ➕ enable `spring-rabbit`, or apply K1/K2 through a `MessagePostProcessor` (publish: `{{CORRELATION_HEADER}}` + `message_id` properties; `ConfirmCallback` → `{{PREFIX}}.publish.result`) and a `@RabbitListener` advice (`MessageProperties.getHeader("{{CORRELATION_HEADER}}")`, `getMessageId()`, `getDeliveryTag()`).
* `StreamBridge` producers: keep the existing `x-correlation-id` header but source it from MDC (K1), not a new UUID; register `messageId` from `MessageHeaders` as `messaging.message.id`; Spring Cloud Stream `send` returns a boolean, so the outcome is `{{PREFIX}}.publish.result=accepted|rejected` (no broker offset).

### K7 — Kafka clients outside the JVM (Node `kafkajs`, Python)

Same attributes and headers; OTel Node SDK + `@opentelemetry/instrumentation-kafkajs` (producer/consumer spans, `traceparent` headers) and a `send` callback that registers `partition/offset` from the broker response.

### K8 — Stored-then-dispatched work (outbox, retry tables, backfill queues)

* Pattern (from the shared table-outbox library): inject the W3C context into a JSON column when scheduling (`fillTracingContext`), extract and open a span under the original trace on dispatch (`RunWithTracingContext`). ➕ Reuse it for backfill/retry tables and any deferred e-mail; store `{{CORRELATION_HEADER}}` in its **own column** next to the context map; register `{{PREFIX}}.job.items_*` on the dispatch span.
* Dispatch hours later → prefer a **link** (`spanBuilder(...).addLink(extracted)`) over a child so the original trace's duration is not stretched; say which per service.

---

## 4. Configuration touch-points

| Concern | Where |
|---|---|
| Producer interceptor (K1) | `ProducerConfig.INTERCEPTOR_CLASSES_CONFIG` in every producer config (`kafkaTemplate()` beans, shared `KafkaConfigHelper`-style classes, an existing producer interceptor extended, library producers) |
| Record interceptor (K2) | every `ConcurrentKafkaListenerContainerFactory` (per-topic configs, `kafka-support` modules, shared library factories) |
| Error handlers (K3) | every `CommonErrorHandler` / `DefaultErrorHandler` / retry-topic DLT configuration |
| Executors (K4) | thread configs, `TaskDecorator`s, `AsyncConfiguration` |
| Agent | `OTEL_INSTRUMENTATION_SPRING_RABBIT_ENABLED`, `otel.instrumentation.messaging.experimental.receive-telemetry.enabled` (leave off unless poll latency matters), `otel.instrumentation.kafka.experimental-span-attributes` (optional queue-time) |
| Never traced | `*.kafka.auth.*`, `*.kafka.aws.*` / cloud credentials, SASL/JAAS, schema-registry settings |

---

## 5. Verification (TraceQL)

```
{ span.{{PREFIX}}.publish.result != nil } | by(span.{{PREFIX}}.event.type, span.{{PREFIX}}.publish.result)     -- acked / failed / exhausted / pending per event type (on the caller's span)
{ span.{{PREFIX}}.publish.result = "exhausted" } | select(span.{{PREFIX}}.reference_id, span.{{PREFIX}}.error_key)
{ kind = consumer } | by(span.messaging.destination.name, span.{{PREFIX}}.consumer.action, span.{{PREFIX}}.skip_reason)
{ kind = consumer && span.{{PREFIX}}.correlation_id_generated = true } | by(span.messaging.destination.name)   -- producers still sending no header
{ span.messaging.message.id = "<id>" }                                                                     -- producer + consumer spans of one message, even if the trace broke
{ span.{{PREFIX}}.correlation_id = "<C>" }                                                                  -- request → publish → process → downstream, one result
{ kind = consumer && status = error } | by(span.{{PREFIX}}.error_code, span.{{PREFIX}}.consumer.action)
{ kind = consumer } >> { span.peer.service = "<dependency>" && status = error }
{ span.messaging.system = "rabbitmq" }                                                                     -- must not be empty if Rabbit is in use (K6)
```

---

## 6. Limits — say so, do not guess

* Consumers outside the estate (an ERP behind a data hub, a platform whose producer is not in your repositories): the headers from K1 arrive there; whether they are used is outside the estate. `{{PREFIX}}.correlation_id_generated=true` on your consumer spans tells which upstream producers send nothing.
* Library internals (a shared notifications library): header injection and the returned ack need a change in that library.
* Environments with the agent off: no PRODUCER/CONSUMER spans; the header, MDC, message id and log lines are the only joins — which is why K1/K2 are code, not agent config.

---

## 7. Checklist — one producer, one consumer

- [ ] §2 inventory rows exist for every send and every listener (system, topic, key, headers, message id, correlation id, sync/async, retry, ack read?, filters, error handler, gates)
- [ ] Producer: interceptor on every producer factory adds `{{CORRELATION_HEADER}}` (+ `event.id`, explicit `traceparent`), `messaging.message.id`, `messaging.message.conversation_id` on the caller's span; record has a key and a message id; every send goes through `sendTraced` (or reads `SendResult`) so the **ack is registered** (`{{PREFIX}}.publish.result/partition/offset` + the ack log line); `@Recover` → `exhausted` + `SpanOutcome.recordHandled`; caller's span carries `{{PREFIX}}.event.type` + business id
- [ ] Consumer: record interceptor on every factory restores MDC + `{{PREFIX}}.correlation_id` (generates only if absent, flags it), registers receipt ids (`message.id`, offset, partition, key, group, client id); listener registers event type/status/source, business ids, every skip as `{{PREFIX}}.consumer.action=skipped` + `{{PREFIX}}.skip_reason`, rows written, route taken
- [ ] Listener `try/catch` (not the error handler) records skipped/retry/DLT on the CONSUMER span via `SpanOutcome.record` + `{{PREFIX}}.consumer.action`; error handler logs ids only; deserialization failures → log + metric; payload never logged
- [ ] RabbitMQ paths covered (K6) and verified to produce spans
- [ ] Thread hops on MDC-aware executors; no common-pool `runAsync`; no request-scoped beans off-thread
- [ ] Deferred work stores W3C context + correlation id with the row and restores them on dispatch
- [ ] Verified: `{ span.messaging.message.id = "<id>" }` returns the producer and consumer spans; `{ span.{{PREFIX}}.correlation_id = "<C>" }` shows request → publish → process in one trace with the agent on; consumer log line carries the same correlation id with the agent off
