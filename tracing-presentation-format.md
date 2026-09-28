# Distributed Tracing: The Missing Foundation

---

## 1. Intro

Goal: make it possible for everyone in NA to support any project, not just their own.

To get there, we need to:
- Enable observability
- Enable distributed tracing

---

## 2. The Three Pillars of Grafana Observability

- **Prometheus (metrics):** how much and how often. Request rate, error rate, CPU, memory.
- **Loki (logs):** the detailed text of what happened inside a service.
- **Tempo (traces):** the path of one request across every service it touched.

Grafana is the single screen that sits on top of all three.

> **Example:** a customer's order fails.
> Prometheus shows the error rate spiked.
> Tempo shows which service in the chain failed.
> Loki gives the full exception detail from that one service.
> Each pillar answers a different question. Today we only use one of them properly.

---

## 3. Role of Tempo vs Loki

In a monolith, one request lives in one process and one log file. Reading logs works.
In microservices, one request crosses many services, so its logs are scattered across all of them.

- **Loki:** detailed description only.
- **Tempo:** the fundamental database for observability. It covers analytics, user tracking,
  error detection, application performance monitoring, bug monitoring, and answers
  *where* an issue happened and *why*.

### What goes in a trace vs a log

| On the trace (searchable)     | In the error log (detail)       |
|-------------------------------|---------------------------------|
| Where the error happened      | The full detailed description   |
| How it happened               |                                 |
| The cause of the error        |                                 |

> **Example:** `{ span.error_code = "ERR-BIL-0042" }` → one row: billing-api, timeout, retried 3×.
> The trace tells you where and why; the log line has the full exception text.

> **Diagram:** logs scattered across services vs one trace spanning the whole request.
> *(Open: "how tokens are saved for searches" still needs to be confirmed.)*

---

## 4. The Gap Today

- We have a correlation id at the gateway, but nothing follows the request across services.
- Every investigation is done through logs.
- Tempo, the database that makes all of this possible, has no data in it.

We are missing the fundamental base for maintaining an application. We can get by for a while
with Grafana AI and log-based monitoring dashboards, but we will eventually lose efficiency.

---

## 5. What We Can Achieve with Tempo

### Follow one request across every service
> **Animation:** paste one correlation id; the rows drop in one service at a time, failing hop in red.
> **Example:** `{ span.correlation_id = "corr-7f3a9c" }`

| Start | Service | Span | Status | error_code | Failure | Duration |
|---|---|---|---|---|---|---|
| 10:42:07 | api-gateway | POST /orders | error | — | HTTP 502 | 15.4 s |
| 10:42:07 | orders-api | POST /orders | error | — | HTTP 502 | 15.3 s |
| 10:42:07 | billing-api | POST /billing/charge | error | ERR-BIL-0042 | retries exhausted (3) | 15.2 s |
| 10:42:07 | ledger-service | POST /entries | error | — | timeout | 5.0 s |
| 10:42:12 | ledger-service | POST /entries | error | — | timeout | 5.0 s |
| 10:42:17 | ledger-service | POST /entries | error | — | timeout | 5.0 s |

The path, the timing, and the failing service — one id, one table.

### Early warning from retry trends
Retry counts recorded on spans become a trend line. A rising trend warns you before a call fully fails.
> **Example:** a payment call normally retries 0–1 times. Over a week, the average climbs to 3–4.
> Nothing has failed yet, but the graph shows it coming, so the dependency is fixed first.
> *(This is early warning from trends, not machine-learning prediction.)*

### Zoomable service map
> **Animation:** a map of all services. Zoom into one service to see its retries,
> failures, and slow calls.
> This is the Grafana service map built from trace data, so it can be demoed live.

### Every team sees other teams' services live
Every team can see what is happening in other teams' services, and in the services they depend on.
> **Example:** your call to another team's service fails. Instead of raising a ticket and waiting,
> both teams open the same trace and see the failing hop. Same picture, no back-and-forth.

---

## 6. How to Achieve This Immediately

### Step 1
- **Internal error codes are highly recommended.** Every hard failure gets a code that
  links the trace, the log, and the error the client sees.
- **Each team runs the tracing MD guides on its service and pushes to PROD without delay.**
  Having at least some traces goes a long way. Don't worry about missing traces.
- **Traces are lower risk than logs.** Setting the same attribute twice overwrites it,
  so duplication is harmless. The guides forbid creating spans inside loops,
  so runaway volume is designed out.
- **Tempo is built for very high write volume.** Our request volume is small by comparison.
- **Give the Claude skill an API key for our Confluence** so it can publish the
  documentation automatically.
- **The documentation gives a full trace definition for each microservice.**
  Every service gets the same page format, under its product's parent page —
  one documentation pattern for every service, so anyone can read any service's page.

> **Example:** today, service A's documentation looks nothing like service B's,
> so you relearn the layout every time. With the template, every service's page
> has the same sections in the same order.

### Step 2
- Fix traces in later releases where needed.
- The Confluence documentation becomes the structure for future improvements
  to production monitoring.
- **Stop building a separate monitoring dashboard for each application.**
  Separate dashboards never give a complete picture of the system.
  Build it in Grafana, using Tempo with TraceQL. Grafana is built for this.

> **Example:** one TraceQL query covers every service at once:
> `{ resource.service.name = "billing" && status = error }`
> shows every failed request touching billing, across all callers,
> with no new dashboard to build.