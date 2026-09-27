# 02 — Error Code Design: How Services Create Error Codes

> **Scope:** the *logic* for defining, numbering and returning error codes in Spring Boot API services — the code format, the catalogue, the exception classes, the response body and the HTTP-status policy. It says nothing about spans — that is `03-ERROR-CODE-TRACING.md`. Span placement is `01`. Placeholders are defined in `README.md §0`.
>
> **How to use with an AI assistant:** *"Add endpoint `<X>` to `<service>` with error codes defined per 02-ERROR-CODE-DESIGN.md."* The assistant must then (1) pick the keys, (2) add catalogue entries with the next free numbers in the right band, (3) throw the service's application exception with the resolved code, (4) let the existing exception handler build the response.
>
> Legend: **MUST** = rule (what well-built reference services do); ➕ = recommended, often not in current code; ⚠ = not verified beyond reading the source.

---

## 0. Reference patterns (what this is generalised from)

| What | Observed in the reference services |
|---|---|
| Catalogue | services using an in-house REST library: a YAML catalogue (`application-error-code.yaml`) keyed by constants, loaded locally via `spring.config.additional-location` and in-cluster from the config server; a service without that library: a Java `enum` whose `getCode()` prefixes the module id |
| Code resolution | always **by key at runtime**, never by literal string: `errorProperties.getErrorCode(KEY)` → numeric suffix → module prefix added at the boundary; enum `CODE.getCode()` → prefixed string |
| Exception classes | one application exception per service carrying `HttpStatus`, the resolved code and a list of detail entries; a few custom subclasses (policy exception, upstream-API exception, validation exception, "no record" exceptions); legacy plain `RuntimeException`s mapped later |
| Exception handlers | one `@RestControllerAdvice` at `HIGHEST_PRECEDENCE` per service (or the library's global handler when the service extends the library's base controller); dedicated advices that turn "no record" exceptions into a 200 with an empty typed body; a WebFlux `ErrorWebExceptionHandler` in the authorisation service |
| Downstream error mapping | each outbound client maps non-2xx to its **own** `<DEPENDENCY>_API_ERROR` code and logs the raw body; one service passes upstream 400/404 status+body through and maps the rest to 500 |
| Response body type | an array of error entries `{errorCode, errorMessage, details, correlationId, help, additionalInfo[]}`; one service adds `timestamp` and omits empty fields |
| Validation | Jakarta Bean Validation everywhere; one service accumulates every field error into `additionalInfo`, another throws on the first violation |
| A second family | one product line returns `{"errorCode":"<PRODUCT>-<NAME>","correlationId":"<trace id>"}` with name-based codes and the OpenTelemetry trace id as the correlation id |

---

## 1. The code format

```
   {{ERR_PREFIX}} - 4210  024
   │                │     └── Code    (3 digits; 4 in a few entries) — unique within the module, allocated by band (§4)
   │                └──────── Module  (4 digits) — one per service, from the platform registry
   └───────────────────────── Prefix  `{{ERR_PREFIX}}-` — constant across the company
```

* Always a **string**; the client-facing form is `{{ERR_PREFIX}}-<module><code>` (`{{ERR_PREFIX}}-4210024`).
* Build it by **resolution, never by concatenation in business code**:
  * catalogue services — an entry keyed by a constant:
    ```yaml
    error-codes:
      DATE_OUT_OF_RANGE:
        error-code: '024'
        message: 'Start and end date is out of range.'
    ```
    and in code `errorProperties.getErrorCode(ErrorCode.DATE_OUT_OF_RANGE)`; the boundary utility prepends `{{ERR_PREFIX}}-<module>` when it sees a bare `024`.
  * enum services — `ErrorCodes.GET_COUNTRY_DETAILS.getCode()` returns the prefixed string; callers `throw new InternalServerException(ErrorCodes.GET_COUNTRY_DETAILS)`.
* The same code therefore identifies **which service and which condition** without a lookup. There is no API/endpoint id in the code: two endpoints of one service that fail for the same reason share the code (see §4 on how to keep that useful).

### 1.1 Nothing in a success body identifies the producing service
On failures the module id inside the error code says which service answered; on success nothing in the body does. The span's `resource.service.name` is the identifier (01).

### 1.2 A second code family may exist — keep it, register it the same way
If a product line returns name-based codes (`<PRODUCT>-<NAME>`, e.g. `AUTHORIZATION_HEADER_MISSING` → 401, `ACT_ON_BEHALF_DENIED` → 403) with the trace id as `correlationId`, do not convert them to numbers; on the span the attribute is still `error_code` (with that value) and `error.type` (03), so one TraceQL works across both families.

---

## 2. Identifier registries

### 2.1 Module ids (4 digits, platform-wide)
One per service, assigned by the platform registry (⚠ ask the platform team where it lives); never reuse. The prefix is applied at one place per service (the boundary utility or the enum's `withPrefix`).

### 2.2 Keys (the stable identifiers in code)
The key is the stable identifier in code; the number is looked up.

Naming: `SCREAMING_SNAKE`, `<Subject>_<Condition>` for business rules (`ACCOUNT_NOT_BELONGS_TO_USER`), `<Dependency>_API_ERROR` for upstream failures, `<Store>_DB_ERROR` for datastores, `<Topic>_PUBLISH_FAILED` / `EVENT_PROCESSING_FAILED` for messaging (05), and the fixed trio `NOT_ENOUGH_PERMISSIONS` / `VALIDATION_FAILURE` / `INTERNAL_SERVER_ERROR` that every service has.

Minimum set every service MUST have: `NOT_ENOUGH_PERMISSIONS`, `VALIDATION_FAILURE`, `INTERNAL_SERVER_ERROR`, one `<X>_NOT_FOUND` per primary entity, one `<Dependency>_API_ERROR` per outbound system, one `<Store>_DB_ERROR` per datasource, one messaging key per topic it publishes.

---

## 3. Exception classes (what to throw)

| Class | Carries | Use for |
|---|---|---|
| the service's application exception (`AppException` / the library's `ApplicationException`) | `HttpStatus`, resolved code, `List<Detail>` | **every** failure a service raises itself, including mapped downstream failures — `throw new AppException(HttpStatus.BAD_REQUEST, errorProperties.getErrorCode(ErrorCode.DATE_OUT_OF_RANGE))` |
| the library's runtime exception | same | internal/unexpected; often **not** caught by the `instanceof ApplicationException` branches and falls through to a hard-coded 500 (§7.1). Prefer the application exception |
| policy exception | policy + cause | thrown by the policy interceptor/input provider; handled by the policy advice |
| upstream-API exception | `HttpStatus`, upstream error | dependency-specific failures before mapping |
| validation exception (extends the application exception) | 400 + `VALIDATION_FAILURE` + field details | request validation |
| `*NoRecordException` family | — | "no data" outcomes that advices turn into **200 with an empty typed body** (§7) |
| legacy `RuntimeException`s | message | mapped by the client utility to the application exception (404/500) |
| `BaseException` + `BadRequestException`, `InternalServerException`, … (enum services) | `HttpStatus`, `List<ErrorModel>` | everything; subclasses fix the status |

Rule: **one exception type per service for business failures**, carrying the resolved code. Don't add new exception classes to carry new codes — add catalogue keys.

---

## 4. Number bands (the allocation rule for the code)

The convention the reference catalogues follow (observed, ⚠ not a written rule — treat as the convention to keep):

| Band | Meaning | Examples |
|---|---|---|
| `001`–`019` | **Caller-side / access**: permissions, mandatory input, ownership | `005 NOT_ENOUGH_PERMISSIONS`, `010 VALIDATION_FAILURE`, `001 ACCOUNT_NOT_FOUND`, `002 ACCOUNT_IS_MANDATORY`, `003 ACCOUNT_NOT_BELONGS_TO_USER` |
| `020`–`029` | **Request limits / ranges** | `024 DATE_OUT_OF_RANGE`, `025 MAX_NO_DOCUMENT_EXCEEDED` |
| `030` | **Unknown / internal** — fixed | `030 INTERNAL_SERVER_ERROR` |
| `100`–`399` | **Domain / entity state** | `100 INVALID_ACCOUNT_NUMBER`, `3005 USER_LOCKED` |
| `400`–`409`, `500` | enum services only: mirrors of the HTTP status the handler will use | `400 BAD_REQUEST`, `401 UNAUTHORIZED_ACCESS`, `403 FORBIDDEN_ACCESS`, `404 NOT_FOUND`, `409 CONFLICT`, `500 INTERNAL_SERVER_ERROR` |
| `450`–`459` | **Not found** | `451 ACCOUNT_NOT_FOUND` |
| `460`–`469` | **Upstream API failure**, one per dependency — fixed meaning | `460 <billing hub>`, `461 <document store>`, `462 <admin API>`, `463`/`464 <file storage / OAuth>`, `465 <account platform>` |
| `470`–`479` ➕ | **Messaging failure** (event not emitted after retries, message could not be processed) — needed by 05 K1/K3 (`<TOPIC>_PUBLISH_FAILED`, `EVENT_PROCESSING_FAILED`, `EVENT_INVALID`) | — |
| `480`–`489` | **Datastore failure**, one per datasource — fixed meaning | `480 <finance db>`, `481 <terminal db>` |

Rules:
* `005`, `010`, `030`, `46x`, `47x`, `48x` are the **platform-wide signals** — dashboards and runbooks can rely on `{{ERR_PREFIX}}-<module>46*` = "an upstream API failed", `48*` = "a database failed", `*030` = "unknown". Keep them fixed in every catalogue service.
* Gaps are fine; never renumber.
* One key ⇒ one number per module. A failure that must be distinguished **per endpoint** gets its own key (`INVOICE_NOT_FOUND` vs `PAYMENT_NOT_FOUND`), not a reused key. The endpoint itself is on the span (`http.route`, 03).
* Upstream-failure entries share one generic message on purpose: the number tells support *which* dependency, the message must not leak internals. User-actionable entries (permissions, validation, ranges) carry a real sentence.

---

## 5. The catalogue (where codes live)

Catalogue services: `application-error-code.yaml`, loaded through `-Dspring.config.additional-location=file:../config/application-error-code.yaml` locally and by the config server in-cluster (00 §3.5). Keep every copy identical — a `config/` copy that drifted to a handful of dev codes is a real defect pattern.

Every key the service can throw MUST be in the catalogue. A missing key makes `getErrorCode(key)` return `null`, and the handler then throws an NPE while building the error → a second, contentless 500 that hides the real failure. ➕ A unit test that iterates the key constants and asserts each resolves is the cheapest guard.

Enum services: the enum **is** the catalogue; adding a constant adds the code. The handler reads the errors from the exception, so the message lives on the enum too.

---

## 6. Where errors originate and how they are mapped per layer

```
Datastore (MyBatis/JPA/Hikari)      ── handler / catch ──►  <STORE>_DB_ERROR (48x) as AppException(500, …)
Outbound HTTP (WebClient)           ── onErrorMap / handleErrorResponse ──►  any non-2xx → AppException(500, <DEP>_API_ERROR); a "no data" marker in the body → NoRecordException
Outbound HTTP (pass-through style)  ── client utility ──►  400/404 → AppException(status, upstream code+body); other → 500 generic
Outbound HTTP (RestTemplate)        ── response error handler + catch ──►  404 → BaseException(404, NOT_FOUND); other → InternalServerException; raw body logged at WARN, never forwarded
Authorization (policy engine)       ── policy interceptor / input provider ──►  PolicyException → 403 NOT_ENOUGH_PERMISSIONS (see §7.1 for the status-derived-code defect)
Service (business rules)            ─────────────────────────────►  AppException(4xx, ACCOUNT_NOT_FOUND / ACCOUNT_NOT_BELONGS_TO_USER / DATE_OUT_OF_RANGE …)
Controller input (Bean Validation)  ── handler ──►  400 VALIDATION_FAILURE; accumulate every field error in additionalInfo (one reference throws on the FIRST violation — do not copy that)
```

Three properties of this design that matter for tracing:
* **A code is minted only for a hard failure** — an operation the service itself fails and answers with an error body (or gives a record up to retry/DLT, or ends a job run `failed`). A failure the code catches and continues past (an ignored notification failure, a fallback value, a timeout reported as processed) gets **no** code in this service — it is a `<dependency>.<action>.failure` key on the span (03 §2.1); the code for that problem belongs to the service that actually failed. The catalogue therefore holds keys for outcomes the client can receive, not for every `catch` block.
* **Downstream codes are not passed through** (except the documented 400/404 pass-through). A caller turns the upstream's code into its own `*_API_ERROR` and logs the original. The client sees where it *surfaced*, not where it *originated*. 03 §3 puts the origin on the span so support can still see it; ➕ if pass-through is ever wanted, carry the upstream code in `details`, never replace `errorCode`.
* **Validation returns all fields** in the better reference; new code SHOULD accumulate (`additionalInfo[{property, reason}]`).

---

## 7. Response body and HTTP status policy

```json
[
  {
    "errorCode": "{{ERR_PREFIX}}-4210024",
    "errorMessage": "Start and end date is out of range.",
    "details": "…optional, human readable…",
    "correlationId": "fec23861-…",              // == {{CORRELATION_HEADER}} request header
    "help": "…optional…",
    "additionalInfo": [ { "property": "endDate", "reason": "must be after startDate" } ]
  }
]
```

HTTP status is the **real** status — a business failure is never hidden behind a 200 with an error envelope, with one exception listed below:

| Situation | HTTP status | Body |
|---|---|---|
| Validation, missing/invalid parameter or header, unreadable body | **400** | error array with `VALIDATION_FAILURE`/`BAD_REQUEST`, field list in `additionalInfo` |
| Not authorised by policy / missing permission | **403** | `NOT_ENOUGH_PERMISSIONS` (`{{ERR_PREFIX}}-<module>005`, or the enum service's `403`) |
| Entity not found | **404** | `*_NOT_FOUND` |
| "No records" for a list/download endpoint (one reference service) | **200** with an **empty typed body** (`*NoRecordException` → empty paginated response) | no error body — **the only place a business outcome hides behind 200**; make it visible on the span (03 §4) |
| Upstream API / datastore / messaging failure | **500** | `<DEP>_API_ERROR` (46x) / `<STORE>_DB_ERROR` (48x) / `*_PUBLISH_FAILED` (47x) |
| Unknown | **500** | `INTERNAL_SERVER_ERROR` (030) |
| Health probe failure | 503 from actuator | actuator body, no code |

### 7.1 Known defect pattern to keep out of new code — a status-derived code
One reference policy advice built `errorCode` from the **HTTP status** (`String.valueOf(status.value())`, fallback `500`) instead of the catalogue, and the input provider threw the library's *runtime* exception when the user had no permissions row, which failed the `instanceof ApplicationException` check and landed in that fallback → `{{ERR_PREFIX}}-<module>500` / "Internal Server Error" for an ordinary "no access" state, on every endpoint. The correct shape: `AppException(HttpStatus.FORBIDDEN, errorProperties.getErrorCode(NOT_ENOUGH_PERMISSIONS))` → 403 / `…005`. Rule: **an error code is always a catalogue code, never an HTTP status number.** Until such a defect is fixed, 03 registers the code exactly as the body carries it — the mismatch is what makes it findable.

---

## 8. Checklist — adding a new API or a new error

- [ ] Key added to the constants class / enum with `<Subject>_<Condition>` / `<Dependency>_API_ERROR` / `<Store>_DB_ERROR` / `<Topic>_PUBLISH_FAILED` naming
- [ ] Catalogue entry added with the next free number **in the right band** (§4) and a client-safe message; every copy of the catalogue identical
- [ ] Every dependency the endpoint calls has a `46x` key; every datasource it reads has a `48x` key; every topic it publishes has a `47x` key
- [ ] Code thrown as the application exception with the resolved key — never the library's runtime exception, never a hard-coded string, never an HTTP status as the code
- [ ] Downstream non-2xx mapped in the client class, raw body logged once at WARN/ERROR with the correlation id, not forwarded
- [ ] Validation errors accumulated into `additionalInfo[{property, reason}]`
- [ ] HTTP status per §7; any "no records → 200 empty" path flagged on the span (03)
- [ ] Unit test asserts every key resolves to a non-null catalogue entry
