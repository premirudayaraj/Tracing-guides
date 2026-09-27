# 09 — After Placing Traces: Publish the Service's Trace Documentation to Confluence

> **Role:** every project, as the **last step of every instrumentation run**. Once the checklists of the guides you applied are ticked (README §2 step 5), the assistant writes one Confluence page for the service — *what* traces exist, *why* each was placed, *when* it is useful and *which functionality* it serves — and publishes it through the Confluence REST API. One project per run; the page is found or created by the project's **Git repository name**, so a second run updates the same page instead of adding another. Placeholders (`{{CONFLUENCE_BASE_URL}}`, `{{CONFLUENCE_SPACE_KEY}}`, `{{CONFLUENCE_PARENT_PAGE_ID}}`, `{{TRACE_BACKEND}}`…) are defined in `README.md §0`.
>
> **How to use with an AI assistant:** *"Publish the trace documentation of `<project>` per 09-PUBLISH-TRACE-DOCS-CONFLUENCE.md: build the page from §4, then run §3."*
>
> Legend: **MUST** = rule; ➕ = add; ⚠ = confirm at runtime.

---

## 1. What is published, and why it is part of the run

The trace placements are code; nobody reads code to learn what a service's spans mean. The page is the **contract of the service's telemetry** for support, on-call and the next engineer: it lists every span the rules added or rely on, the attributes it registers, the queries that find them, and the limits. It is regenerated from the code on every run, so it never drifts from what is deployed — which is why it is written by the same run that wrote the code, not by hand later.

Rules:
* MUST be produced at the end of **every** run that changed tracing code, before the run reports done; a run that changed nothing still refreshes the page's *Change log* row ("verified unchanged").
* MUST be **one page per repository**, titled exactly the repository name (§3.1); MUST live under the configured parent page (§2); MUST use the fixed template (§4) so two services' pages look the same and one service's page looks the same after ten runs.
* MUST describe every trace at four levels — **what** the span/attribute is, **why** it was placed (which rule), **when** it is useful (the support question it answers), and **what functionality** of the program it covers — in the Trace map table (§4, section 3). Nothing goes on the page that could not go on a span: no tokens, no PII, no payloads, no credentials, no internal hostnames beyond what `peer.service` names.
* The page is documentation of *this* service only; estate-wide rules stay in these guides (link to them; do not copy them).

---

## 2. Configuration and credentials

Two kinds of values: **where** to publish (config, safe to commit to the guides folder) and **who** publishes (credentials, never committed, never printed, never on a span or in a page).

### 2.1 Where — config values (placeholders in README §0, or a config file next to the guides)

| Key | Meaning | Example |
|---|---|---|
| `{{CONFLUENCE_BASE_URL}}` | Base URL of the Confluence site. Cloud: `https://<tenant>.atlassian.net/wiki`; Data Center / Server: `https://confluence.<company>.com` (no `/wiki`) | `https://acme.atlassian.net/wiki` |
| `{{CONFLUENCE_SPACE_KEY}}` | Key of the space holding the product/workspace page | `PLAT` |
| `{{CONFLUENCE_PARENT_PAGE_ID}}` | Numeric id of the **parent page**: the product / workspace page under which one child page per service lives (open the page → *Page information*, or the number in the URL) | `123456789` |

The assistant may also read these from `{{GUIDES_DIR}}/confluence.config.json` (`{"baseUrl": "…", "spaceKey": "…", "parentPageId": "…", "flavour": "cloud|datacenter"}`) when the placeholders were not instantiated. `flavour` decides the API (§3.3); default `cloud` when the base URL ends in `atlassian.net/wiki`.

### 2.2 Who — credentials, two accepted ways (document which one the run used, never the value)

**(a) Already present in the execution environment** — the shell that runs the coding assistant, a skill's environment, or the assistant's own settings (`settings.json` → `env`) export:

| Variable | Cloud | Data Center / Server |
|---|---|---|
| `CONFLUENCE_AUTH` | `basic` | `bearer` (personal access token) or `basic` |
| `CONFLUENCE_USER` | the Atlassian account **e-mail** | user name (basic only) |
| `CONFLUENCE_TOKEN` | an **API token** created at `id.atlassian.com` → Security → API tokens | a **Personal Access Token** (Profile → Personal Access Tokens) or the password (basic) |

**(b) A separate credentials file the user provides** — path in `CONFLUENCE_CREDENTIALS_FILE` (default `$HOME/.config/confluence/credentials.json`, permissions `600`), JSON with the same three fields in camel case:
```json
{ "auth": "basic", "user": "someone@example.com", "token": "…" }
```
Precedence: environment variables first, then the file; if neither is present the run **stops before any API call** and asks the user for one of the two — it never guesses, never prompts for a password interactively, never writes the token to a log, a page, a span, a commit or the guides folder.

Auth header built from them (⚠ verify with a `GET` of the parent page before writing anything):
```sh
# Cloud (basic = email:api-token)          Data Center (bearer PAT)
-u "$CONFLUENCE_USER:$CONFLUENCE_TOKEN"     -H "Authorization: Bearer $CONFLUENCE_TOKEN"
```
The account needs *view* + *add/edit pages* in the space. Cloud OAuth 2.0 (3LO) is also possible for a long-lived integration but is not the shape of a coding-assistant run; API token / PAT is.

---

## 3. Procedure (idempotent — safe to run after every instrumentation run)

### 3.1 Page identity = the Git repository name, verbatim

```sh
cd "$PROJECT"                                                     # the checked-out project the run instrumented (one per run)
REPO=$(basename -s .git "$(git remote get-url origin 2>/dev/null)")   # last path segment of the origin URL, ".git" stripped (works for https://…/org/repo.git and git@host:org/repo.git)
[ -n "$REPO" ] || REPO=$(basename "$(git rev-parse --show-toplevel)")                   # no remote: the top-level directory name
```
* The title is `$REPO` **exactly** — same case, no prefix, no suffix, no date. Byte-identical titles are what make the second run find the first run's page; a title with a date or a version would create a page per run.
* Multi-module repositories get one page (the repository), with the modules as rows of the Trace map (§4).
* Record `$REPO`, the branch and the commit (`git rev-parse --short HEAD`) for the Change log; they go on the page, not in the title.

### 3.2 Find-or-create under the parent, then write

```
1. resolve space id (Cloud only)      GET  {base}/api/v2/spaces?keys={{CONFLUENCE_SPACE_KEY}}            → results[0].id
2. look for the page                   GET  {base}/api/v2/pages/{{CONFLUENCE_PARENT_PAGE_ID}}/children?limit=250   → the child whose title == $REPO (paginate with _links.next)
                                       (Data Center: GET {base}/rest/api/content?spaceKey=…&title=$REPO&expand=version,ancestors → the result whose last ancestor id == parent)
3a. found  → UPDATE                    PUT  {base}/api/v2/pages/{id}   body: { id, status:"current", title:$REPO, body:{representation:"storage", value:<html>}, version:{number: <current+1>, message:"tracing run <date> <commit>"} }
3b. absent → CREATE                    POST {base}/api/v2/pages        body: { spaceId, status:"current", title:$REPO, parentId:"{{CONFLUENCE_PARENT_PAGE_ID}}", body:{representation:"storage", value:<html>} }
4. verify                              GET  {base}/api/v2/pages/{id}?body-format=storage  → title == $REPO, parentId == parent, version.number advanced; print _links.webui as the page URL
```
* **Exact title match**, case-sensitive, trimmed — never "contains". If two children already carry the title (a manual duplicate), update the older one and report the duplicate; never create a third.
* Update replaces the whole body (the page is generated); the previous versions stay in Confluence's page history, which is the audit trail — do not keep old content on the page.
* The run prints: `confluence: <created|updated> "<REPO>" → <url> (version N)`. Any non-2xx stops the run with the status and the response's `message`; nothing is retried blindly (a 409 on update means the version number is stale — re-read and retry once).

### 3.3 Calls, concretely (Cloud, `curl`; Data Center differences noted)

```sh
# credentials: (a) environment, else (b) the credentials file — the values are used, never echoed
F="${CONFLUENCE_CREDENTIALS_FILE:-$HOME/.config/confluence/credentials.json}"
[ -n "$CONFLUENCE_TOKEN" ] || { [ -r "$F" ] || { echo "no Confluence credentials: export CONFLUENCE_USER/CONFLUENCE_TOKEN or provide $F" >&2; exit 2; }
  CONFLUENCE_AUTH=$(jq -r '.auth // "basic"' "$F"); CONFLUENCE_USER=$(jq -r '.user // ""' "$F"); CONFLUENCE_TOKEN=$(jq -r '.token' "$F"); }
B="{{CONFLUENCE_BASE_URL}}"
if [ "${CONFLUENCE_AUTH:-basic}" = bearer ]; then A=(-H "Authorization: Bearer $CONFLUENCE_TOKEN"); else A=(-u "$CONFLUENCE_USER:$CONFLUENCE_TOKEN"); fi
H=(-H "Content-Type: application/json" -H "Accept: application/json")
curl -sSf "${A[@]}" "$B/api/v2/pages/{{CONFLUENCE_PARENT_PAGE_ID}}" > /dev/null || { echo "cannot read the parent page — check base URL, flavour and credentials" >&2; exit 2; }   # verify before any write
SPACE_ID=$(curl -sS "${A[@]}" "$B/api/v2/spaces?keys={{CONFLUENCE_SPACE_KEY}}" | jq -r '.results[0].id')
# find the child page(s) titled exactly $REPO — walk every page of children (the API pages results)
URL="$B/api/v2/pages/{{CONFLUENCE_PARENT_PAGE_ID}}/children?limit=250"; IDS=""
while [ -n "$URL" ]; do R=$(curl -sS "${A[@]}" "$URL"); IDS="$IDS $(echo "$R" | jq -r --arg t "$REPO" '.results[] | select(.title == $t) | .id')"
  NEXT=$(echo "$R" | jq -r '._links.next // empty'); URL=${NEXT:+"$B${NEXT#/wiki}"}; done
set -- $IDS; PAGE_ID=$1; [ $# -le 1 ] || echo "WARNING: $# pages titled \"$REPO\" under the parent — updating the oldest ($PAGE_ID); remove the duplicates: ${*:2}" >&2   # ids are ascending = creation order
BODY=$(jq -Rs . < page.storage.html)                                                  # the §4 template rendered to Confluence storage format (XHTML), JSON-escaped
if [ -n "$PAGE_ID" ]; then
  VER=$(curl -sS "${A[@]}" "$B/api/v2/pages/$PAGE_ID" | jq '.version.number')
  curl -sS "${A[@]}" "${H[@]}" -X PUT "$B/api/v2/pages/$PAGE_ID" -d "{\"id\":\"$PAGE_ID\",\"status\":\"current\",\"title\":\"$REPO\",\"body\":{\"representation\":\"storage\",\"value\":$BODY},\"version\":{\"number\":$((VER+1)),\"message\":\"tracing run $(date -u +%F) $(git rev-parse --short HEAD)\"}}"
else
  curl -sS "${A[@]}" "${H[@]}" -X POST "$B/api/v2/pages" -d "{\"spaceId\":\"$SPACE_ID\",\"status\":\"current\",\"title\":\"$REPO\",\"parentId\":\"{{CONFLUENCE_PARENT_PAGE_ID}}\",\"body\":{\"representation\":\"storage\",\"value\":$BODY}}"
fi
# Data Center / Server (v1 API only): find  GET $B/rest/api/content?spaceKey=…&title=$REPO&expand=version,ancestors
#   create POST $B/rest/api/content  {"type":"page","title":$REPO,"space":{"key":"…"},"ancestors":[{"id":"<parent>"}],"body":{"storage":{"value":…,"representation":"storage"}}}
#   update PUT  $B/rest/api/content/{id}  {"id":…,"type":"page","title":$REPO,"version":{"number":N+1},"body":{"storage":{…}}}
```
* Confluence **Cloud**: use the v2 API (`/wiki/api/v2/...`); the v1 `/rest/api/content` create/read/update endpoints were deprecated in 2023 with removal announced for 31 March 2025 — treat them as gone. **Data Center / Server** has only the v1 API. ⚠ Confirm which flavour the site is with one `GET` before the first run.
* Rate limits (Cloud) answer `429` with `Retry-After`; honour it once, then stop.
* The body is **storage format** (XHTML): `<h2>`, `<p>`, `<table><tbody><tr><th>…`, and code blocks as `<ac:structured-macro ac:name="code"><ac:parameter ac:name="language">sql</ac:parameter><ac:plain-text-body><![CDATA[ … ]]></ac:plain-text-body></ac:structured-macro>`. Escape `<`, `&` in text; keep TraceQL inside CDATA.

---

## 4. The fixed page template (every service page has exactly these sections, in this order)

Section headings are fixed strings; a section with nothing to say still appears with the line *"none in this service"*. Tables keep their columns even when short.

```
1. Service
   repository (= page title) · path under {{ESTATE_ROOT}} · roles found by 00 §3 (API, PRODUCER, …) · language/framework · tracing stack in use (Java agent / OTel SDK / Micrometer bridge / none — 00 §3.5) · service.name · error-code module id (02 §2.1) · guides version (git commit of {{GUIDES_DIR}}) · last run date
2. How to find this service's traces
   {{TRACE_BACKEND}} starter queries: { resource.service.name = "<service.name>" }, by correlation id, by error code; the correlation header ({{CORRELATION_HEADER}}) and where it is echoed; the {{LOG_BACKEND}} query that opens a trace from a log line
3. Trace map  (one row per span or placement the rules added or rely on)
   | Span / placement | Where in the code (file : class.method) | What it is | Why it was placed (guide § / rule) | When it is useful (the support question it answers) | What functionality it serves |
   rows: the door span and its attributes (01 §3, §3.1 caller) · the auth chain decisions (01 §4) · request decisions (01 §5) · each per-purpose retry span (04 §4.4) · each outbound handover (04) · each producer ack / consumer receipt (05) · each job root (06) · UI/BFF spans (07)
4. Attributes registered
   | Attribute | Values / type | Set where | Meaning |   — every attribute this service's code writes, plus the standard keys it relies on; grouped as in 01 §7
5. Outbound handovers
   | Dependency (peer.service) | Purpose(s) | Auth step | Request fields registered | Response fields registered | Outcome facets (<dep>.outcome values seen) | Retry span (name, max) |
6. Messaging
   | Direction | Topic / exchange | Key | Message id source | Correlation id (header / payload) | Ack / receipt attributes | Skip reasons / consumer.action values | Error handling (retry topic, DLT, seek-past) |
7. Jobs
   | Job | Trigger | Root span name | Run id → correlation id | Counts registered | Status values | Lock |
8. Error codes and calls the service carries on past
   codes this service can emit (hard failures, 03 §2.1) with key and band · the failure keys it can write (<dependency>.<action>.failure, one line each: key → what the program does instead → where the real code lives) · hidden-200 outcomes (03 rule C)
9. Verification queries
   the TraceQL / LogQL from the checklists of every guide applied, each one run against a real span in a non-production environment, with the date it was proven
10. Not traced and known limits
   exclusions (00 §3.1), ⚠ items still unverified, libraries that must change (00 §5), environments with the agent off, upstream systems outside the estate
11. Change log
   | Date | Guides version | Service commit | Run summary (guides applied, spans added / changed / removed, "verified unchanged") |   — one row appended per run; the previous rows are kept
```

Rendering rules:
* Rows of the Trace map are written in plain sentences a support engineer can act on: *"`POST /billing/v3/invoices` SERVER span — the request's root; placed by 01 §3 so every log line, error body and span share one correlation id; useful when a customer quotes an error code and a correlation id; covers every invoice-listing request"*.
* The **Why** column cites the guide and rule (`01 §4.2`, `04 §4.4`, `05 K1`) so the page and the guides stay cross-referenced; the **What functionality** column names the business function in the service's own terms (invoice listing, purchase-order creation, account onboarding), not the class name.
* Attribute values are listed as enums or types, never as examples containing real customer data.
* The Change log is the only section that accumulates; everything else is regenerated from the code on each run.

---

## 5. What must not be done

* No page per run, per branch, per date, per module — one page per repository, titled with the repository name only.
* No credentials in the page, the config file in the guides folder, the commit, the assistant's transcript or a span; no `--verbose` curl in logs (it prints the auth header).
* No prose copied from these guides into the page — link to them.
* No page for a repository the run did not instrument (a library gets a page only when the run changed its hooks — 00 §5).

---

## 6. Checklist — one service page

- [ ] Config resolved (`{{CONFLUENCE_BASE_URL}}`, `{{CONFLUENCE_SPACE_KEY}}`, `{{CONFLUENCE_PARENT_PAGE_ID}}`, flavour) and credentials found through (a) environment or (b) the credentials file — which one, stated in the run output; a `GET` of the parent page succeeded before any write
- [ ] `$REPO` taken from the origin remote (fallback: top-level directory name) and used verbatim as the title
- [ ] Child pages of the parent listed; exact-title match → update with `version.number+1`; none → create with `parentId`; duplicates reported, never multiplied
- [ ] Page body follows §4 exactly: eleven sections in order, Trace map rows with what / why / when / which functionality, attributes table, handovers, messaging, jobs, codes + failure keys, proven queries, limits, change-log row appended
- [ ] Verified after the write: title, parent, version advanced; page URL printed in the run summary
- [ ] Nothing sensitive on the page (tokens, PII, payloads, credentials, internal hostnames)
