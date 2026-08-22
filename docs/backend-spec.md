# Spec: Email Service (queue-based delivery API)

- Status: Draft for review
- Owner: stbensonimoh
- Repo: `rippledcyeorg/email-service`
- Date: 2026-08-20, revised 2026-08-22 (issue-driven refinements, recorded as
  decisions 6-11)

The key words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY in this document are to be
interpreted as described in [RFC 2119](https://www.ietf.org/rfc/rfc2119.txt).

## 1. Objective

Every RipplED product sends email, but only the volunteer pipeline does today, and its
queue, D1, and Worker are coupled to volunteer data (queue messages reference
`volunteer-db` rows). This spec defines a standalone email service any product can call:
one secure endpoint, a durable queue, one consumer Worker, delivery from the already
onboarded `rippledcye.org` domain. It is stack agnostic by design: any form, API, script,
or Worker that can POST JSON with a secret header can send email.

The audience: RipplED engineers (the builders of the forms) and the founder (who approves
sender addresses and copy).

Success looks like: a caller POSTs once, gets an immediate 202, the email arrives from the
sender address that caller was granted, and duplicates are impossible.

## 2. Architecture Overview

```mermaid
flowchart TB
    subgraph callers["Any product (stack agnostic)"]
        F1["Form A"] --> API1["Product API A"]
        F2["Form B"] --> API2["Product API B"]
        F3["Apps Script"] --> API3["Product API C"]
    end

    subgraph svc["email-service (Cloudflare Worker)"]
        HTTP["POST /send<br/>fetch handler<br/>secret + validation + rate limit"]
        W["queue consumer<br/>claim + send"]
        subgraph cf["Cloudflare account"]
            D1[("email-db<br/>canonical store")]
            Q[["email-events<br/>retries + DLQ"]]
        end
        EM["Email Sending<br/>from @rippledcye.org"]
    end

    API1 -->|"X-Email-Secret"| HTTP
    API2 -->|"X-Email-Secret"| HTTP
    API3 -->|"X-Email-Secret"| HTTP
    HTTP -->|"INSERT status=pending"| D1
    HTTP -->|"enqueue email.created"| Q
    Q --> W
    W -->|"load row, atomic claim"| D1
    W --> EM
```

```mermaid
sequenceDiagram
    participant C as Caller
    participant S as /send handler
    participant D as D1
    participant Q as Queue
    participant W as Consumer
    participant E as Email Sending

    C->>S: POST /send + X-Email-Secret
    S->>S: secret, validate payload, rate limit
    S->>D: SELECT by idempotency_key
    alt duplicate key
        D-->>S: row exists
        S-->>C: 202 { ok, id } (replay, no side effects)
    else new email
        S->>D: INSERT row, status=pending
        S->>Q: enqueue { type: email.created, emailId }
        S-->>C: 202 { ok, id }
        Q->>W: deliver (retries on failure, DLQ after 5)
        W->>D: load row by id
        W->>D: atomic claim (sent_at conditional UPDATE)
        alt claim lost
            W->>D: re-read row; orphan rows: complete bookkeeping, no send
            W-->>W: ack, nothing to do
        else claim won
            W->>E: send
            W->>D: status = sent, processed_at = sent
        end
    end
```

Principles, carried over from the volunteer pipeline (proven in production 2026-08-20):

- D1 is the canonical store. The queue message carries the email id only, so a retried
  message can never re-send stale data.
- The send is claimed atomically with a conditional UPDATE before the email goes out, so
  simultaneous attempts and redeliveries send at most once.
- The service MUST NOT reach into caller databases. The full payload lives in its own D1
  row.

## 3. Components

### 3.1 HTTP API: `POST /send`

Served by the Worker's fetch handler (the service is a standalone Worker with a public
route, not a Pages Function).

- Auth: `X-Email-Secret` header, constant-time compare against the `EMAIL_SECRET` secret.
  Missing or incorrect secret MUST return 401. The secret is shared across RipplED
  products in v1; per-caller keys are out of scope.
- Payload (JSON):
  - `to`: required, `^[^\s@]+@[^\s@]+\.[^\s@]+$`, max 320 chars.
  - `from`: optional; defaults to `DEFAULT_FROM`. When present, MUST be exactly one of the
    `ALLOWED_SENDERS` config entries. Callers can only use sender addresses they were
    granted.
  - `subject`: required, 1..200 chars, no control characters (`[\x00-\x1F\x7F]`).
  - `text`: required, 1..100000 chars, no control characters (`[\x00-\x1F\x7F]`).
  - `html`: optional, max 200000 chars.
  - `replyTo`: optional, single address, same pattern as `to`.
  - `idempotencyKey`: optional, 1..100 chars; defaults to a generated UUID. Callers SHOULD
    pass a stable key per business event so retries cannot double-send.
- Invalid payloads MUST return 400 with field-level errors, keyed by field name:
  `{ ok: false, errors: { "<field>": "<message>" } }`. Field names are `to`, `from`,
  `subject`, `text`, `html`, `replyTo`, and `idempotencyKey`; malformed JSON returns 400
  with a `body` field. Unknown extra fields are ignored. The field names are part of the
  contract: the integration flow (5) asserts them.
- Rate limit MUST be D1-backed: at most 10 requests per minute per IP and 200 requests per
  day per sender address. Over limit returns 429 with `Retry-After` (`60` for the IP
  limit, `86400` for the sender limit). The limit counts requests, not sends: the check
  runs before the dedupe step, so a replay of an existing key also consumes a slot
  (decision 7).
- Duplicate handling: a payload whose `idempotency_key` already exists MUST return 202
  with the existing row id and MUST NOT enqueue. This makes caller retries harmless. The
  `idempotency_key UNIQUE` constraint backs the check (3.2); the insert path MUST also
  recover from the unique-violation a racing duplicate causes, returning 202 with the
  existing row id.
- On success: generate a UUID, insert with `status = 'pending'`, enqueue, respond 202
  `{ ok: true, id }`. If the enqueue fails after the insert, the request MUST still
  return 202; the row is canonical and the reconciliation job (3.4) recovers it.
- Logs MUST NOT contain the subject, bodies, or addresses. The email id only.

### 3.2 D1 database: `email-db`

```sql
CREATE TABLE IF NOT EXISTS emails (
  id               TEXT PRIMARY KEY,           -- uuid
  idempotency_key  TEXT NOT NULL UNIQUE,       -- dedupe gate
  to_address       TEXT NOT NULL,
  from_address     TEXT NOT NULL,
  reply_to         TEXT,
  subject          TEXT NOT NULL,
  text_body        TEXT NOT NULL,
  html_body        TEXT,
  status           TEXT NOT NULL DEFAULT 'pending',  -- pending | sent | failed
  sent_at          TEXT,                       -- atomic claim (conditional UPDATE)
  processed_at     TEXT,                       -- set by consumer after success
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_emails_processed ON emails(processed_at);

CREATE TABLE IF NOT EXISTS rate_limits (
  ip         TEXT NOT NULL,
  sender     TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rate_limits_created ON rate_limits(created_at);
```

- `idempotency_key UNIQUE` is the dedupe gate; the application check (3.1) MUST be backed
  by the constraint.
- `sent_at` doubles as the send claim, the same pattern as `decision_email_at` in the
  volunteer pipeline.
- The schema ships as `migrations/0001_init.sql` and is idempotent (`CREATE ... IF NOT
  EXISTS`) so it can be re-applied. Later migrations are NOT idempotent: SQLite has no
  `ADD COLUMN IF NOT EXISTS`, so re-running them fails loudly by design, matching the
  volunteer migration convention. Apply locally with `wrangler d1 execute email-db --local`
  and to the live database with `wrangler d1 migrations apply email-db`.

### 3.3 Queue: `email-events` + `email-events-dlq`

- Message shape: `{ "type": "email.created", "emailId": "<uuid>", "occurredAt": "<ISO>" }`.
- Retry policy: 5 retries at a fixed 60-second delay, matching the volunteer queue.
- Exhausted messages MUST land in `email-events-dlq`; the consumer MUST mark the row
  `status = 'failed'` and `processed_at = 'failed'` and ack, never re-run the send.
- The same Worker MUST be registered as the consumer of BOTH queues: a second
  `[[queues.consumers]]` entry for `email-events-dlq` (drain only, no retry or DLQ
  settings). Wrangler v4 consumer entries have no binding key; the queue name arrives on
  `batch.queue`, which is how the handler routes between the main path and the failed
  marking. Without the DLQ entry, the deployed Worker never drains the DLQ, exhausted
  messages expire, and their rows stay `pending`, so the reconciliation job re-enqueues
  them forever.
- `wrangler deploy` creates both queues from the producer and consumer entries; the DLQ is
  auto-created from the `dead_letter_queue` name. No manual queue creation step.

### 3.4 Consumer (queue handler in the same Worker)

For each `email.created` message:

1. Load the row by id. Missing row: log and ack.
2. Claim the send: `UPDATE emails SET sent_at = ?, updated_at = ? WHERE id = ? AND
   sent_at IS NULL`. Zero changed rows means this attempt did not win the claim: re-read
   the row and ack. Two cases: if `processed_at` is set, there is nothing to do. If
   `sent_at` is set but `processed_at IS NULL`, an earlier attempt sent but died before
   writing completion; complete the bookkeeping (`status = 'sent'`,
   `processed_at = 'sent'`) and ack WITHOUT calling the email API, or the reconciliation
   job re-enqueues the row every 15 minutes forever. The tradeoff (a death between claim
   and send marks a row sent without sending) is accepted; see decision 8.
3. Send via the `EMAIL` binding. If `TEST_EMAIL` is set, redirect the recipient to it.
4. On send failure: release the claim (`SET sent_at = NULL WHERE id = ? AND sent_at = ?`,
   pinned to this attempt's timestamp), then throw so the queue retries.
5. On success: set `status = 'sent'` and `processed_at = 'sent'` and ack.

Reconciliation (scheduled handler, every 15 minutes): re-enqueue `email.created` for rows
older than 10 minutes with `processed_at IS NULL`, capped at 100 per run, and prune
`rate_limits` rows older than 24 hours. This job also surfaces orphaned rows (`sent_at`
set, `processed_at` NULL) for the bookkeeping in step 2.

### 3.5 Email Sending

- The `rippledcye.org` zone is already onboarded to Cloudflare Email Sending (SPF, DKIM,
  DMARC `p=reject` verified 2026-08-19). No new DNS work.
- The `send_email` binding MUST set `allowed_sender_addresses` to the `ALLOWED_SENDERS`
  list so the platform enforces the same allowlist the API checks. Wrangler config cannot
  reference vars, so the list exists in two places; a unit test MUST assert the two stay
  in sync.
- The founder creates the sender mailboxes. v1 senders are decided per product (see 6).

## 4. Configuration and secrets

All secrets live in Cloudflare Worker secrets; `.env.example` documents names only.

| Name | Kind | Purpose |
| --- | --- | --- |
| `DB` (D1 binding) | config | email-db |
| `QUEUE` (queue producer binding) | config | email-events (enqueue side; consumers attach by queue name) |
| `EMAIL` (send_email binding) | config | Email Sending from rippledcye.org |
| `EMAIL_SECRET` | secret | shared secret for `X-Email-Secret` |
| `ALLOWED_SENDERS` | config | comma-separated sender addresses callers may use |
| `DEFAULT_FROM` | config | sender used when `from` is omitted |
| `TEST_EMAIL` | config | dev-only redirect for every recipient (set to an owned inbox) |

`keep_vars = true` MUST be set as a top-level key in wrangler.toml (before any `[[...]]`
tables) so dashboard-set vars survive deploys.

`secrets.required = ["EMAIL_SECRET"]` SHOULD be set in wrangler.toml so deploys fail loudly
when the secret is missing instead of shipping a Worker that 401s every request. Local
development supplies it via `.dev.vars` (git-ignored).

## 5. Testing strategy

Deliverability-safe testing (decision 11). Hard bounces (nonexistent addresses or
domains) are added to the Cloudflare suppression list and degrade the `rippledcye.org`
sending reputation, which hurts real mail. No test MUST send to a fake, reserved, or
nonexistent address on any path that can reach Email Sending. The owned inboxes
`benxenon@gmail.com` and `rippledcye@gmail.com` are the v1 test recipients. In
particular:

- Test payloads MUST use the owned inboxes for `to`. Plus addressing (for example
  `benxenon+emailsvc1@gmail.com`) provides distinct recipients without new mailboxes.
  This applies to the integration flow even though `wrangler dev` sends nothing: if a
  remote binding is ever attached locally, the payloads must already be safe.
- The live smoke test MUST set `TEST_EMAIL` to an owned inbox and MUST unset it right
  after the run. The redirect is a safety net, not the address policy.
- Unit tests never send: the `EMAIL` binding and the queue are fakes, so synthetic
  addresses in `FakeD1` seed rows are acceptable. Nothing from this suite reaches
  Email Sending.
- The `send_email` binding MUST NOT set `remote = true` in wrangler.toml. Local
  development simulates the binding and writes the message to local files; if remote
  development is ever required, set `TEST_EMAIL` first.

- Unit tests (vitest): payload validation, secret auth, idempotent replay, rate limits,
  the claim/release cycle (including the orphan bookkeeping), DLQ marking, migration
  schema, and reconciliation (cutoff math, cap, prune). The volunteer backend's `FakeD1`
  test helper pattern applies here unchanged.
- Integration: a scripted flow against `wrangler dev` with local D1, mirroring
  `backend/scripts/integration-flow.mjs` from call-for-volunteers:
  1. POST with valid payload and secret -> 202, row in D1.
  2. Same idempotency key again -> 202 with the same id, no second queue message
     (asserted by row count; enqueue happens only on insert).
  3. Missing or wrong secret -> 401.
  4. Bad payload (bad `to`, unknown `from`, control characters) -> 400 with field errors.
  5. 11th request in a minute -> 429 with `Retry-After`, using a dedicated
     `cf-connecting-ip` header value that earlier cases do not share; the dev server sets
     no such header, so the script sets it explicitly.
  6. Send failure releases the claim so a retry re-sends. `wrangler dev` runs no queue
     consumers, so this case invokes the consumer function directly with a `FakeD1` and a
     rejecting `EMAIL` fake instead of the local queue.
  The script cleans `.wrangler/state` first, then applies `migrations/0001_init.sql` with
  `wrangler d1 execute email-db --local` (unconditional after the clean), and reads the
  secret from `.dev.vars`.
- Live: one real email per sender to the owned inboxes (`benxenon@gmail.com`,
  `rippledcye@gmail.com`), redirected via `TEST_EMAIL`, before the first product goes
  live.

## 6. Decisions

1. **Interface: HTTP + shared secret.** Stack agnostic by requirement; anything that can
   POST JSON works. A service binding MAY be added later for in-account Workers that want
   zero-auth calls, but it cannot be the only interface.
2. **Queue + D1 design copied from call-for-volunteers.** Same message shape (id only),
   same atomic claim, same DLQ and reconciliation. The patterns shipped and survived
   production the same week this spec was written.
3. **Per-form senders via an allowlist.** Callers pass `from`; the API and the send_email
   binding both enforce the allowlist, so one product cannot send as another.
4. **Idempotent replay returns the existing id** (202) instead of 409. Callers retrying
   after a network blip get the same answer and no duplicate email.
5. **Standalone repo and Worker.** No caller shares this repo's deploys, and this repo
   never imports caller code.
6. **The same Worker consumes the DLQ.** A second `[[queues.consumers]]` entry registers
   the Worker on `email-events-dlq`; the queue name on the batch routes to the `failed`
   marking path. Without the entry, exhausted messages are never marked and their rows get
   re-enqueued by reconciliation forever.
7. **Replays consume rate-limit quota.** The rate limit check runs before the dedupe
   check, so the limit counts requests, not sends. A caller retrying the same business
   event more than 200 times in a day exhausts its sender bucket. Accepted for v1; if it
   bites, move the rate-limit row insert to after the dedupe check.
8. **Orphaned claims complete, never re-send.** If a send succeeded but the completion
   write failed, the next attempt that loses the claim (`sent_at` set, `processed_at`
   NULL) finishes the bookkeeping instead of plain acking. This refines the sequence
   diagram's "claim lost" branch. Tradeoff: a death between claim and send marks a row
   sent without sending; the window is one API call, and the alternative is a permanently
   stuck row needing human triage.
9. **Structured 400 field errors.** `{ ok: false, errors: { "<field>": "<message>" } }`
   lets callers render per-field messages. The field names are asserted by the
   integration flow, so they are contract, not convenience.
10. **Pinned Retry-After values.** `60` seconds for the per-minute IP limit and `86400`
    for the daily sender limit. Fixed values keep the integration flow deterministic;
    computed values are a later refinement if callers need them.
11. **Testing never targets fake addresses.** Hard bounces from fake or nonexistent
    addresses degrade the sending domain's reputation and fill the suppression list, so
    every send path in testing uses the owned inboxes (`benxenon@gmail.com`,
    `rippledcye@gmail.com`) or a `TEST_EMAIL` redirect. Unit tests are exempt because
    nothing reaches a send path; `wrangler dev` is exempt because the binding simulates
    locally and no queue consumer runs, but its payloads still use real addresses so a
    future remote binding cannot bounce fake mail.

## 7. Out of scope for v1

- Per-caller API keys (one shared secret).
- Attachments, templates, unsubscribe handling.
- A delivery dashboard. DLQ rows surface as `status = 'failed'` in D1 and are queryable
  via the D1 console; a dashboard MAY come later if the founder needs one.
