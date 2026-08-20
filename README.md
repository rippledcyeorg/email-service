# email-service

Queue-based email delivery for RipplED products. Any form, API, or script can send email
through one secure endpoint. Stack agnostic by design.

The spec is [docs/backend-spec.md](docs/backend-spec.md). It is a draft for review; no
implementation exists yet. Read the spec before building.

## Design in one paragraph

Callers POST `{to, from, subject, text, html?, replyTo?, idempotencyKey?}` to `POST /send`
with an `X-Email-Secret` header. The Worker validates, dedupes by idempotency key in D1,
enqueues an id-only message, and returns 202 immediately. The consumer claims the send
atomically and delivers via Cloudflare Email Sending from the already onboarded
`rippledcye.org` domain. Retries and a dead letter queue carry the same guarantees the
volunteer pipeline proved in production.

## Status

- Spec: draft, awaiting review (this repo)
- Implementation: not started

## Commands

Nothing to run yet. Once implemented, the target is three commands:

```
npm test            # unit + integration tests
npm run integration # scripted flow against wrangler dev with local D1
npx wrangler deploy # deploy the worker from this directory
```
