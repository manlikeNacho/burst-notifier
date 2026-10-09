# burst-notifier

A small Go service that turns a burst of events into **one** notification.

If a partner receives 400 orders in a minute, they should get one message that says "400 new orders", not 400 pings. burst-notifier collects events per key, waits for things to go quiet, then delivers a single signed webhook with a count, a per-type breakdown and a few sample events.

## How the debounce works

Each key (a partner, an order, a user) has its own window.

- **Quiet window** (`QUIET_WINDOW`, default 5s): the window closes once no event has arrived for this long.
- **Max wait** (`MAX_WAIT`, default 30s): a hard cap since the first event, so a steady trickle can never delay a notification forever. Without it, one event every 4 seconds would postpone delivery indefinitely.

The batch carries `reason: "quiet" | "max_wait"` so you can see which rule fired.

## Design decisions

| Concern | Decision |
|---|---|
| Producer retries | Events with an `id` (or an `Idempotency-Key` header) are deduped for 10 minutes. A retry returns `200 {"duplicate": true}` and is not counted twice. |
| Receiver retries | Failed deliveries retry with exponential backoff (2s, 4s, 8s...) up to 5 attempts, then go to a dead-letter hook (`OnDead`, logged by default). |
| Duplicate deliveries | `X-Batch-Id` is deterministic per window (`key@firstEventNanos`) and **stable across retries**, so a receiver can dedupe. |
| Authenticity | Set `WEBHOOK_SECRET` and every delivery carries `X-Signature: sha256=<hmac of body>`. |
| Slow receivers | Sends happen outside the lock, so a slow webhook never blocks `POST /events`. |
| Testability | The clock is injected. The debounce tests simulate minutes of traffic in microseconds and never `sleep`. |

## Run it

```bash
go run ./cmd/burst-notifier
```

With no `WEBHOOK_URL`, the service delivers to its own built-in demo receiver, so one process shows the whole flow:

```bash
# send a burst
for i in $(seq 1 100); do
  curl -s -X POST localhost:8080/events \
    -d "{\"id\":\"evt-$i\",\"key\":\"partner-1\",\"type\":\"order.created\"}" &
done; wait

# ~5s later: one batch, count=100
curl localhost:8080/demo/received
```

## API

`POST /events` (requires `Authorization: Bearer $API_KEY` when `API_KEY` is set)

```json
{ "id": "evt-1", "key": "partner-1", "type": "order.created", "payload": { "order": 1 } }
```

Returns `202` when accepted, `200` for a duplicate, `400` for bad input, `401` when unauthorized.

`GET /healthz` returns `{"ok": true, "pending_keys": N}`.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | HTTP port |
| `WEBHOOK_URL` | built-in demo receiver | where batches are delivered |
| `WEBHOOK_SECRET` | none | HMAC signing key |
| `API_KEY` | none | Bearer token for `POST /events` |
| `QUIET_WINDOW` | `5s` | quiet period that closes a window |
| `MAX_WAIT` | `30s` | maximum age of a window |
| `MAX_SAMPLES` | `5` | sample events kept per batch |

## Deploy to Render

`render.yaml` is included. Create a Blueprint from this repo, and Render builds the Dockerfile and generates an `API_KEY`. Leave `WEBHOOK_URL` unset to use the demo receiver.

## Tests

```bash
go test -race ./...
```

Covered: a 100-event burst becoming one batch, max-wait stopping starvation, independent keys, duplicate event IDs, retry backoff, dead-lettering after the last attempt, stable batch IDs across retries, 1,000 concurrent adds, webhook signing, and the HTTP auth and idempotency paths.

## Limits and what I'd do next

- **State is in memory.** It runs as a single instance, and a restart loses open windows. Moving pending state behind a `Store` interface backed by Redis would allow multiple instances and survive restarts.
- The outbox is in memory too, so a crash mid-retry loses that batch. A durable queue would fix that.
- The demo receiver is for demos only and isn't meant for production use.
