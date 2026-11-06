# payments-sim

A simulated card payment processor that keeps its books in a separate [double-entry ledger](https://github.com/flashyash/ledger)
over HTTP, and is built to stay correct when processes die and responses vanish. It covers authorize, partial capture, void,
refund, chargeback and settlement, with a fake card network, signed webhooks, daily reconciliation and a chaos harness.

No real cards, money, rails or credentials. Card "numbers" are `tok_*` strings, and the API rejects anything else.

> **How this was built:** Claude Code built this project autonomously at my request, including the design and core logic
> (state machine, ledger posting, idempotency, reconciliation), which I'd normally write myself. No load predictions were written
> down beforehand, for the same reason.

## Run it

```bash
npm install && npm run db:up && npm test
```

Needs Node ≥ 24, Docker, and [ledger](https://github.com/flashyash/ledger) checked out next to this folder with its
dependencies installed (`../ledger`, or set `LEDGER_DIR`). Postgres runs on `localhost:5434`. The ledger is used as is, through
its own server and migrations, on its own database.

| Command | What it does |
|---|---|
| `npm run demo` | Guided tour: one payment's life, a lost network response, a chargeback, an injected settlement mismatch |
| `npm run chaos` | The full chaos harness (about 5 minutes). `-- --profile ci` is the short version |
| `npm run load` | Load test. Keep the machine awake and plugged in: results are labelled invalid if it pauses |
| `npm run recon-eval` | Scores reconciliation against injected mismatches |
| `npm start` | Runs everything: API `:3100`, ledger `:3101`, network `:3102`, merchant sink `:3103` |

## How it works

```
merchant ─HTTP→ api ──┐                         ┌─→ fake network + issuer (own DB)
                      ├─ processor DB ─ engine ─┤
network ─notify→ api ─┘  worker (retries, recon,└─→ ledger (own DB, HTTP only)
sink ←signed webhooks─ outbox relay    expiry)
```

- **Every operation is a durable to-do list.** A driver holding a lease does one step, commits, and moves on. If it dies, the lease
  runs out and another process resumes at the last committed step. A fencing token stops a driver that lost its lease from
  writing anything.
- **Durable intent → external fact → books.** The ledger records what already happened, so capture and void go to the network
  first. Refunds post first, because they're the only flow with a real funds check.
- **A timeout means unknown, never failed.** The merchant gets `202`, then the outcome by webhook. Retransmits reuse the same
  reference, and the network replays its original answer.
- **The ledger enforces authorization limits by itself.** Each payment has its own no-overdraft hold account, so an over-capture
  would overdraw it and be refused, even if the state machine were wrong.

[DESIGN.md](DESIGN.md) answers the four design questions (holds without a hold concept, where re-run rejections break a flow, the state
machine, and "approved but we timed out") and records the decisions and the chart of accounts.

## API

| Method + path | Success | Errors |
|---|---|---|
| `POST /v1/merchants` | 201, returns the webhook secret | 409 `merchant_exists` |
| `POST /v1/merchants/:id/reserve` `{amount}` | 201 | 404 |
| `POST /v1/payments` `{merchantId, amount, cardToken}` | 201 `authorized`; 202 processing | 402 `card_declined` / `network_timeout` |
| `POST /v1/payments/:id/capture` `{amount?, final?}` | 201; 202 | 402, 409 `illegal_transition`, 422 `amount_exceeds_authorization` / `authorization_expired` |
| `POST /v1/payments/:id/void` | 200; 202 | 409 |
| `POST /v1/payments/:id/refund` `{amount?}` | 201; 202 | 402 `refund_declined`, 409, 422 `amount_exceeds_refundable` / `insufficient_merchant_funds` |
| `GET /v1/payments/:id` | 200 | 404 |

All `POST`s that move money need an `Idempotency-Key`. Rejections are stored too, so a retry gets the same answer even if the world
has changed since. `409 operation_in_progress` is the one answer that isn't stored: retry it with the same key. Amounts are strings of
integer minor units.

Webhooks carry `x-sim-signature: t=<unix>,v1=<hmac-sha256 of "t.body">` and `x-sim-event-id`. Delivery is at-least-once, so merchants
dedupe on the event id.

## Numbers

All measured on an Apple M5 Pro laptop, with client, services and a Postgres container (Docker VM: 2 CPUs, 4 GB) sharing the machine.

**Tests:** 171 (93 state-machine matrix, 27 flows, 14 refunds and chargebacks, 13 reconciliation, 10 webhooks, 8 fake network, 6 recovery).

**Chaos** (`npm run chaos`, [results/chaos-full.json](results/chaos-full.json)): 40 scenarios, **0 invariant violations**, every scenario
confirmed to have actually fired.

| Part | What happened |
|---|---|
| SIGKILL at each of 22 named steps across all flows | 22 of 22 hit, 0 violations |
| Lost requests, then lost responses, at each of 7 external calls (50% of matching calls) | 14 of 14 fired, 0 violations |
| 90 s of everything at once | 478 full flows, 3,346 payments, 73 random SIGKILLs across API, worker, ledger, network and sink; 0 violations |
| Sabotage controls: unstable ledger key, unstable network ref, skipped outbox | 3 of 3 caught, as 59, 39 and 21 violations |

In the 90 s storm, the faults that actually occurred were 968 network retransmits, 2,283 ops retried, 18 refunds reversed, 900 disputes, 624 notifications parked for
ordering, and 1,111 duplicate webhook deliveries (out of 9,742 events, all delivered). Longest time to go quiet after faults stopped: about 15 s, against a 60 s bound.

This storm didn't happen to exercise auth tombstones, payments failed by timeout, or a refund rejected for funds (0 of each), so those paths are covered by `npm test`
and the targeted scenarios above.

**Load** (`npm run load`: closed loop, 20 clients each running authorize → capture, 15 s, no faults; two runs per row, with the pause detector on; `failed` was 0 and
invariants held in all of them):

| Scenario | Full flows/s | Authorize p50 / p99 (ms) | Capture p50 / p99 (ms) |
|---|---|---|---|
| 16 shards, 20 merchants | 92–113 | 86–105 / 200–259 | 77–96 / 173–187 |
| 1 shard (hot accounts), 20 merchants | 92–96 | 67–93 / 222–307 | 88–132 / 306–365 |
| 16 shards, 1 merchant | 108–123 | 69–75 / 174–227 | 83–97 / 167–227 |
| 16 shards, 20 merchants, network 20–50 ms | 105–115 | 87–91 / 171–223 | 82–86 / 136–185 |

Raw output: [results/load.json](results/load.json). [results/load-sleep-interrupted.json](results/load-sleep-interrupted.json) is kept as an example of what not to trust: a run hit by laptop sleep, with a p99 of 425 s.

The client scaling table below is from an earlier set of runs:

| Clients (16 shards, 20 merchants, 12 s, one run each) | Flows/s | Authorize p50 / p99 (ms) |
|---|---|---|
| 1 | 24 | 20 / 44 |
| 5 | 92 | 28 / 51 |
| 20 | 105–121 | 85–92 / 139–262 |
| 50 | 120 | 215 / 435 |

What these show:
- A full flow (authorize + capture) costs about **43 database commits** across the four databases, webhooks included.
- Throughput tops out around **90–120 flows/s** (about 180–250 requests/s). The ledger alone did about 1,960 posts/s in its own load test, so the ledger isn't the limit.
- Sharding helped less than expected in throughput, but with 1 shard capture p99 was roughly 2× worse.
- **The bottleneck isn't identified.** It isn't commit fsync: a run with `synchronous_commit=off` was unchanged at 111 flows/s. No Node service looked saturated (about 40%
  of a core at most, from `ps`, which averages), Postgres used about 85% of the VM's 200%, and the SSH port-forward carrying all Postgres traffic used about 52% of a core. Proper profiling is the next step.

Also: webhook delivery p50 47–73 ms, p99 85–185 ms under that load. Reconciliation plus settlement took about 1.1 s for 1,800 lines.

**Reconciliation** (`npm run recon-eval`, [results/recon-eval.json](results/recon-eval.json)): over 5 simulated days and 1,007 file lines, 68 injected mismatches of 6 kinds, 68 detected,
0 false positives. Ground truth comes from the injector's manifest, which the matcher can't see. An earlier run scored one apparent miss, which was the same line being
shifted two days running: reconciliation correctly didn't re-report an exception that was still open, and the evaluator now counts those separately.

## Not verified

- **Load numbers come from one laptop,** with client, services and databases sharing it, and are not a capacity claim.

## What the build showed / what I'd do differently

_Drafted by Claude from what happened during the build._

- **The chaos harness needed its own test.** A run with zero violations means nothing until sabotage runs prove the checker can see violations.
- **Bugs found on the way:** an app-clock cutoff left just-applied captures out of their own settlement file (the fix uses the database's clock); a final outcome returned as `504` made well-behaved clients
  retry forever (now `402`); one dropped read failed an entire reconciliation file (now retried, and unknown is never reported as a mismatch).
- **Two clocks caused real bugs, twice.** Comparing database timestamps with the app's clock broke settlement cutoffs, then auth expiry and the auth deadline. Those two only showed up when the laptop sleeping made the Docker VM's clock drift. The database's clock now judges everything it stamped.
- **The first explanation of a slow run is usually wrong.** Commit fsync looked like the throughput limit, and a run with it off showed it wasn't. Stalls first looked like overlapping jobs, then a slow disk, and the power log showed the
  laptop going to sleep. Load tests now record pauses so a bad run gets labelled by evidence.
- **Tight timeouts made tests flaky under load.** Deadline tests now move the deadline in the database instead of waiting on a clock.
- **Differently next time:** one commit per step is simple and costs about 43 commits per flow, so I'd batch steps that have no external call between them. I'd also add ledger support for final rejections and lookup by key (proposals in DESIGN.md), and let
  refunds draw on pending funds.
