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

**Tests:** 174 (93 state-machine matrix, 27 flows, 14 refunds and chargebacks, 13 reconciliation, 10 webhooks, 8 fake network, 6 recovery, 3 sabotage).

**Chaos** (`npm run chaos`, [results/chaos-full.json](results/chaos-full.json)): 40 scenarios, **0 invariant violations**, every scenario
confirmed to have actually fired.

| Part | What happened |
|---|---|
| SIGKILL at each of 22 named steps across all flows | 22 of 22 hit, 0 violations |
| Lost requests, then lost responses, at each of 7 external calls (50% of matching calls) | 14 of 14 fired, 0 violations |
| 90 s of everything at once | 586 full flows, 4,102 payments, 70 random SIGKILLs across API, worker, ledger, network and sink; 0 violations |
| Sabotage controls: unstable ledger key, unstable network ref, skipped outbox | 3 of 3 caught, as 85, 42 and 21 violations. The sabotage now lives in the harness (wire rewrites and a database rule), so services carry no sabotage code |

In the 90 s storm, the faults that actually occurred were 1,195 network retransmits, 2,619 ops retried, 30 refunds reversed, 1,081 disputes, 742 notifications parked for
ordering, and 1,331 duplicate webhook deliveries (out of 11,902 events, all delivered). Longest time to go quiet after faults stopped: about 15 s, against a 60 s bound.

This storm didn't happen to exercise auth tombstones, payments failed by timeout, or a refund rejected for funds (0 of each), so those paths are covered by `npm test`
and the targeted scenarios above.

**Load** (`npm run load`: closed loop, 20 clients each running authorize → capture, 15 s, no faults; two runs per row, with the pause detector on; `failed` was 0 and
invariants held in all of them):

| Scenario | Full flows/s | Authorize p50 / p99 (ms) | Capture p50 / p99 (ms) |
|---|---|---|---|
| 16 shards, 20 merchants | 109–122 | 81–92 / 167–200 | 74–83 / 150 |
| 1 shard (hot accounts), 20 merchants | 92–100 | 59–88 / 205–319 | 81–137 / 302–421 |
| 16 shards, 1 merchant | 126–140 | 51–58 / 145–224 | 79–82 / 197–208 |
| 16 shards, 20 merchants, network 20–50 ms | 101–117 | 82–98 / 180–215 | 78–92 / 152–169 |

Raw output: [results/load.json](results/load.json). [results/load-sleep-interrupted.json](results/load-sleep-interrupted.json) is kept as an example of what not to trust: a run hit by laptop sleep, with a p99 of 425 s.

The client scaling table below is from an earlier set of runs, before the query changes described below:

| Clients (16 shards, 20 merchants, 12 s, one run each) | Flows/s | Authorize p50 / p99 (ms) |
|---|---|---|
| 1 | 24 | 20 / 44 |
| 5 | 92 | 28 / 51 |
| 20 | 105–121 | 85–92 / 139–262 |
| 50 | 120 | 215 / 435 |

What these show:
- A full flow (authorize + capture) costs about **41 database commits** across the four databases, webhooks included.
- Throughput tops out around **90–140 flows/s** (about 180–280 requests/s). The ledger alone did about 1,960 posts/s in its own load test, so the ledger isn't the limit.
- Sharding helped less than expected in throughput, but with 1 shard capture p99 was roughly 2× worse.
- **The ceiling at this setup is the laptop's Docker path, not the payment code.** Each limit below only showed up after removing the one above it (single 15 s runs, 20 clients; raw files in [results/profiling](results/profiling)):
  1. **Docker VM CPU, at 2 vCPUs.** The VM was 3–4% idle. Postgres used about 100 of its 200%, while sshd, docker-proxy and softirq together used about 110. Close to half the VM goes on moving bytes. A flow makes about 114 client round trips, and each statement took about 1.7 ms from the client against about 0.11 ms of Postgres execution.
  2. **The SSH port-forward, with more vCPUs.** 2 vCPUs: 101–118 flows/s. 4: 156–171. 8: 166–175, with the VM 45% idle. Inside the 8-vCPU VM with no forward: 437–515 flows/s, though webhook delivery backed up (p50 1.5–2.8 s).
  3. **Durable commit on native Postgres, with no VM at all.** `F_FULLFSYNC`: 152 flows/s, about 3.1 ms per WAL flush. Plain macOS fsync, which doesn't flush the drive cache and so isn't a durable result: 421 flows/s, with the API event loop saturated.
- **What it isn't:** Node services at the 2-vCPU setting (API event-loop use about 0.45), connection-pool waits (0.02 ms), or commit fsync at that setting (`synchronous_commit=off` gave no gain).
- **Fewer round trips, gain not settled.** Prepared statements, single fenced statements for step updates, and less frequent lease renewal cut commits from about 43 to about 41 per flow, with no durability change. An interleaved A/B (6 runs per side) showed about +28% (mean 102 to 130 flows/s), or +19% with network latency, but the separate full load runs above show no clear gain. Treat it as somewhere between 0 and 28%.

Also: webhook delivery p50 47–78 ms, p99 87–164 ms under that load. Reconciliation plus settlement took about 1.1 s for 1,800 lines.

**Reconciliation** (`npm run recon-eval`, [results/recon-eval.json](results/recon-eval.json)): over 5 simulated days and 1,005 file lines, 60 injected mismatches of 6 kinds, 60 detected,
0 false positives. Ground truth comes from the injector's manifest, which the matcher can't see. An earlier run scored one apparent miss, which was the same line being
shifted two days running: reconciliation correctly didn't re-report an exception that was still open, and the evaluator now counts those separately.

## Not verified

- **Load numbers come from one laptop,** with client, services and databases sharing it, and are not a capacity claim. Other CPU-heavy jobs ran during some profiling runs; those rounds are excluded.

## What the build showed / what I'd do differently

_Drafted by Claude from what happened during the build._

- **The chaos harness needed its own test.** A run with zero violations means nothing until sabotage runs prove the checker can see violations.
- **Bugs found on the way:** an app-clock cutoff left just-applied captures out of their own settlement file (the fix uses the database's clock); a final outcome returned as `504` made well-behaved clients
  retry forever (now `402`); one dropped read failed an entire reconciliation file (now retried, and unknown is never reported as a mismatch).
- **Two clocks caused real bugs, twice.** Comparing database timestamps with the app's clock broke settlement cutoffs, then auth expiry and the auth deadline. Those two only showed up when the laptop sleeping made the Docker VM's clock drift. The database's clock now judges everything it stamped.
- **The first explanation of a slow run is usually wrong.** Commit fsync looked like the throughput limit, and a run with it off showed it wasn't. Stalls first looked like overlapping jobs, then a slow disk, and the power log showed the
  laptop going to sleep. Load tests now record pauses so a bad run gets labelled by evidence.
- **Tight timeouts made tests flaky under load.** Deadline tests now move the deadline in the database instead of waiting on a clock.
- **Differently next time:** one commit per step is simple and costs about 41 commits per flow, so I'd batch steps that have no external call between them. I'd also add ledger support for final rejections and lookup by key (proposals in DESIGN.md), and let
  refunds draw on pending funds.
