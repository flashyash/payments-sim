# payments-sim: design

_Written and decided by Claude in fully autonomous build mode, at Yash's request. That instruction waived his usual
drafting of the design and core logic, the concept note and the written predictions. Nothing here touches real cards,
money or rails._

## What it is
A simulated card **processor (acquirer side)**. Merchants call it to authorize, capture, void and refund. It talks to
a fake card network + issuer over HTTP and keeps its books in the existing `../ledger` service, over the ledger's public
HTTP API only. The ledger is not forked, copied or changed.

```
merchant ─HTTP→ api ──┐                     ┌─→ fake network + issuer (own DB)
                      ├─ processor DB ─ engine ─┤
network ─notify→ api ─┘   worker (retries,  └─→ ledger (own DB, HTTP only)
sink ←signed webhooks─ outbox relay  recon, expiry)
```

## The four design questions

### 1. The ledger has no hold. How do authorizations reserve funds?
**The processor doesn't reserve anyone's money, because it doesn't have it.** The real hold lives at the issuer: an
auth lowers the cardholder's available credit there, and no money moves until settlement. So what we need is not a
reservation of funds but an **enforceable record of a promise**, and the ledger's existing overdraft guard provides one
without any change:

- Each payment gets its own **hold account** (no overdraft), a memo account with no cash meaning. Authorizing posts
  `hold +A`, `hold_contra −A`. Capturing C takes `hold −C` in the same ledger transaction that credits the merchant.
- Capturing past the authorization would overdraw the hold, so **the ledger itself rejects it**, under its row lock,
  even if our state machine is wrong. The same trick as the fund layer's `uncalled` accounts.
- Local counters (`amount_captured + reserved_capture + amount_released <= amount_authorized`, a DB `CHECK`) reject it
  first with a good error. The issuer checks a third time. Three independent records, which the harness compares.

Cost: one extra `POST /accounts` per auth, and that endpoint isn't idempotent, so a crash can orphan an empty account
(harmless; see "Proposed ledger changes"). Hold accounts stay in the ledger forever.

### 2. The ledger doesn't store keys for rejected requests, so a retry re-runs. Where does that break?
Verified in `ledger/src/ledger.ts` (the key row rolls back with the rejection). A 201 is safe to retry; a 422 is not
a stable answer. It breaks in four places:

| Scenario | What goes wrong | What we do |
|---|---|---|
| Refund gets 422 `insufficient_funds`, merchant is told "failed", then a blind retry after the merchant's balance grew | Refund posts after we said it failed | Outcome is stored in *our* DB and is final. Terminal ops are never re-driven. Merchant retries with the same key get the stored 422. |
| Crash after the ledger's 422, before we record it | The retry may now succeed | Allowed: nobody was told the first answer. The first outcome *we record* is the truth. |
| Two drivers: A's lease expired, B re-drives while A's request is still in flight | B records 422, then A's late POST commits | Fencing token stops A's *local* write, but can't recall a POST already on the wire. Client timeout (2 s) ≪ lease (10 s) shrinks the window to near zero. Closing it fully needs ledger support: see below. |
| Capture gets a 422 after the network already took it | Money moved, books refuse it | Must never happen; network-first means the books can't veto a fact. The op goes `stuck` rather than guessing, and the invariant checker fails the run. |

Also: the ledger hashes the *whole body* including `description` and `effectiveAt`, so a retry with a fresh timestamp
is a 409. Every posting is written to `ledger_postings` before the first attempt and resent verbatim.

### 3. State machine and illegal transitions
See `src/stateMachine.ts`, tested as a full matrix (every status × every command).

```
authorizing ─┬→ authorized ─┬→ partially_captured ─┬→ captured ─┬→ partially_refunded ─→ refunded
             ├→ declined    ├→ captured            │            └→ refunded
             └→ failed      ├→ voided              └→ captured (void/expiry closes the remainder)
                            └→ expired
```

Illegal, returned as a stored 409 (`illegal_transition`): capture after void/expiry/decline; void after full capture
(that's a refund); refund before anything is captured, or while partially captured (close it first); anything on a terminal
state. Amount rules (422): capture > remaining, refund > captured − refunded − disputed. Chargebacks sit outside this
machine: they are facts the network imposes, so they can't be rejected, and they get a separate `open → won | lost`
dispute record.

Two levels of machine: the **payment** status above is what merchants see; each **operation** walks internal steps
(`network → post`, etc.) so a crash resumes at the right step. One merchant op per payment at a time, enforced by a
partial unique index; a concurrent one gets a non-final `409 operation_in_progress`.

### 4. The network approved but we timed out. What happens?
A timeout means **unknown**, never "failed". The network dedupes on our `ref`, so retransmitting the same ref is safe
and returns the original answer (the network's message log is the idempotency store):

- **API:** the merchant gets `202 processing`, then the outcome by webhook. We never guess.
- **Resolved inside the deadline (15 s):** the retransmit returns "approved"; we open the hold and carry on, as if the
  response had been slow.
- **Past the deadline:** we send a **reversal** for the ref and mark the payment `failed`. If the auth had landed, the
  cardholder's hold is released. If it hadn't, the reversal leaves a **tombstone**, so a late arrival is declined
  instead of creating a hold nobody knows about. Without the tombstone, the reversal could overtake the auth and
  strand the cardholder's credit, which the harness checks for as "orphan issuer holds".
- Capture, void and refund have no deadline: they retransmit until they get an answer, since giving up would leave
  money and books disagreeing.

## Ordering rule
**Durable intent → external fact → books.** Every op commits its intent locally before anything external happens. The
ledger records what already happened, so capture and void go network-first. The one exception:

- **Refund posts first**, because it's the only op with a real funds check. If the merchant can't cover it, nothing
  has left the building. If the network then declines, we post the ledger's reversal.

## Data model
Details in `migrations/processor/001_processor.sql`. Main tables: `payments`, `operations` (leased, fenced work items),
`ledger_postings` (exact bodies, keyed), `idempotency_keys` (merchant-facing, stores rejections), `outbox`,
`network_inbox`, `settlement_items`, `recon_runs`, `recon_exceptions`.

### Chart of accounts
The ledger's overdraft guard stops a balance going below zero, and we want it on what we owe merchants. So here a
**positive balance is a credit** (what we owe); assets carry negative balances on `allow_negative` accounts.

| Account | Overdraft | Meaning |
|---|---|---|
| `hold:<payment>` | guarded | Authorized, not yet captured or released (memo) |
| `hold_contra:<shard>` | allowed | Other side of holds (memo) |
| `merchant:<id>:pending` | guarded | Captured, not yet settled |
| `merchant:<id>:available` | guarded | Settled. Refunds draw only from here |
| `merchant:<id>:dispute_debt` | allowed | Chargebacks. Goes negative legitimately |
| `network_receivable:<shard>` | allowed | What the network owes us (negative) |
| `bank_cash` | allowed | Cash at our bank (negative = we hold it) |

| Event | Entries |
|---|---|
| Authorize A | `hold +A`, `hold_contra −A` |
| Capture C (R released if final) | `hold −(C+R)`, `hold_contra +(C+R)`, `pending +C`, `receivable −C` |
| Void, R remaining | `hold −R`, `hold_contra +R` |
| Refund R | `available −R`, `receivable +R` |
| Chargeback X | `dispute_debt −X`, `receivable +X` |
| Chargeback won X | `dispute_debt +X`, `receivable −X` |
| Settle capture C, fee F | `receivable +C`, `bank_cash −(C−F)`, `pending −C`, `available +(C−F)` |
| Settle refund / chargeback X | `receivable −X`, `bank_cash +X` |
| Settle chargeback won X | `receivable +X`, `bank_cash −X` |

## Invariants (checked by `src/invariants.ts` after every chaos run)
1. **Ledger balances:** every transaction sums to zero; cached balances match entries.
2. **Nothing captured twice:** one approved network capture per capture op, and network total = local total = ledger total for each payment.
3. **Nothing captured beyond the authorization,** on all three records.
4. **Holds agree:** each hold account's balance = authorized − captured − released.
5. **No lost or duplicated postings:** every ledger key we used maps to exactly one transaction, and every ledger transaction with our prefix is known locally.
6. **No orphan issuer holds:** the issuer's outstanding holds = our open authorizations, and no card's credit leaked.
7. **Every webhook delivered:** every outbox event reached the merchant with a valid signature, and none is undelivered at quiesce.
8. **Nothing left running:** no op in progress or `stuck`, no inbox message unprocessed.
9. **Merchant and clearing balances:** each ledger balance = what the local records imply.

"Eventually delivered" needs a bound to be testable: after faults stop and processes are back, the system must quiesce
within the harness timeout (60 s), and every webhook must be delivered by then.

## Decisions
| Decision | Why | Alternative rejected |
|---|---|---|
| Acquirer's books only | That's the role of a processor; the issuer's side is in the fake network | Modelling cardholder balances in the ledger: conflates two companies' books |
| Separate database per service | Forces HTTP boundaries, makes "no shared transaction" real | Shared DB: would hide exactly the failure modes the project is about |
| Leased, fenced `operations` table, no queue | Postgres is already the source of truth; no second system to keep consistent, no dual write | Kafka/Redis: more moving parts, same at-least-once |
| API drives inline, worker recovers | Low latency on the happy path, same code on the recovery path, so recovery gets exercised | Worker only: adds polling latency to every call |
| One in-flight op per payment | Eliminates capture/void/refund races by construction; matches Stripe's lock-conflict behaviour | Allowing concurrent partial captures: more throughput on a path no merchant needs |
| Rejections stored and final on our side | Answers question 2; a merchant must never see a "failed" refund later succeed | Passing the ledger's semantics through |
| Refunds draw only from `available` | One guarded account, a real rejection path, and no pending-balance edge cases when lines settle out of order | Drawing from pending too: settlement could then be overdrawn by file ordering |
| Separate `dispute_debt` account | The overdraft flag is per account and fixed at creation. Chargebacks must go negative; refunds must not | One merchant account: either refunds can overdraw or chargebacks can fail |
| Hot accounts sharded (16) by payment id | The ledger's measured hot-account ceiling was ~585 req/s. Load test compares 1 vs 16 | No sharding: every posting would serialize on two rows |
| Network fees passed through by netting at settlement | Keeps pricing out of scope; fee is on the file and in `settlement_items` | Fee revenue accounts: no learning value here |
| Webhooks retry forever with capped backoff | The goal is every webhook delivered; age of the oldest undelivered event is the alarm | Dead-letter queue: an operational choice; add when there's an operator |
| Chargeback ordering by per-dispute `seq`, parked in inbox | Network ordering isn't guaranteed; "won" can arrive before "opened" | Applying on arrival: corrupts debt |
| Settlement posts matched lines only; exceptions stay open | Never move a merchant's money on a line we couldn't verify | All-or-nothing per file: one bad line would block every merchant |
| Dropped-response faults via an HTTP proxy, crashes via SIGKILL failpoints | Same code runs in prod and chaos; proxies test the real client | Mocks: wouldn't test real socket behaviour |
| Compose runs Postgres only; services run on the host | The harness has to SIGKILL and restart them quickly; containers would slow every kill and add nothing | Containerizing each service |
| Merchant secrets stored plaintext, generated per merchant | Simulator with fake merchants, nothing real to protect. Production would use a KMS | — |

## Proposed ledger changes (none made)
payments-sim works against the ledger as it stands at `a704449`. These would each be a separate change to `ledger`:

1. **`Idempotency-Mode: final` to persist rejections per key.** Closes the one race fencing can't (question 2, row 3).
2. **`GET /transactions?idempotencyKey=` over HTTP** (`findIdempotentResult` exists but is library-only). Today the only way to ask "did this post?" is to re-POST, which can post it.
3. **Idempotency keys on `POST /accounts`.** Prevents orphan hold accounts.
4. **`GET /accounts/:id/entries` and transaction listing.** Reconciliation can only verify IDs we stored; the harness reads the ledger's DB directly to audit for strays, and a real service couldn't.
5. **Partial reversals.** Not needed by this design, since partial amounts are ordinary transactions.

## Known gaps
- No merchant auth on the API, and network notifications aren't signed: anyone who can reach the port can act as anyone.
- USD only. Network fees are a flat bps; no interchange tables, no processor margin.
- No merchant payouts: `available` accumulates. No chargeback representment evidence flow; the win/loss just arrives.
- Reconciliation exceptions are reported, not auto-corrected. No operator UI to resolve them, apart from timing differences, which clear themselves.
- A chargeback can exceed the remaining refundable amount (refund then dispute = double payment). It's accepted as a fact; a real system would alert.
- Hold accounts grow forever (one ledger account per auth).

## Done means
| Target | Measure |
|---|---|
| Invariants hold under chaos | `npm run chaos`: a crash at every failpoint, process kills and dropped responses, zero violations |
| The checker can actually catch bugs | Sabotage run with idempotency broken must report violations |
| Concurrency and failure paths tested | `npm test`: races, replays, every illegal transition, fault paths |
| Measured latency and throughput | `npm run load`: p50/p99 and throughput in the README, never estimated |
| Reconciliation finds what was injected | Every injected mismatch detected, no false positives |

## Milestones
- [x] M1 Scaffold + fake network + issuer (plumbing)
- [ ] M2 State machine, ledger posting, authorize/capture/void, idempotency (core)
- [ ] M3 Refunds, chargebacks, out-of-order inbox (core)
- [ ] M4 Transactional outbox, signed webhooks (plumbing + core)
- [ ] M5 Settlement + reconciliation with injected mismatches (core)
- [ ] M6 Chaos harness + invariant checker + sabotage control
- [ ] M7 Load test, README numbers, SKILLS row, bullets
