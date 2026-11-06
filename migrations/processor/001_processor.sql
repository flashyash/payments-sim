create table merchants (
  id             text primary key,
  webhook_url    text,
  webhook_secret text not null,
  created_at     timestamptz not null default now()
);

-- name → ledger account id. The ledger has no lookup-by-name, so this is our chart of accounts.
create table ledger_accounts (
  name text primary key,
  id   uuid not null
);

create table payments (
  id                uuid primary key,
  merchant_id       text not null references merchants(id),
  currency          char(3) not null,
  card_token        text not null,
  status            text not null check (status in
    ('authorizing','authorized','partially_captured','captured','partially_refunded','refunded',
     'voided','expired','declined','failed')),
  amount_authorized bigint not null check (amount_authorized > 0),
  amount_captured   bigint not null default 0,
  amount_released   bigint not null default 0,   -- hold given back by void/expiry/final capture
  amount_refunded   bigint not null default 0,
  amount_disputed   bigint not null default 0,   -- open or lost chargebacks
  reserved_capture  bigint not null default 0,   -- claimed by the in-flight capture
  reserved_refund   bigint not null default 0,   -- claimed by the in-flight refund
  hold_account_id   uuid,
  auth_ref          text not null unique,
  expires_at        timestamptz not null,
  event_seq         int not null default 0,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now(),
  -- The local enforcement of "never capture past the authorization / refund past the capture".
  check (amount_captured >= 0 and amount_released >= 0 and amount_refunded >= 0 and amount_disputed >= 0),
  check (reserved_capture >= 0 and reserved_refund >= 0),
  check (amount_captured + reserved_capture + amount_released <= amount_authorized),
  check (amount_refunded + reserved_refund <= amount_captured)
);
create index payments_expiry on payments (expires_at) where status in ('authorized','partially_captured');

create table disputes (
  id          uuid primary key,          -- the network's dispute id
  payment_id  uuid not null references payments(id),
  amount      bigint not null check (amount > 0),
  state       text not null check (state in ('open','won','lost')),
  last_seq    int not null
);

-- One durable unit of work. A driver holding the lease walks it through `step`; any process can take over.
create table operations (
  id              uuid primary key,
  payment_id      uuid references payments(id),
  merchant_id     text not null,
  kind            text not null check (kind in
    ('authorize','capture','void','expire','refund','chargeback_open','chargeback_won','settlement')),
  amount          bigint,
  final_capture   boolean not null default false,
  dispute_id      uuid,
  step            text not null,
  state           text not null check (state in ('in_progress','succeeded','failed','stuck')),
  error_code      text,
  network_ref     text unique,
  network_at      timestamptz,
  attempts        int not null default 0,
  next_attempt_at timestamptz not null default now(),
  lease_token     bigint not null default 0,  -- fencing token: every write checks it
  lease_until     timestamptz,
  deadline_at     timestamptz,
  result          jsonb,                      -- frozen API response, so replays are byte-stable
  created_at      timestamptz not null default now(),
  finished_at     timestamptz
);
create index operations_due on operations (next_attempt_at) where state = 'in_progress';
-- At most one merchant-initiated operation in flight per payment. Serializes capture/void/refund races.
create unique index operations_one_active on operations (payment_id)
  where state = 'in_progress' and kind in ('authorize','capture','void','expire','refund');

-- Merchant-facing idempotency. Unlike the ledger's table, rejections are stored here and stay final.
create table idempotency_keys (
  merchant_id     text not null,
  key             text not null,
  request_hash    text not null,
  operation_id    uuid references operations(id),
  response_status int,
  response_body   jsonb,
  created_at      timestamptz not null default now(),
  primary key (merchant_id, key)
);

-- Exactly what we will send the ledger, written before the first attempt. Retries resend this row verbatim,
-- because the ledger hashes the whole body: a regenerated timestamp or description would be a 409.
create table ledger_postings (
  key           text primary key,
  operation_id  uuid not null references operations(id),
  purpose       text not null,
  body          jsonb not null,
  reverses_txn  uuid,
  state         text not null check (state in ('pending','posted','rejected')),
  ledger_txn_id uuid,
  rejection     text,
  created_at    timestamptz not null default now(),
  posted_at     timestamptz
);
create index ledger_postings_op on ledger_postings (operation_id);

-- Transactional outbox: written in the same local transaction as the state change it announces.
create table outbox (
  id              bigserial primary key,
  event_id        uuid not null unique,
  merchant_id     text not null,
  payment_id      uuid,
  type            text not null,
  payload         jsonb not null,
  attempts        int not null default 0,
  next_attempt_at timestamptz not null default now(),
  lease_until     timestamptz,
  last_error      text,
  created_at      timestamptz not null default now(),
  delivered_at    timestamptz
);
create index outbox_due on outbox (next_attempt_at) where delivered_at is null;

-- Inbox for network notifications. message_id dedupes; seq reorders.
create table network_inbox (
  message_id   uuid primary key,
  type         text not null,
  payload      jsonb not null,
  dispute_id   uuid,
  seq          int,
  state        text not null default 'pending' check (state in ('pending','processed','ignored')),
  parked_count int not null default 0,
  received_at  timestamptz not null default now(),
  processed_at timestamptz
);
create index network_inbox_pending on network_inbox (received_at) where state = 'pending';

-- One row per money movement the settlement file should contain.
create table settlement_items (
  ref           text primary key,      -- the network message ref
  type          text not null check (type in ('capture','refund','chargeback','chargeback_reversal')),
  merchant_id   text not null,
  payment_id    uuid not null references payments(id),
  amount        bigint not null check (amount > 0),
  shard         int not null,
  ledger_txn_id uuid not null,
  network_at    timestamptz not null,
  state         text not null default 'unsettled' check (state in ('unsettled','settling','settled')),
  file_id       uuid,
  fee           bigint,
  settle_op_id  uuid,
  created_at    timestamptz not null default now()
);
create index settlement_items_open on settlement_items (network_at) where state = 'unsettled';

create table recon_runs (
  file_id        uuid primary key,
  sha256         text not null,
  cutoff         timestamptz not null,
  lines          int not null,
  matched        int not null,
  exceptions     int not null,
  resolved       int not null default 0,
  created_at     timestamptz not null default now()
);

create table recon_exceptions (
  id          bigserial primary key,
  file_id     uuid not null references recon_runs(file_id),
  ref         text not null,
  type        text not null check (type in ('missing_in_file','missing_in_books','amount_mismatch','fee_mismatch',
                                            'duplicate_in_file','already_settled','ledger_mismatch','merchant_mismatch')),
  expected    jsonb,
  actual      jsonb,
  state       text not null default 'open' check (state in ('open','resolved')),
  resolution  text,
  created_at  timestamptz not null default now(),
  unique (file_id, ref, type)
);
