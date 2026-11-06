-- The fake card network + issuer bank. Durable, so it can be killed like everything else.
create table cards (
  token     text primary key,
  available bigint not null check (available >= 0)
);

-- Every message the network has processed, keyed by the sender's reference: this is what makes retransmits safe.
create table messages (
  ref          text primary key,
  type         text not null check (type in ('auth','capture','void','refund','chargeback','chargeback_reversal')),
  auth_ref     text,
  merchant_id  text,
  card_token   text,
  amount       bigint not null,
  status       text not null check (status in ('approved','declined','tombstoned')),
  reason       text,
  remaining    bigint,            -- auth only: still capturable at the issuer
  params_hash  text not null,
  duplicates   int not null default 0,
  applied_at   timestamptz not null default now(),
  settlement_file_id uuid
);
create index messages_unsettled on messages (applied_at) where settlement_file_id is null and status = 'approved';
create index messages_auth on messages (auth_ref);

create table disputes (
  id       uuid primary key default gen_random_uuid(),
  auth_ref text not null,
  amount   bigint not null,
  state    text not null check (state in ('open','won','lost'))
);

-- Outgoing notifications to the processor. Deliver_after and dup_of give duplicates and reordering.
create table notifications (
  id            bigserial primary key,
  message_id    uuid not null,   -- same id on a duplicate: the processor dedupes on it
  type          text not null,
  payload       jsonb not null,
  deliver_after timestamptz not null default now(),
  delivered_at  timestamptz
);
create index notifications_due on notifications (deliver_after) where delivered_at is null;

create table settlement_files (
  id         uuid primary key default gen_random_uuid(),
  cutoff     timestamptz not null,
  csv        text not null,
  manifest   jsonb not null,  -- what was deliberately broken; only tests and the harness read this
  created_at timestamptz not null default now()
);
