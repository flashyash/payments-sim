-- The fake merchant's webhook endpoint. Records every attempt that reached it, so the harness can audit delivery.
create table deliveries (
  id          bigserial primary key,
  event_id    text not null,
  merchant_id text not null,
  type        text,
  valid_sig   boolean not null,
  response    int not null,
  received_at timestamptz not null default now()
);
create index deliveries_event on deliveries (event_id);

create table secrets (
  merchant_id text primary key,
  secret      text not null
);

-- What a correct merchant does with at-least-once delivery: apply each event id once.
create table processed_events (
  event_id     text primary key,
  merchant_id  text not null,
  type         text not null,
  processed_at timestamptz not null default now()
);
