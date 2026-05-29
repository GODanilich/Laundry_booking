create schema if not exists identity;
create schema if not exists machine;
create schema if not exists schedule;
create schema if not exists booking;
create schema if not exists events;

create table if not exists identity.users (
    id uuid primary key,
    email varchar(255) not null unique,
    password_hash text not null,
    full_name varchar(255) not null,
    role varchar(32) not null default 'USER',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists identity.refresh_tokens (
    id uuid primary key,
    user_id uuid not null references identity.users(id) on delete cascade,
    token_hash varchar(128) not null unique,
    expires_at timestamptz not null,
    revoked_at timestamptz null,
    created_at timestamptz not null default now()
);

create table if not exists machine.washing_machines (
    id uuid primary key,
    name varchar(255) not null,
    location varchar(255) not null,
    capacity_kg numeric(6,2) not null,
    status varchar(32) not null default 'ACTIVE',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists schedule.slots (
    id uuid primary key,
    machine_id uuid not null,
    starts_at timestamptz not null,
    ends_at timestamptz not null,
    status varchar(32) not null default 'FREE',
    booking_id uuid null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique(machine_id, starts_at, ends_at)
);

create table if not exists booking.bookings (
    id uuid primary key,
    user_id uuid not null,
    machine_id uuid not null,
    slot_id uuid not null,
    status varchar(32) not null,
    price numeric(10,2) not null default 100,
    idempotency_key varchar(128) null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique(user_id, idempotency_key)
);

create table if not exists events.audit_log (
    id uuid primary key,
    event_id uuid null,
    event_type varchar(128) not null,
    aggregate_id uuid null,
    user_id uuid null,
    payload jsonb not null,
    created_at timestamptz not null default now()
);

create unique index if not exists audit_log_event_id_uq
    on events.audit_log(event_id)
    where event_id is not null;

create table if not exists events.analytics_daily (
    day date primary key,
    bookings_count integer not null default 0,
    cancelled_count integer not null default 0,
    payment_success_count integer not null default 0,
    payment_failed_count integer not null default 0,
    revenue numeric(10,2) not null default 0
);

create table if not exists events.processed_events (
    event_id uuid primary key,
    processed_at timestamptz not null default now()
);

