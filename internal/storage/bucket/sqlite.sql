-- The bucket, declared once.
--
-- Postgres reaches this shape by replaying the recorded migrations, because
-- deployments exist that have to travel. SQLite has no history to carry, so it
-- states the shape directly.
--
-- Types follow what the models already write: a timestamp is the RFC3339 text
-- Value() produces, an object or array is JSON text, a volumes pair is the
-- "(input, output)" text Volumes.Value() produces, and an id is an integer.

create table if not exists "{{.Schema}}".transactions (
    ledger              text    not null,
    id                  integer not null,
    timestamp           text    default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    reference           text,
    reverted_at         text,
    updated_at          text    default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    inserted_at         text    default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    postings            text    not null,
    sources             text    not null,
    destinations        text    not null,
    sources_arrays      text    not null,
    destinations_arrays text    not null,
    metadata            text    not null default '{}',
    post_commit_volumes text,
    template            text,
    primary key (ledger, id)
);

create unique index if not exists "{{.Schema}}".transactions_reference on transactions (ledger, reference) where reference is not null and reference <> '';
create index if not exists "{{.Schema}}".transactions_date on transactions (timestamp);
create index if not exists "{{.Schema}}".transactions_id_desc on transactions (id desc);

create table if not exists "{{.Schema}}".transactions_metadata (
    seq             integer primary key autoincrement,
    ledger          text    not null,
    transactions_id integer not null,
    revision        integer not null default 0,
    date            text    not null,
    metadata        text    not null default '{}'
);

create index if not exists "{{.Schema}}".transactions_metadata_idx on transactions_metadata (ledger, transactions_id, revision desc);

create table if not exists "{{.Schema}}".accounts (
    ledger         text not null,
    address        text not null,
    address_array  text,
    insertion_date text default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at     text default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    metadata       text not null default '{}',
    first_usage    text          default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    primary key (ledger, address)
);

create index if not exists "{{.Schema}}".accounts_first_usage on accounts (first_usage);

create table if not exists "{{.Schema}}".accounts_metadata (
    seq              integer primary key autoincrement,
    ledger           text    not null,
    accounts_address text    not null,
    revision         integer not null default 0,
    date             text,
    metadata         text    not null default '{}'
);

create index if not exists "{{.Schema}}".accounts_metadata_idx on accounts_metadata (ledger, accounts_address, revision desc);

-- accounts_volumes carries the running balance. Every posting lands here, so
-- this table is what the double entry invariant is read off.
create table if not exists "{{.Schema}}".accounts_volumes (
    ledger           text not null,
    accounts_address text not null,
    asset            text not null,
    input            text not null,
    output           text not null,
    primary key (ledger, accounts_address, asset)
);

create table if not exists "{{.Schema}}".moves (
    seq                           integer primary key autoincrement,
    ledger                        text    not null,
    transactions_id               integer not null,
    accounts_address              text    not null,
    asset                         text    not null,
    amount                        text    not null,
    insertion_date                text    default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    effective_date                text    default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    post_commit_volumes           text,
    post_commit_effective_volumes text,
    is_source                     integer not null
);

create index if not exists "{{.Schema}}".moves_ledger on moves (ledger);
create index if not exists "{{.Schema}}".moves_transactions_id on moves (ledger, transactions_id);
create index if not exists "{{.Schema}}".moves_account_address on moves (ledger, accounts_address);
create index if not exists "{{.Schema}}".moves_asset on moves (ledger, asset);
create index if not exists "{{.Schema}}".moves_date on moves (effective_date);
create index if not exists "{{.Schema}}".moves_range_dates on moves (ledger, accounts_address, asset, effective_date, seq);

create table if not exists "{{.Schema}}".logs (
    ledger           text    not null,
    id               integer not null,
    type             text    not null,
    hash             blob,
    date             text    default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    data             text    not null,
    idempotency_key  text,
    memento          blob,
    idempotency_hash blob,
    schema_version   text,
    primary key (ledger, id)
);

create unique index if not exists "{{.Schema}}".logs_idempotency_key on logs (ledger, idempotency_key) where idempotency_key is not null;
create index if not exists "{{.Schema}}".logs_ids on logs (id);

create table if not exists "{{.Schema}}".schemas (
    ledger       text not null,
    version      text not null,
    created_at   text default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    chart        text not null,
    transactions text not null default '{}',
    queries      text not null default '{}',
    primary key (ledger, version)
);
