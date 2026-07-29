-- The system bucket, declared once. See internal/storage/bucket/sqlite.sql for
-- why SQLite states its shape rather than replaying a history.
--
-- A ledger's id is the engine's own row counter here, where Postgres reads it
-- off a sequence; the name still identifies a ledger, so it carries the unique
-- index the model expects.

create table if not exists "_system".ledgers (
    id         integer primary key autoincrement,
    name       text not null unique,
    added_at   text default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    bucket     text not null,
    metadata   text not null default '{}',
    features   text,
    state      text not null default 'initializing',
    deleted_at text
);

create table if not exists "_system".exporters (
    id         text primary key,
    driver     text,
    config     text,
    created_at text default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

create table if not exists "_system".pipelines (
    id          text primary key,
    ledger      text,
    exporter_id text,
    created_at  text default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    enabled     integer,
    last_log_id integer,
    error       text,
    version     integer
);

create unique index if not exists "_system".pipelines_ledger_exporter_id_idx
    on pipelines (ledger, exporter_id);
