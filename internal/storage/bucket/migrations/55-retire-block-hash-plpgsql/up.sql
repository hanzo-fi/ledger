set search_path = '{{ .Schema }}';

-- The async log-block hash chain (FeatureHashLogs=ASYNC) is now computed in Go by
-- the AsyncBlockRunner (crypto/sha256 over the same canonical body the plpgsql
-- built) — the async counterpart of the migration-54 sync log-hash port. Drop the
-- create_blocks procedure and then the create_block function it called. The
-- logs_blocks table, its (ledger, previous) primary key and the `block` composite
-- type are left unchanged (the block type is the createBlock return shape).
drop procedure if exists create_blocks(varchar, integer);
drop function if exists create_block(varchar, integer, block);
