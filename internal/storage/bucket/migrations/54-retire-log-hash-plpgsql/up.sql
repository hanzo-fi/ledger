set search_path = '{{ .Schema }}';

-- The log hash chain (FeatureHashLogs=SYNC) is now computed in Go by
-- Store.InsertLog via the canonical ledger.Log.ComputeHash — the same hashing
-- ledgercore uses on every dialect — so the in-database hash path is retired.
-- Drop the per-ledger set_log_hash triggers first, then the now-unused
-- set_log_hash() trigger function and the compute_hash() function (dead since the
-- migration 37 set_log_hash inlined its body and no longer called it).
do $$
	declare
		r record;
	begin
		for r in
			select id from _system.ledgers where bucket = current_schema
		loop
			execute format('drop trigger if exists "set_log_hash_%s" on logs', r.id);
		end loop;
	end
$$;

drop function if exists set_log_hash();
drop function if exists compute_hash(bytea, logs);
