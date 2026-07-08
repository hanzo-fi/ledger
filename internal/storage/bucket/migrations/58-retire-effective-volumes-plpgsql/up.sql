set search_path = '{{ .Schema }}';

-- post_commit_effective_volumes (FeatureMovesHistoryPostCommitEffectiveVolumes=SYNC)
-- is now computed in Go by Store.InsertMoves (the volume arithmetic stays in SQL, so
-- the stored numerics are byte-identical to the plpgsql). Drop the per-ledger
-- set_effective_volumes (before insert) / update_effective_volumes (after insert)
-- triggers, then the now-unused trigger functions.
do $$
	declare
		r record;
	begin
		for r in
			select id from _system.ledgers where bucket = current_schema
		loop
			execute format('drop trigger if exists "set_effective_volumes_%s" on moves', r.id);
			execute format('drop trigger if exists "update_effective_volumes_%s" on moves', r.id);
		end loop;
	end
$$;

drop function if exists set_effective_volumes();
drop function if exists update_effective_volumes();
