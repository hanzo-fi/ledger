set search_path = '{{ .Schema }}';

-- Metadata history (Feature{Transaction,Account}MetadataHistory=SYNC) is now
-- appended in Go by the Store's metadata write paths (InsertTransaction,
-- updateTxWithRetrieve, UpdateAccountsMetadata, DeleteAccountMetadata,
-- UpsertAccounts) via the canonical revision = max(revision)+1 (or 1) rows the
-- triggers produced — so the in-database trigger path is retired. Drop the
-- per-ledger {insert,update}_{transaction,account}_metadata_history triggers
-- first, then the now-unused trigger functions.
do $$
	declare
		r record;
	begin
		for r in
			select id from _system.ledgers where bucket = current_schema
		loop
			execute format('drop trigger if exists "insert_transaction_metadata_history_%s" on transactions', r.id);
			execute format('drop trigger if exists "update_transaction_metadata_history_%s" on transactions', r.id);
			execute format('drop trigger if exists "insert_account_metadata_history_%s" on accounts', r.id);
			execute format('drop trigger if exists "update_account_metadata_history_%s" on accounts', r.id);
		end loop;
	end
$$;

drop function if exists insert_transaction_metadata_history();
drop function if exists update_transaction_metadata_history();
drop function if exists insert_account_metadata_history();
drop function if exists update_account_metadata_history();
