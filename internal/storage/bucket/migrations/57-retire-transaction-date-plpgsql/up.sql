set search_path = '{{ .Schema }}';

-- The per-transaction date (one instant shared by a logical transaction and all
-- of its accounts, moves and log) is now stamped in Go by the Store: BeginTX
-- captures a single UTC-microsecond timestamp and the transactions/accounts/
-- moves/logs write paths thread it into their date columns. This retires the
-- transaction_date() plpgsql function (a temp-table cache of statement_timestamp()
-- that these column DEFAULTs and the Store's own SQL called) together with the
-- set_transaction_updated_at trigger (before insert: updated_at = inserted_at),
-- which InsertTransaction now mirrors in Go.

-- The set_transaction_updated_at trigger + function first.
drop trigger if exists set_transaction_updated_at on transactions;
drop function if exists set_transaction_updated_at();

-- Drop the transaction_date() column DEFAULTs. Every write path now supplies these
-- dates explicitly, so a missing value is a bug rather than a silent statement_timestamp.
alter table transactions
	alter column timestamp drop default,
	alter column inserted_at drop default;

alter table accounts
	alter column first_usage drop default,
	alter column insertion_date drop default,
	alter column updated_at drop default;

alter table moves
	alter column insertion_date drop default,
	alter column effective_date drop default;

alter table logs
	alter column date drop default;

-- transaction_date() is now unreferenced: the surviving effective-volumes triggers
-- (set_effective_volumes / update_effective_volumes) key only off effective_date,
-- which the Store supplies. Drop the function last, after its DEFAULT users are gone.
drop function if exists transaction_date();
