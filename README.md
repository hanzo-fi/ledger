# Hanzo Ledger

Hanzo Ledger is the programmable financial core ledger at the heart of **Hanzo Finance**. It provides a foundation for all kinds of money-moving applications: an atomic multi-postings transaction system, account-based modeling, and programmability via [numscript](https://github.com/formancehq/numscript), a built-in DSL to model financial transactions.

The ledger runs either as a standalone micro-service or as part of the Hanzo platform. It shines for financial applications requiring centralized state-keeping of the assets they orchestrate, such as:

* User balance-holding apps, where ownership of funds held in FBO accounts must be fine-grained in a ledger
* Digital asset platforms and exchanges, where funds in various denominations are represented
* Payment systems, where funds are cycled through a series of steps from acquiring to payouts
* Loan management systems, where a sophisticated structure of amounts due and to be disbursed is orchestrated

By default Hanzo Ledger uses **Hanzo Base (embedded SQLite), per-tenant**, so each org/project gets its own isolated, zero-contention ledger with no external database to run. **PostgreSQL remains a first-class, opt-in option** for shared/multi-instance production deployments. Either way, the ledger ships its logs to replica data stores for OLAP-optimized querying.

## Localhost

To get started locally with the default embedded Base (SQLite) storage:

```
go run . serve
```

Or with the full Postgres-backed stack:

```
docker compose -f examples/standalone/docker-compose.yml up
```

Once the system is up, start using the ledger:

```shell
# Create a ledger
http POST :8080/v2/quickstart
# Create a first transaction
http POST :8080/v2/quickstart/transactions postings:='[{"amount":100,"asset":"USD/2","destination":"users:1234","source":"world"}]'
```

## Storage

* **`sqlite` (default)** — embedded, per-tenant Hanzo Base files (`data/{tenant}.db`), single-writer, zero contention, IAM-multitenant. Best for local dev and per-org isolation.
* **`postgres` (opt-in)** — shared transactional store for multi-instance scale. Select via `STORAGE_DRIVER=postgres` (or `--storage.driver postgres`).

### Internals — dialect-agnostic double-entry spine

Upstream Formance braids the double-entry business rules *into* Postgres as ~38 plpgsql functions + 6 triggers (`internal/storage/bucket/migrations`), so the logic only runs where it is *stored*. [`internal/storage/ledgercore`](./internal/storage/ledgercore) lifts that spine *out* into plain Go over [bun](https://bun.uptrace.dev): posting a balanced transaction (postings → moves → running post-commit volumes), the balance/volume read path, and revert. The log hash chain — each log's hash is `H(prev_hash, payload)` — is the canonical `ledger.Log.ChainLog` (`internal/log.go`), which the plpgsql `compute_hash`/`set_log_hash` were written to match; reusing it makes the chain byte-identical on both dialects by construction. Storage stays plain tables (bigints → decimal `TEXT`, dates → RFC3339 `TEXT`, per-ledger sequences → a counter row), so bun builds and drives the schema identically on SQLite and Postgres. Formance's schema-per-bucket becomes one SQLite file per tenant (`data/{tenant}.db`); single-writer-per-file serializes writes, so no advisory locks are needed on that path.

## Docs

The Ledger API is described in [`openapi.yaml`](./openapi.yaml).

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## Attribution

Hanzo Ledger is a fork of [Formance Ledger](https://github.com/formancehq/ledger), MIT-licensed. See [LICENSE](./LICENSE). Upstream copyright remains with Formance Solutions; Hanzo modifications © Hanzo AI, Inc.
