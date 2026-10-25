# Jobicy → PostgreSQL data pipeline

Import remote jobs into PostgreSQL with Python, cursor pagination, ID-based upserts and explicit status reconciliation. The official [Jobicy Python SDK](https://github.com/Jobicy/jobicy-python) is pinned to a GitHub commit.

## Start locally

Requires Python 3.10+, Git and PostgreSQL 16+. Docker Compose is optional for the local database.

```bash
git clone https://github.com/Jobicy/jobicy-data-pipeline.git
cd jobicy-data-pipeline
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Set `POSTGRES_PASSWORD` to your own local password and update the password in `DATABASE_URL` to match. For an existing database, set its connection URL instead; use your provider's required TLS settings. This script needs permission to create its tables, read and write them, and acquire PostgreSQL advisory locks. Use a dedicated database or schema.

```bash
docker compose up -d --wait
python pipeline.py --init-only
python pipeline.py
```

The Compose database binds only to localhost and persists data in `postgres-data`. `docker compose down` preserves it; deleting the volume deletes that local database. No cloud database is provisioned.

## Scheduling

`pipeline.py` performs one pass and exits. Schedule it hourly or less often, using absolute paths in cron:

```cron
0 * * * * cd /absolute/path/jobicy-data-pipeline && .venv/bin/python pipeline.py >> sync.log 2>&1
```

Successful passes less than one hour apart are skipped. A transaction-scoped advisory lock prevents two workers from synchronizing this pipeline concurrently in the same PostgreSQL database. The implementation uses the default connection search path; use the same schema/search path for every run.

Optional filters are `JOBICY_GEO`, `JOBICY_INDUSTRY` and `JOBICY_TAG` in `.env`. Keep filters unchanged within a traversal; the SDK handles opaque cursors and deduplicates IDs. Filter changes affect future feed imports and do not delete existing rows.

## Stored data and guarantees

[`schema.sql`](schema.sql) defines `jobicy_jobs` and `jobicy_sync_state`. The job table stores canonical URLs, titles, companies, eligibility locations, publication time, a normalized SDK snapshot, status, first/last-seen times and status-check time. It does not claim to preserve the raw API payload or full HTML descriptions.

A pass downloads the available seven-day feed and checks every retained job's status, including older records. It then commits upserts, status updates and the success watermark in one transaction. Upstream errors and database errors roll back the pass. Missing from the feed is not closure; `unknown` remains distinct from `closed`. No historical backfill before your first run is available through this feed. Your stored history describes collected Jobicy listings, not the full employment market.

Keep original Jobicy URLs and display **[Jobs powered by Jobicy](https://jobicy.com/)** in public interfaces built on this data. Protect `.env` and database backups. Logs report error types rather than printing connection strings.

## Verify with a disposable PostgreSQL database

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/test_database \
  python -m unittest discover -s tests -v
```

Tests create and drop uniquely named schemas in the supplied test database. They use real PostgreSQL and local HTTP fixtures to verify repeat imports, older active/unknown records and rollback on upstream/database errors. Without `TEST_DATABASE_URL` these integration tests are skipped. GitHub Actions supplies its own disposable PostgreSQL service. Live Jobicy connectivity and your database permissions require deployment verification.

[All integrations](https://github.com/Jobicy/jobicy-api-examples) · [MIT license](LICENSE) · [Data terms](PROVENANCE.md)
