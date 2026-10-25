"""One transactional Jobicy -> PostgreSQL synchronization. Schedule hourly."""
from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
import sys

from dotenv import load_dotenv
from jobicy import JobicyClient
import psycopg
from psycopg.types.json import Jsonb


def initialize(connection: psycopg.Connection) -> None:
    connection.execute(Path(__file__).with_name('schema.sql').read_text())


def synchronize(connection: psycopg.Connection, client: JobicyClient, filters: dict | None = None) -> dict:
    """Commit rows, statuses and the hourly watermark together, or roll back all."""
    with connection.transaction():
        if not connection.execute('SELECT pg_try_advisory_xact_lock(80582878, 2)').fetchone()[0]:
            return {'skipped': 'another synchronization holds the database lock'}
        recent = connection.execute("SELECT completed_at > now() - interval '1 hour' FROM jobicy_sync_state WHERE key = 'jobs'").fetchone()
        if recent and recent[0]:
            return {'skipped': 'less than one hour since the previous successful run'}
        jobs = client.get_all_jobs(count=100, **(filters or {}))
        stored_ids = {row[0] for row in connection.execute('SELECT id FROM jobicy_jobs')}
        current_ids = {int(job.id) for job in jobs}
        identifiers = sorted(stored_ids | current_ids)
        statuses = []
        for offset in range(0, len(identifiers), 100):
            statuses.extend(client.get_job_statuses(identifiers[offset:offset + 100]))
        with connection.cursor() as cursor:
            cursor.executemany('''
                INSERT INTO jobicy_jobs (id, url, title, company, location, published_at, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET url=EXCLUDED.url, title=EXCLUDED.title,
                    company=EXCLUDED.company, location=EXCLUDED.location,
                    published_at=EXCLUDED.published_at, payload=EXCLUDED.payload, last_seen_at=now()
            ''', [(int(job.id), job.url, job.job_title, job.company_name, job.location,
                   job.published_at or None, Jsonb(asdict(job))) for job in jobs])
            cursor.executemany('UPDATE jobicy_jobs SET status=%s, status_checked_at=clock_timestamp() WHERE id=%s',
                               [(item.status, item.id) for item in statuses])
        connection.execute("INSERT INTO jobicy_sync_state (key, completed_at) VALUES ('jobs', clock_timestamp()) ON CONFLICT (key) DO UPDATE SET completed_at=clock_timestamp()")
        return {'feed_jobs': len(jobs), 'stored_jobs': len(identifiers), 'statuses_checked': len(statuses)}


def main() -> None:
    load_dotenv()
    if sys.argv[1:] not in ([], ['--init-only']):
        raise ValueError('Usage: python pipeline.py [--init-only]')
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise ValueError('Set DATABASE_URL in .env or the process environment')
    filters = {key: value for key in ('geo', 'industry', 'tag') if (value := os.environ.get('JOBICY_' + key.upper(), '').strip())}
    with psycopg.connect(database_url, autocommit=True, connect_timeout=10) as connection:
        initialize(connection)
        if '--init-only' in sys.argv:
            print('Schema initialized.')
            return
        with JobicyClient() as client:
            print(json.dumps(synchronize(connection, client, filters)))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Database errors can contain connection details; report the type, not a DSN or server message.
        print(f'Synchronization failed ({type(error).__name__}); no transaction was committed. Check configuration and service availability.', file=sys.stderr)
        raise SystemExit(1)
