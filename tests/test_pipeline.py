from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from threading import Thread
import unittest
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

from jobicy import JobicyClient, JobicyError
import psycopg
from psycopg import sql
from pipeline import initialize, synchronize


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'), 'Set TEST_DATABASE_URL to run PostgreSQL integration tests')
class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.connection = psycopg.connect(os.environ['TEST_DATABASE_URL'], autocommit=True)
        self.schema = 'jobicy_test_' + uuid4().hex
        self.connection.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(self.schema)))
        self.connection.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(self.schema)))
        initialize(self.connection)
        self.fail_status = False
        self.identifiers = [1, 2]
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args): pass
            def do_GET(self):
                url = urlsplit(self.path); query = parse_qs(url.query)
                if url.path.endswith('/status'):
                    if owner.fail_status:
                        self.send_response(503); self.end_headers(); return
                    records = [{'id': int(value), 'status': 'active' if int(value) in owner.identifiers else 'unknown'} for value in query['ids'][0].split(',')]
                    payload = {'success': True, 'count': len(records), 'jobs': records}
                else:
                    second = 'cursor' in query
                    ids = owner.identifiers[1:] if second else owner.identifiers[:1]
                    payload = {'jobs': [{'id': identifier, 'url': f'https://jobicy.com/jobs/{identifier}', 'jobTitle': 'Engineer', 'pubDate': '2026-10-20T00:00:00Z'} for identifier in ids],
                               'nextCursor': None if second else 'opaque+/=', 'hasMore': not second}
                self.send_response(200); self.send_header('Content-Type', 'application/json'); self.end_headers()
                self.wfile.write(json.dumps(payload).encode())
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = Thread(target=self.server.serve_forever, daemon=True); self.thread.start()
        self.client = JobicyClient(api_url=f'http://127.0.0.1:{self.server.server_port}/api/jobs')

    def tearDown(self):
        self.client.close(); self.server.shutdown(); self.server.server_close(); self.thread.join()
        self.connection.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(self.schema)))
        self.connection.close()

    def expire_watermark(self):
        self.connection.execute("UPDATE jobicy_sync_state SET completed_at=now()-interval '2 hours'")

    def test_idempotent_import_retains_older_jobs_and_distinguishes_unknown(self):
        self.assertEqual(synchronize(self.connection, self.client)['stored_jobs'], 2)
        self.assertIn('skipped', synchronize(self.connection, self.client))
        self.expire_watermark(); self.identifiers = [2]
        self.assertEqual(synchronize(self.connection, self.client)['stored_jobs'], 2)
        self.assertEqual(self.connection.execute('SELECT id,status FROM jobicy_jobs ORDER BY id').fetchall(), [(1, 'unknown'), (2, 'active')])
        self.assertEqual(self.connection.execute('SELECT count(*) FROM jobicy_jobs').fetchone()[0], 2)

    def test_upstream_status_failure_does_not_commit_jobs_or_watermark(self):
        synchronize(self.connection, self.client); self.expire_watermark()
        before = self.connection.execute('SELECT completed_at FROM jobicy_sync_state').fetchone()[0]
        self.identifiers = [3]; self.fail_status = True
        with self.assertRaises(JobicyError): synchronize(self.connection, self.client)
        self.assertEqual(self.connection.execute('SELECT id FROM jobicy_jobs ORDER BY id').fetchall(), [(1,), (2,)])
        self.assertEqual(self.connection.execute('SELECT completed_at FROM jobicy_sync_state').fetchone()[0], before)

    def test_database_failure_rolls_back_new_rows(self):
        class InvalidClient:
            def get_all_jobs(_self, **_filters):
                from jobicy import Job
                return [Job.from_api({'id': 1, 'url': 'https://jobicy.com/jobs/1', 'pubDate': 'invalid timestamp'})]
            def get_job_statuses(_self, ids):
                from jobicy import JobStatus
                return [JobStatus(identifier, 'active') for identifier in ids]
        with self.assertRaises(psycopg.Error): synchronize(self.connection, InvalidClient())
        self.assertEqual(self.connection.execute('SELECT count(*) FROM jobicy_jobs').fetchone()[0], 0)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM jobicy_sync_state').fetchone()[0], 0)
