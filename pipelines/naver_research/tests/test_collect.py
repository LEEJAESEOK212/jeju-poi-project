import json
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import collect


class Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return json.dumps(self.payload).encode('utf-8')


class ApiConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = sqlite3.connect(Path(self.tmp.name) / 'checkpoint.sqlite3', check_same_thread=False)
        self.db.execute('CREATE TABLE searches(key TEXT PRIMARY KEY,payload TEXT)')

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_same_query_is_requested_once_and_then_read_from_cache(self):
        api = collect.API(self.db, 'id', 'secret', budget=10)
        calls = 0
        guard = threading.Lock()

        def request(_request, timeout):
            nonlocal calls
            with guard:
                calls += 1
            time.sleep(.05)
            return Response({'items': [{'title': 'result'}]})

        with patch.object(collect, 'urlopen', side_effect=request):
            threads = [threading.Thread(target=api.search, args=('webkr', '제주 테스트')) for _ in range(3)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            api.search('webkr', '제주 테스트')

        self.assertEqual(calls, 1)
        self.assertEqual(api.calls, 1)

    def test_network_wait_does_not_hold_database_lock(self):
        api = collect.API(self.db, 'id', 'secret', budget=10)
        started = threading.Event()
        release = threading.Event()

        def request(_request, timeout):
            started.set()
            release.wait(1)
            return Response({'items': []})

        with patch.object(collect, 'urlopen', side_effect=request):
            thread = threading.Thread(target=api.search, args=('webkr', '제주 A'))
            thread.start()
            self.assertTrue(started.wait(.5))
            # If urlopen held api.lock, this acquire would time out.
            self.assertTrue(api.lock.acquire(timeout=.2))
            api.lock.release()
            release.set()
            thread.join(1)
            self.assertFalse(thread.is_alive())

    def test_budget_stops_before_an_extra_request(self):
        api = collect.API(self.db, 'id', 'secret', budget=1)
        with patch.object(collect, 'urlopen', return_value=Response({'items': []})):
            api.search('webkr', '제주 A')
            with self.assertRaisesRegex(RuntimeError, '한도'):
                api.search('webkr', '제주 B')
        self.assertEqual(api.calls, 1)


if __name__ == '__main__':
    unittest.main()
