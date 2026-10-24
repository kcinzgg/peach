"""资料并发、单写入线程、停止与共享网络预算的隔离验证。"""
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from peach.metadata_parallel import collect_parallel, RequestBudget, workers_for
from peach.processing_control import CURRENT
from peach.library_processing import process_library
from peach.review_csv import read_rows
from peach.settings_file import PeachConfig
from peach.scraping_access import SourcePaused
from support.conditions import windows_ledger_roots
from support.ledger import fresh_ledger


class SchedulerTests(unittest.TestCase):
    def test_jobs_overlap_and_callbacks_run_on_owner(self):
        owner = threading.get_ident()
        barrier = threading.Barrier(2)
        threads, callbacks, finished = set(), [], []

        def work(item, call):
            threads.add(threading.get_ident())
            barrier.wait(timeout=3)
            call(lambda: callbacks.append(threading.get_ident()))
            return item * 2

        collect_parallel([1, 2], key=lambda n: n, work=work,
                         complete=lambda item, result: finished.append((item, result, threading.get_ident())),
                         capacity=lambda: 2, checkpoint=lambda: None)
        self.assertEqual(len(threads), 2)
        self.assertEqual(callbacks, [owner, owner])
        self.assertEqual(sorted(finished), [(1, 2, owner), (2, 4, owner)])

    def test_slow_job_does_not_hold_completed_results(self):
        ready = threading.Event()
        order = []

        def work(item, call):
            if item == 1:
                self.assertTrue(ready.wait(3))
            return item

        def complete(item, result):
            order.append(item)
            if item == 2:
                ready.set()

        collect_parallel([1, 2], key=lambda n: n, work=work, complete=complete,
                         capacity=lambda: 2, checkpoint=lambda: None)
        self.assertEqual(order, [2, 1])

    def test_one_code_is_never_processed_twice_at_once(self):
        active, seen = set(), []
        lock = threading.Lock()

        def work(item, call):
            with lock:
                self.assertNotIn(item, active)
                active.add(item)
            time.sleep(.01)
            with lock:
                active.remove(item)
                seen.append(item)

        collect_parallel(['A', 'A', 'B'], key=lambda n: n, work=work,
                         complete=lambda *_: None, capacity=lambda: 4, checkpoint=lambda: None)
        self.assertEqual(sorted(seen), ['A', 'A', 'B'])

    def test_callback_failure_unblocks_workers_and_stops_new_jobs(self):
        calls = []

        def fail():
            raise ValueError('写入失败')

        def work(item, call):
            calls.append(item)
            call(fail)

        with self.assertRaisesRegex(ValueError, '写入失败'):
            collect_parallel(range(8), key=lambda n: n, work=work,
                             complete=lambda *_: None, capacity=lambda: 2, checkpoint=lambda: None)
        self.assertLessEqual(len(calls), 2)

    def test_network_checkpoints_return_to_owner(self):
        owner = threading.get_ident()
        checked = []
        parent = SimpleNamespace(paused_seconds=12, mode='high',
                                 checkpoint=lambda kind: checked.append((kind, threading.get_ident())))
        token = CURRENT.set(parent)
        try:
            def work(item, call):
                control = CURRENT.get()
                self.assertEqual(control.paused_seconds, 12)
                control.checkpoint('network')
            collect_parallel([1], key=lambda n: n, work=work, complete=lambda *_: None,
                             capacity=lambda: 1, checkpoint=lambda: None)
        finally:
            CURRENT.reset(token)
        self.assertEqual(checked, [('network', owner)])

    def test_speed_modes_bound_concurrency(self):
        self.assertEqual([workers_for(mode) for mode in ('low', 'standard', 'high')], [1, 2, 4])


class BudgetTests(unittest.TestCase):
    def test_time_budget_starts_at_first_request(self):
        with patch('peach.metadata_parallel.deadline_after', return_value=120) as deadline, \
                patch('peach.metadata_parallel.time.monotonic', return_value=100):
            budget = RequestBudget(2, 100, 20)
            deadline.assert_not_called()
            inner = Mock(return_value=SimpleNamespace(body=b'123'))
            budget.run(inner, 'a', 30, 10)
            budget.run(inner, 'b', 30, 10)
        deadline.assert_called_once_with(20)
        self.assertEqual(inner.call_args.args[1], 20)

    def test_independent_transports_charge_one_budget(self):
        from peach.http import HttpRequest
        from peach.scraping_access import SourceTransport
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            budget = RequestBudget(2, 100, 20)
            response = SimpleNamespace(status=200, headers={}, body=b'123')
            sent = Mock(return_value=response)
            first, second = SourceTransport(root), SourceTransport(root)
            first.budget = second.budget = budget
            with patch.object(first, '_transport_for', return_value=sent), \
                    patch.object(second, '_transport_for', return_value=sent):
                first(HttpRequest('GET', 'https://example.test/a', {}), 5, 10)
                second(HttpRequest('GET', 'https://example.test/b', {}), 5, 10)
                with self.assertRaises(SourcePaused):
                    second(HttpRequest('GET', 'https://example.test/c', {}), 5, 10)
            self.assertEqual(sent.call_count, 2)
            self.assertEqual(budget.bytes, 6)

    def test_workers_share_request_count(self):
        budget = RequestBudget(2, 100, 20)
        inner = Mock(return_value=SimpleNamespace(body=b'123'))
        budget.run(inner, 'a', 10, 50)
        budget.run(inner, 'b', 10, 50)
        with self.assertRaises(SourcePaused):
            budget.run(inner, 'c', 10, 50)
        self.assertEqual(inner.call_count, 2)
        self.assertEqual((budget.requests, budget.bytes, budget.reserved), (2, 6, 0))

    def test_inflight_bytes_are_reserved_and_unused_bytes_returned(self):
        budget = RequestBudget(5, 10, 20)

        def inner(request, timeout, limit):
            self.assertEqual(limit, 10)
            with self.assertRaises(SourcePaused):
                budget.run(Mock(), 'second', 10, 10)
            return SimpleNamespace(body=b'123')

        budget.run(inner, 'first', 10, 10)
        accepted = Mock(return_value=SimpleNamespace(body=b'4567890'))
        budget.run(accepted, 'second', 10, 10)
        self.assertEqual(accepted.call_args.args[-1], 7)
        self.assertEqual(budget.bytes, 10)


class LibraryParallelTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()

    @windows_ledger_roots
    def test_metadata_only_collects_two_movies_concurrently_and_persists_each(self):
        media = self.root / 'media'
        media.mkdir()
        for code in ('ABW-101', 'ABW-102'):
            (media / f'{code}.mp4').write_bytes(b'fixture')
        db = fresh_ledger(self.root)
        config = PeachConfig(self.root, self.root / 'config.toml', present=True,
                             locations={'local': (str(media),)})
        process_library(config, db, self.root/'generated', self.root/'covers', stage='scan')
        barrier = threading.Barrier(2)

        def factory():
            provider = Mock()
            def query(code, *args, **kwargs):
                barrier.wait(timeout=5)
                return {'id': code, 'title': code, 'maker': '测试厂牌', 'release_date': '2020-01-01',
                        'actresses': [{'japanese_name': '测试演员'}]}
            provider.query.side_effect = query
            provider.cover.return_value = False
            return provider

        with patch('peach.library_processing._probe_sources') as probe, \
                patch('peach.library_processing._generate_previews') as frames:
            result = process_library(config, db, self.root/'generated', self.root/'covers',
                                     stage='metadata', speed_mode='high', provider_factory=factory,
                                     route_overrides='censored=r18dev')
        probe.assert_not_called()
        frames.assert_not_called()
        self.assertEqual(result['checked'], 2)
        self.assertEqual(result['issue_count'], 0)
        self.assertEqual(len(result['completed_metadata']), 2)
        titles = [row for row in read_rows(self.root/'generated/library-metadata-field-candidates.csv')
                  if row['field'] == 'title']
        self.assertEqual({row['asset_id'] for row in titles}, {'1', '2'})

    @windows_ledger_roots
    def test_throttled_source_falls_back_and_one_failed_cover_does_not_block_other_checkpoint(self):
        from peach.jav_cover_fetch import NotFound
        media = self.root / 'media'
        media.mkdir()
        for code in ('ABW-101', 'ABW-102'):
            (media / f'{code}.mp4').write_bytes(b'fixture')
        db = fresh_ledger(self.root)
        config = PeachConfig(self.root, self.root/'config.toml', present=True,
                             locations={'local': (str(media),)})
        process_library(config, db, self.root/'generated', self.root/'covers', stage='scan')
        queries = []

        def factory():
            provider = Mock()
            def query(code, source, **kwargs):
                queries.append((code, source))
                if source == 'r18dev':
                    raise SourcePaused('来源正在冷却')
                return {'id': code, 'title': code, 'maker': '测试厂牌', 'release_date': '2020-01-01',
                        'actresses': [{'japanese_name': '测试演员'}]}
            def cover(code, *args, **kwargs):
                if code == 'ABW-101':
                    raise NotFound('没有封面')
                return False
            provider.query.side_effect = query
            provider.cover.side_effect = cover
            return provider

        result = process_library(config, db, self.root/'generated', self.root/'covers', stage='metadata',
                                 speed_mode='high', provider_factory=factory, route_overrides='censored=r18dev,dmm')
        self.assertEqual(result['checked'], 2)
        self.assertEqual(len(result['completed_metadata']), 1)
        self.assertTrue(result['completed_metadata'][0].startswith('2:'))
        self.assertEqual(set(queries), {(code, source) for code in ('ABW-101', 'ABW-102')
                                      for source in ('r18dev', 'dmm')})
