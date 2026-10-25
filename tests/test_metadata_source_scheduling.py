"""影片内来源调度、来源容量与取消边界的行为验证。"""
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import unittest
from unittest.mock import patch
from pathlib import Path
import tempfile

from peach.metadata_network import MetadataSourceGate, source_results
from peach.metadata_parallel import collect_parallel
from peach.scripting import HostLimiter


class SourceSchedulingTests(unittest.TestCase):
    def gate(self, *, interval=0, capacity=2):
        return MetadataSourceGate({}, per_source=capacity,
                                  limiter=HostLimiter({}, default_interval=interval))

    def test_twenty_films_use_available_sources_while_dmm_is_busy(self):
        gate = self.gate(capacity=1)
        barrier = threading.Barrier(20)
        completed, sent, snapshots = [], [], []
        lock = threading.Lock()

        def work(film, call):
            def query(source):
                with gate.slot('https://' + source):
                    with lock:
                        sent.append((film, source))
                    barrier.wait(timeout=15)
                    return film
            with source_results(['www.dmm.co.jp', f'source{film}.example'], query) as available:
                for _, result in available:
                    return result.get()

        with gate.slot('https://pics.dmm.co.jp/held'):
            collect_parallel(range(20), key=lambda film: film, work=work,
                             complete=lambda film, result: completed.append(result),
                             capacity=lambda: 20, checkpoint=lambda: None,
                             changed=lambda active, pending: snapshots.append(len(active)))
        self.assertEqual(sorted(completed), list(range(20)))
        self.assertEqual(len(sent), 20)
        self.assertTrue(all(source != 'www.dmm.co.jp' for _, source in sent))
        self.assertEqual(max(snapshots), 20)
        self.assertTrue(all(not queue for queue in gate.waiters.values()))

    def test_all_sources_busy_wait_then_resume_without_replaying_started_requests(self):
        gate = self.gate(interval=.06, capacity=1)
        sent = []
        def query(source):
            for part in range(2):
                with gate.slot('https://' + source):
                    sent.append((source, part, time.monotonic()))
            return source
        with source_results(['a.example', 'b.example'], query) as available:
            self.assertEqual([result.get() for _, result in available], ['a.example', 'b.example'])
        self.assertEqual(len(sent), 4)
        self.assertGreaterEqual(sent[1][2] - sent[0][2], .05)
        self.assertGreaterEqual(sent[3][2] - sent[2][2], .05)

    def test_pacing_and_inflight_capacity_apply_to_scheduled_sources(self):
        gate = self.gate(interval=.02, capacity=2)
        active, peak, starts = 0, 0, []
        lock = threading.Lock()
        def film(number):
            def query(source):
                nonlocal active, peak
                with gate.slot(source):
                    with lock:
                        starts.append(time.monotonic())
                        active += 1
                        peak = max(peak, active)
                    time.sleep(.06)
                    with lock:
                        active -= 1
                    return number
            with source_results(['https://pics.dmm.co.jp/image'], query) as available:
                return next(available)[1].get()
        with ThreadPoolExecutor(max_workers=20) as pool:
            self.assertEqual(sorted(pool.map(film, range(20))), list(range(20)))
        self.assertEqual(peak, 2)
        self.assertTrue(all(b - a >= .017 for a, b in zip(starts, starts[1:])))
        self.assertTrue(all(not queue for queue in gate.waiters.values()))

    def test_cancellation_removes_waiters_and_preserves_source_slots(self):
        gate = self.gate(capacity=1)
        calls = 0
        def checkpoint():
            nonlocal calls
            calls += 1
            if calls >= 3:
                raise InterruptedError('cancel')
        def query(source):
            with gate.slot(source):
                self.fail('被占用的来源不能发送请求')
        with gate.slot('https://www.dmm.co.jp/held'):
            with patch('peach.metadata_network.checkpoint', side_effect=checkpoint):
                with self.assertRaises(InterruptedError):
                    with source_results(['https://pics.dmm.co.jp/image'], query) as available:
                        list(available)
        self.assertTrue(all(not queue for queue in gate.waiters.values()))
        self.assertTrue(gate.slots['dmm'].acquire(blocking=False))

    def test_source_errors_are_returned_once_and_other_sources_continue(self):
        gate = self.gate()
        attempts = []
        def query(source):
            with gate.slot('https://' + source):
                attempts.append(source)
                if source == 'a.example':
                    raise ValueError('unavailable')
                return 'found'
        with source_results(['a.example', 'b.example'], query) as available:
            source, result = next(available)
            with self.assertRaises(ValueError):
                result.get()
            self.assertEqual(next(available)[1].get(), 'found')
        self.assertEqual(attempts, ['a.example', 'b.example'])

    def test_cover_directory_uses_available_official_mirror(self):
        from peach import jav_cover_fetch as covers
        gate = self.gate(capacity=1)
        candidate = covers.candidate_for('https://official.example/cover.jpg')
        sent = []
        def ask(source):
            with gate.slot(source=source):
                sent.append(source)
                return covers.MetadataEvidence((candidate,), sources=frozenset({source}))
        with gate.slot(source='dmm'):
            with patch.object(covers, 'dmm_evidence', side_effect=lambda *a, **k: ask('dmm')):
                with patch.object(covers, 'r18_evidence', side_effect=lambda *a, **k: ask('r18dev')):
                    found = covers._site_candidates(None, 'SSIS-877', covers.MetadataEvidence(),
                                                    delay=0, deadline=None, record=lambda _: None)
        self.assertEqual(found, [candidate])
        self.assertEqual(sent, ['r18dev'])
        self.assertTrue(all(not queue for queue in gate.waiters.values()))

    def test_failing_mirror_keeps_the_busy_official_directory_eligible(self):
        from peach import jav_cover_fetch as covers
        gate = self.gate(capacity=1)
        candidate = covers.candidate_for('https://official.example/cover.jpg')
        held, release = threading.Event(), threading.Event()
        def hold():
            with gate.slot(source='dmm'):
                held.set()
                if not release.wait(5):
                    raise TimeoutError('备用目录未执行')
        def dmm(*args, **kwargs):
            with gate.slot(source='dmm'):
                return covers.MetadataEvidence((candidate,), sources=frozenset({'dmm'}))
        def mirror(*args, **kwargs):
            with gate.slot(source='r18dev'):
                release.set()
                raise covers.Unavailable('目录暂不可用')
        with ThreadPoolExecutor(max_workers=1) as pool:
            holder = pool.submit(hold)
            self.assertTrue(held.wait(3))
            try:
                with patch.object(covers, 'dmm_evidence', side_effect=dmm):
                    with patch.object(covers, 'r18_evidence', side_effect=mirror):
                        found = covers._site_candidates(None, 'SSIS-877', covers.MetadataEvidence(),
                                                        delay=0, deadline=None, record=lambda _: None)
                holder.result(timeout=3)
            finally:
                release.set()
        self.assertEqual(found, [candidate])

    def test_film_using_an_alternate_does_not_block_the_original_source_queue(self):
        gate = self.gate(capacity=1)
        alternate_started, finish_alternate = threading.Event(), threading.Event()
        def alternate_film():
            def query(source):
                with gate.slot(source=source):
                    if source == 'r18dev':
                        alternate_started.set()
                        if not finish_alternate.wait(5):
                            raise TimeoutError('原来源被队首阻塞')
                    return source
            with source_results(['dmm', 'r18dev'], query) as available:
                return next(available)[1].get()
        def dmm_film():
            def query(source):
                with gate.slot(source=source):
                    finish_alternate.set()
                    return source
            with source_results(['dmm'], query) as available:
                return next(available)[1].get()
        with ThreadPoolExecutor(max_workers=2) as pool:
            with gate.slot(source='dmm'):
                first = pool.submit(alternate_film)
                self.assertTrue(alternate_started.wait(3))
                second = pool.submit(dmm_film)
            try:
                self.assertEqual(second.result(timeout=3), 'dmm')
                self.assertEqual(first.result(timeout=3), 'r18dev')
            finally:
                finish_alternate.set()

    def test_cover_probes_use_other_hosts_and_preserve_canonical_ties(self):
        from peach import jav_cover_fetch as covers
        gate = self.gate(capacity=1)
        candidates = [covers.candidate_for(url) for url in
                      ('https://pics.dmm.co.jp/a.jpg', 'https://official.example/b.jpg')]
        held, release = threading.Event(), threading.Event()
        sent = []
        def hold():
            with gate.slot(source='dmm'):
                held.set()
                if not release.wait(5):
                    raise TimeoutError('备用图源未执行')
        def probe(transport, candidate, **kwargs):
            with gate.slot(candidate.url):
                sent.append(candidate)
                release.set()
                return 1000, 700
        with ThreadPoolExecutor(max_workers=1) as pool:
            holder = pool.submit(hold)
            self.assertTrue(held.wait(3))
            try:
                with patch.object(covers, 'probe_size', side_effect=probe):
                    found = covers._measure(None, candidates, lambda _: None,
                                            minimum_width=500, delay=0, deadline=None)
                holder.result(timeout=3)
            finally:
                release.set()
        self.assertEqual(sent, candidates[::-1])
        self.assertEqual([item[1] for item in found], candidates)

    def test_metadata_uses_available_source_without_recording_busy_as_missing(self):
        from peach.library_processing import _RemoteSession
        from peach.settings_file import PeachConfig
        gate = self.gate(capacity=1)
        sent, issues = [], []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            session = _RemoteSession(PeachConfig(root, root / 'config.toml', present=True),
                                     None, None, retrying=True)
            def ask(source, code, *args, **kwargs):
                with gate.slot(source=source):
                    sent.append(source)
                    return [(source, {'id': code, 'title': '核验标题'})]
            with gate.slot(source='dmm'):
                with patch.object(session, '_chain', return_value=('dmm', 'r18dev')):
                    with patch.object(session, '_ask', side_effect=ask):
                        found = session._metadata({'studio': ''}, 'SSIS-877', ['title'],
                                                  update=lambda **k: None,
                                                  issue=lambda *a, **k: issues.append(k))
            self.assertEqual(sent, ['r18dev'])
            self.assertEqual([entry[0] for entry in found], ['r18dev'])
            self.assertEqual(issues, [])
            self.assertEqual(session._task_absent, set())
