"""来源在途限制、跨域间隔和冷却在并发请求中的行为。"""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import threading
import time
import unittest
import json
from unittest.mock import Mock, patch

from peach.http import HttpRequest, HttpResponse
from peach.metadata_network import MetadataSourceGate
from peach.scraping_access import SourceTransport, SourcePaused


class MetadataNetworkTests(unittest.TestCase):
    def test_each_source_has_two_slots_and_other_sources_continue(self):
        gate = MetadataSourceGate({}, limiter=Mock())
        active, peak = {}, {}
        lock = threading.Lock()
        barrier = threading.Barrier(4)
        def work(source):
            with gate.slot('https://' + source):
                with lock:
                    active[source] = active.get(source, 0) + 1
                    peak[source] = max(peak.get(source, 0), active[source])
                barrier.wait(timeout=5)
                time.sleep(.02)
                with lock:
                    active[source] -= 1
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(work, source) for source in ['r18.dev'] * 4 + ['www.dmm.co.jp'] * 4]
            for future in futures:
                future.result(timeout=10)
        self.assertEqual(peak, {'r18.dev': 2, 'www.dmm.co.jp': 2})

    def test_source_domains_use_one_pacing_key_and_release_on_failure(self):
        limiter = Mock()
        gate = MetadataSourceGate({}, limiter=limiter)
        with self.assertRaises(ValueError), gate.slot('https://pics.dmm.co.jp/a'):
            raise ValueError('test')
        with gate.slot('https://www.dmm.com/b'):
            pass
        self.assertEqual([call.args[0] for call in limiter.wait.call_args_list],
                         ['https://www.dmm.co.jp/', 'https://www.dmm.co.jp/'])
        self.assertTrue(gate.slots['dmm'].acquire(blocking=False))
        self.assertTrue(gate.slots['dmm'].acquire(blocking=False))

    def test_redirects_acquire_the_destination_gate_and_cooldown_prevents_sending(self):
        with tempfile.TemporaryDirectory() as directory:
            gate = MetadataSourceGate({}, limiter=Mock())
            transport = SourceTransport(Path(directory).resolve(), gate=gate)
            source = Mock(return_value=HttpResponse(302, {'location': 'https://www.dmm.co.jp/target'}, b''))
            destination = Mock(return_value=HttpResponse(429, {'retry-after': '120'}, b''))
            transport.transports.update(r18dev=source, dmm=destination)
            with self.assertRaises(SourcePaused):
                transport(HttpRequest('GET', 'https://r18.dev/start', {}), 10, 1000)
            self.assertEqual(set(gate.slots), {'r18dev', 'dmm'})
            with self.assertRaises(SourcePaused):
                transport(HttpRequest('GET', 'https://www.dmm.co.jp/other', {}), 10, 1000)
            self.assertEqual(destination.call_count, 1)

    def test_waiting_worker_can_cancel_without_sending(self):
        gate = MetadataSourceGate({}, per_source=1, limiter=Mock())
        with gate.slot('https://r18.dev/a'):
            with patch('peach.metadata_network.checkpoint', side_effect=InterruptedError('stop')):
                with self.assertRaises(InterruptedError), gate.slot('https://r18.dev/b'):
                    self.fail('等待名额时不能发送')

    def test_success_does_not_clear_another_requests_cooldown(self):
        from peach.scraping_access import cooldown_path, pause_source, paused_until
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            cooldown_path(root, 'dmm').write_text(json.dumps({'until': time.time() - 10, 'blocks': 1, 'via': 'http'}))
            transport = SourceTransport(root)
            def response(*args):
                pause_source(root, 'dmm', retry_after=300)
                pause_source(root, 'dmm', retry_after=10)
                return HttpResponse(200, {}, b'ok')
            transport.transports['dmm'] = response
            transport(HttpRequest('GET', 'https://www.dmm.co.jp/a', {}), 10, 1000)
            self.assertGreater(paused_until(root, 'dmm'), time.time() + 290)

    def test_bridge_uses_the_shared_source_gate_and_rechecks_cooldown(self):
        from peach.library_processing import LibraryMetadataProvider
        from peach.scraping_access import pause_source
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            gate = MetadataSourceGate({}, limiter=Mock())
            provider = LibraryMetadataProvider(root, gate=gate)
            with patch.object(provider, '_amane_query_serial', return_value=[]) as query:
                self.assertEqual(provider._amane_query('TEST-1', ['mgstage'], None), [])
                query.assert_called_once()
                self.assertEqual(set(gate.slots), {'mgstage'})
                query.reset_mock()
                pause_source(root, 'mgstage', retry_after=300)
                self.assertIsInstance(provider._amane_query('TEST-2', ['mgstage'], None), SourcePaused)
                query.assert_not_called()
            self.assertTrue(provider.bridge_lock.acquire(blocking=False))
            with patch('peach.processing_control.checkpoint', side_effect=InterruptedError('stop')):
                with self.assertRaises(InterruptedError):
                    provider._amane_query('TEST-3', ['mgstage'], None)
            provider.bridge_lock.release()
