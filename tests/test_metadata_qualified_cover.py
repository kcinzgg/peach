"""缺封面的官网完成策略、来源等待和图片验证。"""
import io
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from PIL import Image

from peach.http import HttpResponse
from peach.jav_cover_fetch import Candidate, MetadataEvidence, NotFound, first_qualified_cover
from peach.library_processing import LibraryMetadataProvider, cover_settled
from peach.metadata_network import MetadataSourceGate
from peach.scripting import HostLimiter


class QualifiedOfficialCoverTests(unittest.TestCase):
    dmm = 'https://pics.dmm.co.jp/digital/video/ssis00877/ssis00877pl.jpg'
    maker = 'https://cdn.up-timely.com/cover.jpg'

    def image(self, size=(800, 538)):
        data = io.BytesIO()
        Image.new('RGB', size, 'gray').save(data, 'JPEG')
        return data.getvalue()

    def transport(self, bodies=None, gate=None):
        calls = []
        def send(request, timeout, limit):
            def respond():
                calls.append(request.url)
                data = (bodies or {}).get(request.url, self.image())
                return HttpResponse(404 if data is None else 200, {}, data or b'', request.url)
            if gate:
                with gate.slot(request.url):
                    return respond()
            return respond()
        return send, calls

    def evidence(self, *urls):
        return MetadataEvidence(tuple(Candidate('official', url) for url in urls))

    def test_busy_dmm_yields_to_verified_maker_and_does_not_resume_comparison(self):
        gate = MetadataSourceGate({}, per_source=1, limiter=HostLimiter({}, default_interval=0))
        send, calls = self.transport(gate=gate)
        with gate.slot(self.dmm):
            candidate, size, data = first_qualified_cover(send, 'SSIS-877', self.evidence(self.dmm, self.maker))
        self.assertEqual(candidate.url, self.maker)
        self.assertEqual(size, (800, 538))
        self.assertEqual(calls, [self.maker, self.maker])
        self.assertTrue(data)
        self.assertTrue(all(not queue for queue in gate.waiters.values()))

    def test_invalid_small_missing_and_thumbnail_candidates_cannot_win(self):
        urls = ['https://cdn.up-timely.com/' + name + '.jpg' for name in ('invalid', 'small-image', 'missing', 'thumb', 'valid')]
        send, calls = self.transport({urls[0]: b'bad', urls[1]: self.image((600, 400)), urls[2]: None})
        candidate, _, _ = first_qualified_cover(send, 'SSIS-877', self.evidence(*urls))
        self.assertEqual(candidate.url, urls[-1])
        self.assertNotIn(urls[3], calls)

    def test_twenty_films_finish_through_shared_available_official_source(self):
        gate = MetadataSourceGate({}, per_source=1, limiter=HostLimiter({}, default_interval=.002))
        send, calls = self.transport(gate=gate)
        def collect(number):
            return first_qualified_cover(send, 'SSIS-877', self.evidence(self.dmm, self.maker))[0].url
        with gate.slot(self.dmm), ThreadPoolExecutor(max_workers=20) as pool:
            results = [future.result(timeout=10) for future in [pool.submit(collect, n) for n in range(20)]]
        self.assertEqual(results, [self.maker] * 20)
        self.assertEqual(calls, [self.maker] * 40)
        self.assertTrue(all(not queue for queue in gate.waiters.values()))

    def test_animated_and_dimension_changed_downloads_do_not_win(self):
        animated = io.BytesIO()
        Image.new('RGB', (800, 538), 'red').save(animated, 'GIF', save_all=True,
                                                append_images=[Image.new('RGB', (800, 538), 'blue')])
        calls = []
        def send(request, timeout, limit):
            calls.append(request.url)
            if request.url == self.dmm:
                data = self.image() if len(calls) == 1 else self.image((900, 600))
            elif request.url.endswith('animated.jpg'):
                data = animated.getvalue()
            else:
                data = self.image()
            return HttpResponse(200, {}, data, request.url)
        candidate, _, _ = first_qualified_cover(send, 'SSIS-877', self.evidence(
            self.dmm, 'https://cdn.up-timely.com/animated.jpg', self.maker))
        self.assertEqual(candidate.url, self.maker)

    def provider(self, send):
        provider = LibraryMetadataProvider.__new__(LibraryMetadataProvider)
        provider.transport = send
        provider._official_candidates = Mock(return_value=())
        provider.community = Mock(side_effect=NotFound('没有社区封面'))
        return provider

    def snapshot(self, code='SSIS-877', source='makers'):
        return [(source, {'id': code, 'title': '作品', 'cover_urls': [self.maker]})]

    def test_verified_cover_is_installed_once_with_completion_evidence(self):
        send, calls = self.transport()
        provider = self.provider(send)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            self.assertTrue(provider.cover('SSIS-877', root, snapshots=self.snapshot()))
            target = root / 'SSIS-877.jpg'
            evidence = json.loads(target.with_suffix('.scraping.json').read_text(encoding='utf-8'))
            self.assertEqual(evidence['selection_policy'], 'first-qualified-official')
            self.assertEqual(evidence['source_url'], self.maker)
            self.assertTrue(cover_settled(target))
            self.assertFalse(provider.cover('SSIS-877', root, snapshots=self.snapshot()))
        provider._official_candidates.assert_not_called()
        provider.community.assert_not_called()
        self.assertEqual(calls, [self.maker, self.maker])

    def test_mismatched_identity_and_community_do_not_use_official_shortcut(self):
        for snapshots in (self.snapshot('SSIS-876'), self.snapshot(source='javdb')):
            with self.subTest(snapshots=snapshots), tempfile.TemporaryDirectory() as directory:
                provider = self.provider(Mock())
                with patch('peach.jav_cover_fetch.first_qualified_cover') as fast, patch(
                        'peach.jav_cover_fetch.best_cover', side_effect=NotFound('没有官方封面')):
                    with self.assertRaises(NotFound):
                        provider.cover('SSIS-877', Path(directory).resolve(), snapshots=snapshots)
                    fast.assert_not_called()
                    provider._official_candidates.assert_called_once()

    def test_unavailable_verified_images_fall_back_to_official_discovery(self):
        send, _ = self.transport({self.maker: None})
        provider = self.provider(send)
        with tempfile.TemporaryDirectory() as directory, patch('peach.jav_cover_fetch.best_cover', return_value=(
                Candidate('official', self.dmm), (800, 538), self.image())) as full:
            self.assertTrue(provider.cover('SSIS-877', Path(directory).resolve(), snapshots=self.snapshot()))
            full.assert_called_once()

    def test_existing_official_cover_cannot_be_replaced_by_smaller_shortcut(self):
        from peach.library_processing import CoverKept
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            target = root / 'SSIS-877.jpg'
            original = self.image((1200, 800))
            target.write_bytes(original)
            target.with_suffix('.scraping.json').write_text(json.dumps({'source_url': self.maker}), encoding='utf-8')
            provider = self.provider(Mock())
            with patch('peach.jav_cover_fetch.first_qualified_cover') as fast, patch(
                    'peach.jav_cover_fetch.best_cover', return_value=(Candidate('official', self.maker),
                                                                    (800, 538), self.image())):
                with self.assertRaises(CoverKept):
                    provider.cover('SSIS-877', root, snapshots=self.snapshot())
                fast.assert_not_called()
            self.assertEqual(target.read_bytes(), original)
