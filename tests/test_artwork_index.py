"""图片关系持久化、逐条更新与后台资料写入的隔离行为。"""
import json
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch

from peach.artwork_index import ArtworkIndex
from peach.web_state import WebContract
from peach.web_router import _catalog_cached
from support.ledger import fresh_ledger


class ArtworkIndexTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.covers = self.root / 'covers'
        self.covers.mkdir()
        self.db = self.root / 'artwork.sqlite3'
        self.reader = Mock(side_effect=lambda path: json.loads(path.with_suffix('.face.json').read_text()))

    def cover(self, key, value):
        (self.covers / f'{key}.jpg').write_bytes(b'image')
        (self.covers / f'{key}.face.json').write_text(json.dumps({'cx': value}), encoding='utf-8')

    def index(self):
        return ArtworkIndex(self.db, self.covers, 'cover', reader=self.reader)

    def test_restart_reads_saved_rows_without_parsing_sidecars(self):
        self.cover('ABC-123', .1)
        self.assertEqual(self.index()['abc-123'], {'cx': .1})
        self.reader.reset_mock()
        self.assertEqual(self.index()['abc-123'], {'cx': .1})
        self.reader.assert_not_called()

    def test_changed_sidecar_updates_only_its_row(self):
        for n in range(30):
            self.cover(f'ABC-{n}', n)
        index = self.index()
        self.assertEqual(len(dict(index)), 30)
        with closing(sqlite3.connect(self.db)) as connection:
            before = dict(connection.execute('SELECT key,payload FROM artwork_index'))
        self.reader.reset_mock()
        (self.covers / 'ABC-7.face.json').write_text('{"cx": 12345}', encoding='utf-8')
        self.assertEqual(index['abc-7'], {'cx': 12345})
        self.assertEqual(index['abc-8'], {'cx': 8})
        self.reader.assert_called_once()
        with closing(sqlite3.connect(self.db)) as connection:
            after = dict(connection.execute('SELECT key,payload FROM artwork_index'))
        self.assertEqual([key for key in before if before[key] != after[key]], ['abc-7'])

    def test_create_delete_rename_and_missing_directory(self):
        index = self.index()
        self.assertEqual(dict(index), {})
        self.cover('ABC-1', 1)
        self.assertIn('abc-1', index)
        (self.covers / 'ABC-1.jpg').rename(self.covers / 'ABC-2.jpg')
        (self.covers / 'ABC-1.face.json').rename(self.covers / 'ABC-2.face.json')
        self.assertNotIn('abc-1', index)
        self.assertIn('abc-2', index)
        (self.covers / 'ABC-2.jpg').unlink()
        self.assertNotIn('abc-2', index)

    def test_twenty_readers_share_one_parse(self):
        self.cover('ABC-123', .1)
        index = self.index()
        start = threading.Barrier(20)
        def read(_):
            start.wait(timeout=10)
            return index['abc-123']
        with ThreadPoolExecutor(20) as pool:
            rows = list(pool.map(read, range(20)))
        self.assertEqual(rows, [{'cx': .1}] * 20)
        self.reader.assert_called_once()

    def test_excluded_cover_can_become_visible(self):
        self.cover('ABC-123', .1)
        self.reader.side_effect = lambda path: None
        index = self.index()
        self.assertNotIn('abc-123', index)
        self.reader.side_effect = lambda path: {'cx': .2}
        (self.covers / 'ABC-123.scraping.json').write_text('{}')
        self.assertEqual(index['abc-123'], {'cx': .2})

    def test_logo_variants_and_builtins_survive_local_deletion(self):
        path = self.covers / 'studio.icon.img'
        path.write_bytes(b'icon')
        index = ArtworkIndex(self.db, self.covers, 'logo', variants=('icon', 'logo'),
                             builtins={'studio': '', 'bundled': ''})
        self.assertTrue(index['studio'])
        path.unlink()
        self.assertEqual(index['studio'], '')
        self.assertEqual(index['bundled'], '')

    def test_avatar_version_survives_restart_and_changes_in_place(self):
        path = self.covers / 'performer-12.img'
        path.write_bytes(b'one')
        first = ArtworkIndex(self.db, self.covers, 'avatar')['performer-12']
        index = ArtworkIndex(self.db, self.covers, 'avatar')
        self.assertEqual(index['performer-12'], first)
        path.write_bytes(b'updated-image')
        self.assertNotEqual(index['performer-12'], first)

    def test_cache_bust_and_sample_commits_keep_artwork_and_catalog_results(self):
        ledger = fresh_ledger(self.root)
        self.cover('ABC-123', .1)
        contract = WebContract(ledger, cover_root=self.covers, avatar_root=self.covers,
                               logo_root=self.covers, marks_root=self.root / 'marks')
        self.assertTrue(contract.has_cover('ABC-123'))
        compute = Mock(return_value={'count': 7})
        _catalog_cached(contract, 'example', compute)
        with patch.object(contract._covers, 'reader', side_effect=AssertionError('重复解析封面')):
            for position in range(20):
                with contract.database.write_transaction() as connection:
                    connection.execute('INSERT INTO code_sample_image '
                                       '(code,position,url,site,source,fetched_at) VALUES(?,?,?,?,?,?)',
                                       ('ABC-123', position + 1, 'https://example.com/image.jpg',
                                        'example', 'test', '2026-01-01'))
                self.assertTrue(contract.has_cover('ABC-123'))
                self.assertEqual(_catalog_cached(contract, 'example', compute), {'count': 7})
        compute.assert_called_once()
        with contract.database.write_transaction() as connection:
            connection.execute("INSERT INTO entity(kind,canonical_name,normalized_name,created_at,updated_at) "
                               "VALUES('tag','test','test','2026-01-01','2026-01-01')")
        _catalog_cached(contract, 'example', compute)
        self.assertEqual(compute.call_count, 2)

    def test_unrelated_commit_during_projection_retains_completed_value(self):
        contract = WebContract(fresh_ledger(self.root))
        def compute():
            contract.cache_bust()
            return 42
        callback = Mock(side_effect=compute)
        self.assertEqual(_catalog_cached(contract, 'in-flight', callback), 42)
        self.assertEqual(_catalog_cached(contract, 'in-flight', callback), 42)
        callback.assert_called_once()
