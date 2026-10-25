"""本机图片的 SQLite 索引；按文件身份更新单条记录，不参与账本缓存失效。"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from collections.abc import Iterator, Mapping
from pathlib import Path


def signature(path: Path) -> tuple | None:
    """文件替换、原地修改和删除均有独立身份。"""
    try:
        stat = path.stat()
        return stat.st_mtime_ns, stat.st_size, stat.st_ino
    except FileNotFoundError:
        return None


class ArtworkIndex(Mapping):
    """图片关系落本机数据库，内存只复用已核对的行。

    目录版本只用于发现新增、删除及改名；已有行按图片和边车的身份核验。
    读取一条只解析这一条变化的边车。重启从数据库接回，不重新解析全目录。
    本机数据库不属于 ledger，也不进入账本复制。
    """

    def __init__(self, database: Path, root: Path, kind: str, *, reader=None,
                 variants=(), builtins=None):
        self.database = Path(database)
        self.requested_root = Path(root)
        self.root = Path(root).resolve()
        self.kind = kind
        self.reader = reader
        self.variants = variants
        self.builtins = builtins or {}
        self.scope = f"v1:{kind}:{self.root}"
        self.lock = threading.RLock()
        self.loaded = False
        self.directory_version = object()
        self.paths: dict[str, list[Path]] = {}
        self.rows: dict[str, tuple[str, object]] = {}
        self.pending = None

    def _connect(self):
        self.database.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.database, timeout=10)
        connection.execute('PRAGMA journal_mode=WAL')
        connection.execute('CREATE TABLE IF NOT EXISTS artwork_index ('
                           'scope TEXT NOT NULL, key TEXT NOT NULL, '
                           'signature TEXT NOT NULL, payload TEXT NOT NULL, '
                           'PRIMARY KEY(scope,key))')
        return connection

    def _load(self):
        if self.loaded:
            return
        connection = self._connect()
        try:
            self.rows = {key: (stamp, json.loads(payload)) for key, stamp, payload in
                         connection.execute('SELECT key,signature,payload FROM artwork_index WHERE scope=?',
                                            (self.scope,))}
        finally:
            connection.close()
        self.loaded = True

    def _key(self, name: str) -> str | None:
        folded = name.casefold()
        if self.kind == 'cover':
            return folded[:-4] if name.endswith('.jpg') else None
        if self.kind == 'avatar':
            if folded.endswith('.img'):
                return folded[:-4]
            if folded.endswith('.jpg') and folded[:-4].isdigit():
                return '#' + str(int(folded[:-4]))
            return None
        if not folded.endswith('.img'):
            return None
        stem = folded[:-4]
        for variant in self.variants:
            if stem.endswith('.' + variant):
                stem = stem[:-len(variant) - 1]
                break
        return stem or None

    def _discover(self):
        self._load()
        stamp = signature(self.root)
        if stamp == self.directory_version:
            return
        paths: dict[str, list[Path]] = {}
        try:
            with os.scandir(self.root) as entries:
                for entry in entries:
                    key = self._key(entry.name)
                    if key is not None and entry.is_file():
                        paths.setdefault(key, []).append(Path(entry.path))
        except FileNotFoundError:
            pass
        removed = self.rows.keys() - paths.keys()
        if removed:
            connection = self._connect()
            try:
                with connection:
                    connection.executemany('DELETE FROM artwork_index WHERE scope=? AND key=?',
                                           [(self.scope, key) for key in removed])
            finally:
                connection.close()
            for key in removed:
                self.rows.pop(key, None)
        self.paths = paths
        self.directory_version = stamp

    def _inputs(self, paths):
        files = list(paths)
        if self.kind == 'cover':
            for path in paths:
                files.extend(path.with_suffix(suffix) for suffix in
                             ('.scraping.json', '.face.json', '.poster.json'))
        return json.dumps([(str(path), signature(path)) for path in files], separators=(',', ':'))

    def _persist(self, rows):
        connection = self._connect()
        try:
            with connection:
                connection.executemany('INSERT INTO artwork_index VALUES(?,?,?,?) '
                                       'ON CONFLICT(scope,key) DO UPDATE SET '
                                       'signature=excluded.signature,payload=excluded.payload', rows)
        finally:
            connection.close()

    def prefetch(self, keys=None):
        """一页所需的新增或变化记录合为一次事务，未变化的行不写入。"""
        with self.lock:
            self._discover()
            self.pending = {}
            try:
                for key in tuple(self.paths) if keys is None else keys:
                    self.get(key)
                if self.pending:
                    self._persist(self.pending.values())
            except BaseException:
                for key in self.pending:
                    self.rows.pop(key, None)
                raise
            finally:
                self.pending = None

    def __getitem__(self, key):
        key = str(key).casefold()
        with self.lock:
            self._discover()
            paths = sorted(self.paths.get(key, ()))
            if not paths:
                if key in self.builtins:
                    return self.builtins[key]
                raise KeyError(key)
            stamp = self._inputs(paths)
            hit = self.rows.get(key)
            if hit is not None and hit[0] == stamp:
                value = hit[1]
            else:
                if self.kind == 'cover':
                    value = self.reader(paths[0]) if signature(paths[0]) is not None else None
                else:
                    versions = [item[0] for path in paths if (item := signature(path)) is not None]
                    value = f'{max(versions):x}' if versions else self.builtins.get(key)
                # 多文件原子替换期间不把混合版本持久化，下次读取继续核对该行。
                if stamp == self._inputs(paths):
                    row = (self.scope, key, stamp, json.dumps(value))
                    if self.pending is None:
                        self._persist([row])
                    else:
                        self.pending[key] = row
                    self.rows[key] = stamp, value
            if value is None:
                raise KeyError(key)
            return value

    def __iter__(self) -> Iterator[str]:
        with self.lock:
            self._discover()
            keys = tuple(self.paths.keys() | self.builtins.keys())
        for key in keys:
            if self.get(key) is not None:
                yield key

    def __len__(self):
        return sum(1 for _ in self)


class EntityImages(Mapping):
    """头像索引中的规范实体图片视图。"""

    def __init__(self, index):
        self.index = index

    def __getitem__(self, key):
        if str(key).startswith('#'):
            raise KeyError(key)
        return self.index[key]

    def __iter__(self):
        return (key for key in self.index if not key.startswith('#'))

    def __len__(self):
        return sum(1 for _ in self)


class GeneratedAvatars:
    """按需生成头像的成员查询，不枚举整个人物图库。"""

    def __init__(self, index):
        self.index = index

    def __contains__(self, key):
        return '#' + str(key) in self.index

    def __iter__(self):
        return (int(key[1:]) for key in self.index if key.startswith('#'))
