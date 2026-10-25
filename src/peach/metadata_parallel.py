"""有限影片并发；回调、账本和断点始终由调用线程处理。"""
from concurrent.futures import Future, ThreadPoolExecutor
from queue import Empty, Queue
import threading
import time
from contextlib import contextmanager

from .processing_control import CURRENT, MAX_METADATA_CONCURRENCY, valid_concurrency


class _WorkerControl:
    def __init__(self, parent, call, network):
        self.parent, self.call = parent, call
        self.network = network
        self.excluded = 0.0
        self.waiting_at = None
        self.parent_at_wait = 0.0

    @property
    def paused_seconds(self):
        paused = self.parent.paused_seconds if self.parent is not None else 0
        waiting = max(0, time.monotonic() - self.waiting_at - (paused - self.parent_at_wait)) if self.waiting_at is not None else 0
        return paused + self.excluded + waiting

    @contextmanager
    def source_wait(self):
        self.waiting_at = time.monotonic()
        self.parent_at_wait = self.parent.paused_seconds if self.parent is not None else 0
        try:
            yield
        finally:
            paused = self.parent.paused_seconds if self.parent is not None else 0
            self.excluded += max(0, time.monotonic() - self.waiting_at - (paused - self.parent_at_wait))
            self.waiting_at = None

    @property
    def mode(self):
        return self.parent.mode if self.parent is not None else 'standard'

    def checkpoint(self, kind='work'):
        return self.call(self.parent.checkpoint, kind) if self.parent is not None else self.call(lambda: None)


def collect_parallel(items, *, key, work, complete, capacity, checkpoint,
                     changed=lambda active, pending: None, network=lambda item, **values: None):
    """工作者通过 call 回到调用线程；同一 key 在任意时刻只执行一项。"""
    pending = list(items)
    events, stopped = Queue(), threading.Event()
    parent = CURRENT.get()

    def call(function, *args, **kwargs):
        if stopped.is_set():
            raise InterruptedError('资料采集已停止')
        reply = Future()
        events.put((reply, function, args, kwargs))
        while True:
            if stopped.is_set():
                raise InterruptedError('资料采集已停止')
            try:
                return reply.result(timeout=.05)
            except TimeoutError:
                if reply.done():
                    raise

    def run(item):
        token = CURRENT.set(_WorkerControl(parent, call, lambda **values: call(network, item, **values)))
        try:
            return work(item, call)
        finally:
            CURRENT.reset(token)

    active = {}
    executor = ThreadPoolExecutor(max_workers=MAX_METADATA_CONCURRENCY, thread_name_prefix='PeachMetadata')
    published = None
    try:
        while pending or active:
            checkpoint()
            keys = {key(item) for item in active.values()}
            limit = valid_concurrency(capacity())
            while len(active) < limit:
                index = next((i for i, item in enumerate(pending) if key(item) not in keys), None)
                if index is None:
                    break
                item = pending.pop(index)
                keys.add(key(item))
                active[executor.submit(run, item)] = item
            signature = (tuple(active), len(pending), limit)
            if signature != published:
                changed(list(active.values()), len(pending))
                published = signature
            try:
                reply, function, args, kwargs = events.get(timeout=.05)
            except Empty:
                pass
            else:
                try:
                    reply.set_result(function(*args, **kwargs))
                except BaseException as error:
                    reply.set_exception(error)
                    raise
            for future in list(active):
                if future.done():
                    item = active.pop(future)
                    complete(item, future.result())
        changed([], 0)
    finally:
        stopped.set()
        executor.shutdown(wait=True, cancel_futures=True)


class CollectionProgress:
    """调用线程统一发布工作者快照；每项拥有自己的阶段和来源。"""
    def __init__(self, update, capacity):
        self.update, self.capacity = update, capacity
        self.finished = self.failed = self.pending = 0
        self.items = {}
        self.results = []

    @staticmethod
    def row(item):
        asset, code = item[:2]
        return dict(asset_id=asset['id'], name=str(asset['name'] or ''), code=code or '',
                    action='querying_metadata', network_status='processing', source='', started_at=time.time())

    def changed(self, active, pending):
        self.items = {item[0]['id']: self.items.get(item[0]['id']) or self.row(item) for item in active}
        self.pending = pending
        self.publish()

    def progress(self, item, **values):
        row = self.items.setdefault(item[0]['id'], self.row(item))
        row.update({key: values[key] for key in ('network_status', 'source') if key in values})
        if values.get('current_action'):
            row['action'] = values['current_action']
        row['updated_at'] = time.time()
        self.publish()

    def complete(self, item, *, failed, covers):
        self.items.pop(item[0]['id'], None)
        self.finished += 1
        self.failed += int(failed)
        self.results = [dict(asset_id=item[0]['id'], name=str(item[0]['name'] or ''), code=item[1] or '',
                             failed=failed, covers=covers), *self.results[:7]]
        self.publish()

    def publish(self):
        rows = [dict(row) for row in self.items.values()]
        waiting = sum(row['network_status'] == 'waiting_source' for row in rows)
        requesting = sum(row['network_status'] == 'requesting_source' for row in rows)
        self.update(stage='采集外部资料与封面', current_asset_id=None, current_asset_name='',
                    current_action='', current_started_at=None, current_deadline_at=None,
                    metadata_concurrency=self.capacity(),
                    parallel=dict(limit=self.capacity(), active=len(rows), pending=self.pending,
                                  waiting=waiting, requesting=requesting, processing=len(rows)-waiting-requesting,
                                  finished=self.finished, succeeded=self.finished-self.failed, failed=self.failed,
                                  items=rows, results=list(self.results)))


