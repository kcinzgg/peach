"""有限影片并发；回调、账本和断点始终由调用线程处理。"""
from concurrent.futures import Future, ThreadPoolExecutor
from queue import Empty, Queue
import threading

from .processing_control import CURRENT


def workers_for(mode):
    return {'low': 1, 'standard': 2, 'high': 4}[mode]


class _WorkerControl:
    def __init__(self, parent, call):
        self.parent, self.call = parent, call

    @property
    def paused_seconds(self):
        return self.parent.paused_seconds if self.parent is not None else 0

    @property
    def mode(self):
        return self.parent.mode if self.parent is not None else 'standard'

    def checkpoint(self, kind='work'):
        return self.call(self.parent.checkpoint, kind) if self.parent is not None else self.call(lambda: None)


def collect_parallel(items, *, key, work, complete, capacity, checkpoint):
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
        token = CURRENT.set(_WorkerControl(parent, call))
        try:
            return work(item, call)
        finally:
            CURRENT.reset(token)

    active = {}
    executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix='PeachMetadata')
    try:
        while pending or active:
            checkpoint()
            keys = {key(item) for item in active.values()}
            while len(active) < capacity():
                index = next((i for i, item in enumerate(pending) if key(item) not in keys), None)
                if index is None:
                    break
                item = pending.pop(index)
                keys.add(key(item))
                active[executor.submit(run, item)] = item
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
    finally:
        stopped.set()
        executor.shutdown(wait=True, cancel_futures=True)


