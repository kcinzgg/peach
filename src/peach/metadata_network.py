"""资料工作者共享来源请求间隔与在途名额；等待支持暂停和取消。"""
from contextlib import contextmanager, nullcontext
from collections import deque
from contextvars import ContextVar
import threading
import time

from .processing_control import CURRENT, checkpoint
from .scripting import HostLimiter, hostname_of


class SourceDeferred(BaseException):
    """来源尚未就绪的调度信号，不属于来源失败或缺失。"""
    def __init__(self, source, ready):
        self.source, self.ready = source, ready
        super().__init__(source)


_ATTEMPT = ContextVar('metadata_source_attempt', default=None)


class _Attempt:
    def __init__(self):
        self.started = False
        self.eligible = True
        self.gates = set()

    def close(self):
        for gate in self.gates:
            gate.forget(self)


def defer_until(source, ready):
    """首次请求尚未发出时让出来源；已开始的来源操作完整执行。"""
    attempt = _ATTEMPT.get()
    if attempt is not None and not attempt.started and not ready():
        raise SourceDeferred(source, ready)


class SourceResult:
    def __init__(self, value=None, error=None):
        self.value, self.error = value, error

    def get(self):
        if self.error is not None:
            raise self.error
        return self.value


@contextmanager
def source_results(items, work):
    """在同一影片内推进可用来源；首次联网后不重放操作，退出时释放排队位置。"""
    pending = [(item, _Attempt(), None) for item in items]
    attempts = [attempt for _, attempt, _ in pending]
    control = CURRENT.get()
    report = getattr(control, 'network', lambda **values: None)
    wait = getattr(control, 'source_wait', nullcontext)

    def results():
        while pending:
            checkpoint()
            completed = False
            for index, (item, attempt, deferred) in enumerate(pending):
                if deferred is not None and not deferred.ready():
                    continue
                for _, other, _ in pending:
                    other.eligible = other is attempt
                token = _ATTEMPT.set(attempt)
                try:
                    try:
                        result = SourceResult(value=work(item))
                    except SourceDeferred as busy:
                        pending[index] = (item, attempt, busy)
                        continue
                    except InterruptedError:
                        raise
                    except Exception as error:
                        result = SourceResult(error=error)
                finally:
                    _ATTEMPT.reset(token)
                    for _, other, _ in pending:
                        other.eligible = True
                pending.pop(index)
                attempt.close()
                completed = True
                yield item, result
                break
            if not completed and pending:
                sources = dict.fromkeys(busy.source for _, _, busy in pending if busy is not None)
                report(network_status='waiting_source', source='、'.join(sources))
                with wait():
                    while not any(busy is None or busy.ready() for _, _, busy in pending):
                        checkpoint()
                        time.sleep(.05)

    iterator = results()
    try:
        yield iterator
    finally:
        iterator.close()
        for attempt in attempts:
            attempt.close()


class MetadataSourceGate:
    def __init__(self, intervals, *, per_source=2, limiter=None):
        self.limiter = limiter or HostLimiter(intervals, default_interval=2.0)
        self.per_source = per_source
        self.lock = threading.Lock()
        self.slots = {}
        self.waiters = {}

    def forget(self, token):
        """影片完成或取消时释放它的排队位置。"""
        with self.lock:
            for queue in self.waiters.values():
                if token in queue:
                    queue.remove(token)

    def _ready(self, source, token, url):
        with self.lock:
            queue = self._queue(source, token)
            if not queue or queue[0] is not token:
                return False
            semaphore = self.slots[source]
            if not semaphore.acquire(blocking=False):
                return False
            semaphore.release()
            return self.limiter.ready_in(url) <= 0

    def _queue(self, source, token):
        """只让正在等待的操作占队首；访问其他来源的影片暂时让位。"""
        queue = self.waiters.setdefault(source, deque())
        for waiting in tuple(queue):
            if not waiting.eligible:
                queue.remove(waiting)
        if token.eligible and token not in queue:
            queue.append(token)
        return queue

    @contextmanager
    def slot(self, url='', *, source=None):
        from .scraping_access import SOURCES, source_for
        source = source or source_for(url) or hostname_of(url)
        with self.lock:
            semaphore = self.slots.setdefault(source, threading.BoundedSemaphore(self.per_source))
        control = CURRENT.get()
        report = getattr(control, 'network', lambda **values: None)
        wait = getattr(control, 'source_wait', nullcontext)
        report(network_status='waiting_source', source=source)
        acquired = False
        pacing_url = SOURCES.get(source, {}).get('login') or url or f'https://{source}.invalid/'
        try:
            attempt = _ATTEMPT.get()
            if attempt is not None and not attempt.started:
                attempt.gates.add(self)
                checkpoint()
                with self.lock:
                    queue = self._queue(source, attempt)
                    if queue[0] is attempt and semaphore.acquire(blocking=False):
                        if self.limiter.try_wait(pacing_url):
                            acquired = True
                            attempt.started = True
                            queue.popleft()
                        else:
                            semaphore.release()
                if not acquired:
                    raise SourceDeferred(source, lambda: self._ready(source, attempt, pacing_url))
            else:
                with wait():
                    while not semaphore.acquire(blocking=False):
                        checkpoint()
                        time.sleep(.05)
                    acquired = True
                    checkpoint()
                    self.limiter.wait(pacing_url)
                    checkpoint()
            report(network_status='requesting_source', source=source)
            yield
        finally:
            if acquired:
                semaphore.release()
            if acquired:
                report(network_status='processing', source='')
