"""资料工作者共享来源请求间隔与在途名额；等待支持暂停和取消。"""
from contextlib import contextmanager, nullcontext
import threading
import time

from .processing_control import CURRENT, checkpoint
from .scripting import HostLimiter, hostname_of


class MetadataSourceGate:
    def __init__(self, intervals, *, per_source=2, limiter=None):
        self.limiter = limiter or HostLimiter(intervals, default_interval=2.0)
        self.per_source = per_source
        self.lock = threading.Lock()
        self.slots = {}

    @contextmanager
    def slot(self, url):
        from .scraping_access import SOURCES, source_for
        source = source_for(url) or hostname_of(url)
        with self.lock:
            semaphore = self.slots.setdefault(source, threading.BoundedSemaphore(self.per_source))
        control = CURRENT.get()
        report = getattr(control, 'network', lambda **values: None)
        wait = getattr(control, 'source_wait', nullcontext)
        report(network_status='waiting_source', source=source)
        acquired = False
        try:
            with wait():
                while not semaphore.acquire(blocking=False):
                    checkpoint()
                    time.sleep(.05)
                acquired = True
                checkpoint()
                self.limiter.wait(SOURCES.get(source, {}).get('login') or url)
                checkpoint()
            report(network_status='requesting_source', source=source)
            yield
        finally:
            if acquired:
                semaphore.release()
            report(network_status='processing', source='')
