"""批次级互斥：原文清除、全量重跑、冻结复算都按 batch_id 串行。

清除与重试可能同时来自两个请求；同一批次必须一先一后，不同批次互不阻塞。
用 RLock 让「重试入口已持锁、再调用 run」这种同线程嵌套不会自锁。
"""
from __future__ import annotations

import threading
from contextlib import contextmanager

_registry: dict[str, threading.RLock] = {}
_registry_guard = threading.Lock()


def _lock_for(batch_id: str) -> threading.RLock:
    with _registry_guard:
        lock = _registry.get(batch_id)
        if lock is None:
            lock = threading.RLock()
            _registry[batch_id] = lock
        return lock


@contextmanager
def batch_lock(batch_id: str):
    lock = _lock_for(batch_id)
    lock.acquire()
    try:
        yield
    finally:
        lock.release()
