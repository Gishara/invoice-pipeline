"""Thread-safe in-process caches with hit/miss counters.

Cache only what is immutable or slow-changing and safe to be briefly stale:
  * PDF text, keyed by content hash       -> immutable, LRU
  * vendor master and purchase orders     -> slow-changing, TTL
Never cache invoice state, business-key duplicate checks or ERP look-ups: those
must always read the source of truth."""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Callable, Generic, Hashable, TypeVar

V = TypeVar("V")
_MISSING = object()


class LRUCache(Generic[V]):
    def __init__(self, maxsize: int = 1024):
        self.maxsize, self._d, self._lock = maxsize, OrderedDict(), threading.Lock()
        self.hits = self.misses = 0

    def get_or_load(self, key: Hashable, loader: Callable[[], V]) -> V:
        with self._lock:
            v = self._d.get(key, _MISSING)
            if v is not _MISSING:
                self._d.move_to_end(key)
                self.hits += 1
                return v  # type: ignore[return-value]
            self.misses += 1
        v = loader()  # load outside the lock; a racing duplicate load is harmless
        with self._lock:
            self._d[key] = v
            self._d.move_to_end(key)
            while len(self._d) > self.maxsize:
                self._d.popitem(last=False)
        return v

    def stats(self) -> dict:
        n = self.hits + self.misses
        return {"hits": self.hits, "misses": self.misses, "hit_rate": round(self.hits / n, 3) if n else 0}


class TTLCache(Generic[V]):
    def __init__(self, ttl_s: float = 300.0, clock: Callable[[], float] = time.monotonic):
        self.ttl, self.clock, self._d, self._lock = ttl_s, clock, {}, threading.Lock()
        self.hits = self.misses = 0

    def get_or_load(self, key: Hashable, loader: Callable[[], V]) -> V:
        now = self.clock()
        with self._lock:
            hit = self._d.get(key)
            if hit and hit[0] > now:
                self.hits += 1
                return hit[1]
            self.misses += 1
        v = loader()
        with self._lock:
            self._d[key] = (now + self.ttl, v)
        return v

    def invalidate(self, key: Hashable | None = None) -> None:
        with self._lock:
            self._d.clear() if key is None else self._d.pop(key, None)

    def stats(self) -> dict:
        n = self.hits + self.misses
        return {"hits": self.hits, "misses": self.misses, "hit_rate": round(self.hits / n, 3) if n else 0}
