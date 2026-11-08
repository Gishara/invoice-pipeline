"""LLM gateway: the one place that knows about provider limits and failures.

Shared by every extraction worker, so its limits are global, not per worker:
  * token bucket   -> all workers together stay under our requests/minute
                      (in production this bucket lives in Redis so it spans processes)
  * bulkhead       -> a cap on concurrent calls, so a slow provider can't tie up every worker
  * retries        -> exponential backoff with jitter, honouring Retry-After
  * circuit breaker per model, and failover to the fallback model
Cost accounting prices cached prompt tokens at 10% of the normal input rate."""
from __future__ import annotations

import random
import threading
from dataclasses import dataclass, field

from app.adapters.models import load_registry
from app.ports import Completion, ProviderTimeout, ProviderUnavailable, RateLimited, ServerError

CACHED_INPUT_PRICE_FACTOR = 0.10


@dataclass
class Breaker:
    window: list = field(default_factory=list)
    open_until: float = 0.0
    trips: int = 0

    def record(self, ok: bool, now: float):
        self.window = (self.window + [ok])[-20:]
        if len(self.window) >= 10 and self.window.count(False) / len(self.window) > 0.5:
            self.open_until, self.window = now + 60, []
            self.trips += 1

    def allow(self, now: float) -> bool:
        return now >= self.open_until


class TokenBucket:
    def __init__(self, rate_per_s: float, capacity: float, clock):
        self.rate, self.cap, self.clock = rate_per_s, capacity, clock
        self.tokens, self.last = capacity, clock.now()
        self._lock = threading.Lock()

    def take(self) -> float:
        """Reserve one request slot; returns how long (simulated s) the caller waited."""
        with self._lock:
            now = self.clock.now()
            self.tokens = min(self.cap, self.tokens + (now - self.last) * self.rate)
            self.last = now
            self.tokens -= 1
            wait = 0.0 if self.tokens >= 0 else -self.tokens / self.rate
        if wait:
            self.clock.sleep(wait)
        return wait


class LLMGateway:
    def __init__(self, models: dict, order: list[str], clock, rpm: int, max_in_flight: int,
                 max_tries: int = 3, seed: int = 1):
        self.models, self.order, self.clock, self.max_tries = models, order, clock, max_tries
        self.breakers = {m: Breaker() for m in order}
        self.bucket = TokenBucket(rpm / 60, max(1, rpm / 12), clock)
        self.bulkhead = threading.BoundedSemaphore(max_in_flight)
        self.prices = load_registry()
        self._lock = threading.Lock()
        self._rnd = random.Random(seed)
        self.stats = {"calls": 0, "ok": 0, "timeouts": 0, "rate_limited": 0, "server_errors": 0, "failovers": 0,
                      "unavailable": 0, "tokens_in": 0, "tokens_cached": 0, "tokens_out": 0, "cost_usd": 0.0,
                      "cost_without_prompt_cache_usd": 0.0, "throttle_wait_s": 0.0, "max_concurrent": 0,
                      "latencies": [], "by_model": {}}
        self._in_flight = 0

    @property
    def primary(self) -> str:
        return self.order[0]

    def _bump(self, k: str, v=1):
        with self._lock:
            self.stats[k] += v

    def _cost(self, c: Completion) -> tuple[float, float]:
        p = self.prices[c.model]
        fresh = c.tokens_in - c.cached_tokens
        cost = (fresh + c.cached_tokens * CACHED_INPUT_PRICE_FACTOR) / 1e6 * p["price_in_per_mtok"] \
            + c.tokens_out / 1e6 * p["price_out_per_mtok"]
        nocache = c.tokens_in / 1e6 * p["price_in_per_mtok"] + c.tokens_out / 1e6 * p["price_out_per_mtok"]
        return cost, nocache

    def complete(self, system: str, prompt: str, document_text: str, attempt_key: str) -> Completion:
        last = None
        for i, name in enumerate(self.order):
            br = self.breakers[name]
            if not br.allow(self.clock.now()):
                continue
            if i > 0:
                self._bump("failovers")
            for attempt in range(self.max_tries):
                self._bump("throttle_wait_s", self.bucket.take())
                with self.bulkhead:
                    with self._lock:
                        self._in_flight += 1
                        self.stats["calls"] += 1
                        self.stats["max_concurrent"] = max(self.stats["max_concurrent"], self._in_flight)
                    try:
                        c = self.models[name].complete(system, prompt, document_text, f"{attempt_key}|{attempt}")
                        err = None
                    except (RateLimited, ProviderTimeout, ServerError) as e:
                        c, err = None, e
                    finally:
                        with self._lock:
                            self._in_flight -= 1
                if err is None:
                    with self._lock:
                        br.record(True, self.clock.now())
                        cost, nocache = self._cost(c)
                        s = self.stats
                        s["ok"] += 1; s["tokens_in"] += c.tokens_in; s["tokens_cached"] += c.cached_tokens
                        s["tokens_out"] += c.tokens_out; s["cost_usd"] += cost
                        s["cost_without_prompt_cache_usd"] += nocache; s["latencies"].append(c.latency_s)
                        s["by_model"][c.model] = s["by_model"].get(c.model, 0) + 1
                    return c
                last = err
                with self._lock:
                    br.record(False, self.clock.now())
                    jitter = self._rnd.uniform(0.5, 1.5)
                if isinstance(err, RateLimited):
                    self._bump("rate_limited")
                    self.clock.sleep(err.retry_after)
                else:
                    self._bump("timeouts" if isinstance(err, ProviderTimeout) else "server_errors")
                    self.clock.sleep(min(30, 2 ** attempt) * jitter)
                if not br.allow(self.clock.now()):
                    break
        self._bump("unavailable")
        raise ProviderUnavailable(str(last) if last else "all circuits open")

    def snapshot(self) -> dict:
        with self._lock:
            s = dict(self.stats)
            lat = sorted(s.pop("latencies")) or [0]
        s["latency_p50_s"], s["latency_p95_s"] = round(lat[len(lat) // 2], 2), round(lat[int(len(lat) * .95)], 2)
        s["breaker_trips"] = {k: b.trips for k, b in self.breakers.items()}
        s["cost_usd"] = round(s["cost_usd"], 4)
        s["cost_without_prompt_cache_usd"] = round(s["cost_without_prompt_cache_usd"], 4)
        return s
