"""Scaled real time. Workers really run in parallel, but a 9-second model call or a
90-second review takes `time_scale` of that in wall time, so a month-end backlog
can be replayed in seconds while concurrency, leases and backoff stay real."""
from __future__ import annotations

import threading
import time


class ScaledClock:
    def __init__(self, scale: float):
        self.scale = scale
        self._t0 = time.monotonic()

    def now(self) -> float:
        """Simulated seconds since start."""
        return (time.monotonic() - self._t0) / self.scale

    def sleep(self, simulated_s: float, stop: threading.Event | None = None) -> None:
        real = max(0.0, simulated_s) * self.scale
        if stop is not None:
            stop.wait(real)
        else:
            time.sleep(real)
