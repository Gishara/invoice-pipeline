"""All tunables in one place: business policy, failure injection, and runtime sizing."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RUNS_DIR = ROOT / "runs"


@dataclass(frozen=True)
class Policy:
    """Business policy. Finance signs these off."""
    auto_post_limit: float = 50_000.00
    money_tolerance: float = 0.02
    price_tolerance: float = 0.01          # 1% unit-price tolerance against the PO
    max_invoice_age_days: int = 180
    max_extraction_attempts: int = 5
    prompt_version: str = "extract-v3"
    today: str = "2026-11-05"
    buyer: str = "Northwind Finance BV"


@dataclass(frozen=True)
class Chaos:
    """Infrastructure failure injection. Model-side failures live in models.json."""
    kafka_producer_dup: float = 0.05
    consumer_crash_before_commit: float = 0.03
    worker_crash: float = 0.03
    erp_timeout_after_commit: float = 0.05
    erp_error_before_commit: float = 0.03
    erp_idempotent: bool = False

    @classmethod
    def preset(cls, name: str) -> "Chaos":
        return {"none": cls(0, 0, 0, 0, 0, False),
                "heavy": cls(0.15, 0.10, 0.10, 0.15, 0.10, False)}.get(name, cls())


@dataclass(frozen=True)
class PoolSizes:
    """Workers per stage. Each stage scales independently."""
    ingest: int = 3           # Kafka consumers in one group (<= partitions)
    extraction: int = 8       # bounded by provider rate limits, not CPU
    review: int = 4           # people
    posting: int = 2          # bounded by what the ERP tolerates


@dataclass(frozen=True)
class Settings:
    model: str = "sim-strong"
    fallback: str | None = "sim-fast"
    chaos: Chaos = field(default_factory=Chaos)
    pools: PoolSizes = field(default_factory=PoolSizes)
    policy: Policy = field(default_factory=Policy)
    partitions: int = 12
    rpm: int = 600                     # requests/minute we allow ourselves across ALL workers
    max_in_flight: int = 64            # bulkhead: concurrent model calls across all workers
    lease_s: float = 120.0             # simulated seconds a claim stays valid
    review_think_s: float = 90.0       # simulated seconds a person spends per review
    time_scale: float = 0.004          # real seconds per simulated second
    seed: int = 42
    naive_poster: bool = False         # ablation: retry ERP posts blind
    run_name: str | None = None

    @property
    def name(self) -> str:
        return self.run_name or (self.model + ("-naive-poster" if self.naive_poster else ""))
