"""Composition root: the only module that knows every concrete class.

It builds the adapters, injects them into services and agents, starts one worker
pool per stage, feeds the topic, and waits until every invoice is finished.
Swapping SQLite for Postgres, the Kafka simulator for a real client, or the mock
ERP for a real one means changing this file only."""
from __future__ import annotations

import base64
import json
import shutil
import sqlite3
import threading
import time
from pathlib import Path

from langgraph.checkpoint.sqlite import SqliteSaver

from app.adapters.erp_sim import MockErp
from app.adapters.kafka_sim import Broker, GroupConsumer, Producer
from app.adapters.models import build_model
from app.adapters.object_store import FileObjectStore
from app.adapters.reference_data import CachedReferenceData, JsonReferenceSource
from app.adapters.sqlite_repository import SqliteInvoiceRepository
from app.agents.graphs import ExtractionStage, PostingStage, RandomCrashInjector, ReviewStage
from app.clock import ScaledClock
from app.config import DATA_DIR, RUNS_DIR, Settings
from app.domain.model import STAGE_STATES
from app.services.extraction import ExtractionService, PdfTextExtractor
from app.services.gateway import LLMGateway
from app.services.posting import PostingService
from app.services.validation import ValidationService
from app.sim.reviewer import GroundTruthReviewer
from app.workers.pools import IngestWorker, Metrics, ReviewWorker, StageWorker

TOPIC, GROUP = "ap.invoices.received", "invoice-ingest"


class Runtime:
    def __init__(self, settings: Settings, run_dir: Path | None = None, log=print):
        self.cfg, self.log = settings, log
        self.dir = run_dir or RUNS_DIR / settings.name
        if self.dir.exists():
            shutil.rmtree(self.dir)
        self.dir.mkdir(parents=True)
        s = settings
        self.clock = ScaledClock(s.time_scale)
        self.stop = threading.Event()
        self.metrics = Metrics()
        self.truth = json.loads((DATA_DIR / "ground_truth.json").read_text())

        # ---- adapters: where state lives
        self.broker = Broker(str(self.dir / "kafka_broker.db"))
        self.broker.create_topic(TOPIC, s.partitions)
        self.objects = FileObjectStore(self.dir / "object_store")
        self.repo = SqliteInvoiceRepository(self.dir / "invoices.db", self.clock)
        self.ckpt = SqliteSaver(sqlite3.connect(self.dir / "langgraph_checkpoints.db", check_same_thread=False))
        self.erp = MockErp(str(self.dir / "erp.db"), s.chaos, seed=s.seed + 1)
        self.ref = CachedReferenceData(JsonReferenceSource(DATA_DIR))

        # ---- services
        order = [s.model] + ([s.fallback] if s.fallback and s.fallback != s.model else [])
        models = {m: build_model(m, self.clock, s.seed + i) for i, m in enumerate(order)}
        self.gateway = LLMGateway(models, order, self.clock, s.rpm, s.max_in_flight, seed=s.seed)
        self.crash = RandomCrashInjector(s.chaos.worker_crash, s.seed + 2)
        self.texts = PdfTextExtractor(self.objects)
        self.extractor = ExtractionService(self.gateway, self.repo, s.model, s.policy.prompt_version)
        self.validator = ValidationService(self.repo, self.ref, s.policy)
        self.poster = PostingService(self.repo, self.erp, self.crash, self.clock, naive=s.naive_poster)

        # ---- agents (one graph per stage, shared checkpointer)
        self.g_extract = ExtractionStage(self.repo, self.texts, self.extractor, self.validator, self.crash,
                                         self.clock, s.policy, s.seed + 3).build(self.ckpt)
        self.g_review = ReviewStage(self.repo, self.ref).build(self.ckpt)
        self.g_post = PostingStage(self.poster).build(self.ckpt)

        # ---- pools
        p, m, c, st = s.pools, self.metrics, self.clock, self.stop
        self.workers = []
        for i in range(p.ingest):
            consumer = GroupConsumer(self.broker, TOPIC, GROUP, i, p.ingest)
            self.workers.append(IngestWorker(f"ingest-{i}", c, st, m, consumer, self.repo, self.objects,
                                             s.chaos.consumer_crash_before_commit, s.seed + 10 + i))
        for i in range(p.extraction):
            self.workers.append(StageWorker(f"extraction-{i}", c, st, m, "extraction", STAGE_STATES["extraction"],
                                            self.g_extract, self.repo, lease_s=600))
        reviewer = GroundTruthReviewer()
        for i in range(p.review):
            self.workers.append(ReviewWorker(f"review-{i}", c, st, m, self.g_review, self.repo, reviewer,
                                             s.review_think_s, lease_s=s.review_think_s * 4))
        for i in range(p.posting):
            self.workers.append(StageWorker(f"posting-{i}", c, st, m, "posting", STAGE_STATES["posting"],
                                            self.g_post, self.repo, lease_s=s.lease_s))
        self.producer = Producer(self.broker, s.chaos.kafka_producer_dup, seed=s.seed + 4)

    # ------------------------------------------------------------ feeding the topic
    def _produce(self, batch: int, every_s: float):
        self.producer.send(TOPIC, "unknown-sender", b"{not json")
        self.producer.send(TOPIC, "unknown-sender", json.dumps(
            {"message_id": "m-bad", "pdf_b64": base64.b64encode(b"%PDF-1.4 truncated upload").decode()}).encode())
        for i in range(0, len(self.truth), batch):
            for t in self.truth[i:i + batch]:
                pdf = (DATA_DIR / "pdfs" / t["file"]).read_bytes()
                msg = {"message_id": f"m-{t['seq']}", "filename": t["file"], "received_at": self.clock.now(),
                       "pdf_b64": base64.b64encode(pdf).decode()}
                self.producer.send(TOPIC, t["supplier_id"] or "unknown-sender", json.dumps(msg).encode())
            self.clock.sleep(every_s, self.stop)
        self.produced = True

    # ------------------------------------------------------------ run
    def run(self, batch: int = 50, every_s: float = 20, timeout_real_s: float = 900) -> dict:
        t0 = time.time()
        self.produced = False
        feeder = threading.Thread(target=self._produce, args=(batch, every_s), daemon=True)
        for w in self.workers:
            w.start()
        feeder.start()
        last_log, self.extraction_done_s = 0.0, None
        while time.time() - t0 < timeout_real_s:
            time.sleep(0.25)
            open_n, lag = self.repo.count_open(), self.broker.lag(GROUP, TOPIC)
            if self.extraction_done_s is None and self.produced and lag == 0:
                by = self.repo.count_by_state()
                if not any(by.get(k) for k in ("RECEIVED", "EXTRACTING", "EXTRACTED")):
                    self.extraction_done_s = self.clock.now()   # extraction backlog drained
            if self.produced and lag == 0 and open_n == 0:
                break
            if time.time() - last_log > 10:
                last_log = time.time()
                self.log(f"  [{self.cfg.name}] t={self.clock.now() / 60:.0f} sim-min lag={lag} open={open_n} "
                         f"{self.repo.count_by_state()}")
        self.stop.set()
        for w in self.workers:
            w.join(timeout=10)
        wall = time.time() - t0
        sim_s = self.clock.now()
        errors = [e for w in self.workers for e in w.errors]
        out = {
            "settings": {"model": self.cfg.model, "fallback": self.cfg.fallback, "naive_poster": self.cfg.naive_poster,
                         "pools": vars(self.cfg.pools), "rpm": self.cfg.rpm, "max_in_flight": self.cfg.max_in_flight,
                         "partitions": self.cfg.partitions, "chaos": vars(self.cfg.chaos)},
            "completed": self.repo.count_open() == 0,
            "wall_s": round(wall, 1), "simulated_minutes": round(sim_s / 60, 1),
            "invoices_per_simulated_hour": round(len(self.truth) / (sim_s / 3600), 0) if sim_s else 0,
            "extraction_drained_minutes": round((self.extraction_done_s or sim_s) / 60, 1),
            "extraction_per_simulated_hour": round(len(self.truth) / ((self.extraction_done_s or sim_s) / 3600)),
            "metrics": self.metrics.snapshot(),
            "worker_crashes_by_point": dict(self.crash.counts),
            "producer": {"sent": self.producer.sent, "duplicates": self.producer.duplicated},
            "erp": self.erp.stats(), "gateway": self.gateway.snapshot(),
            "caches": {"pdf_text": self.texts.cache.stats(), "reference_data": self.ref.stats(),
                       "extraction_results_reused": self.extractor.cache_hits},
            "extraction_repairs": self.extractor.repairs,
            "unexpected_errors": errors[:10],
        }
        (self.dir / "run_metrics.json").write_text(json.dumps(out, indent=1, default=str))
        return out
