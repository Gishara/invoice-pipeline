"""Worker pools. Each pool scales independently; workers share nothing but the
database, the object store and the gateway.

  IngestWorker      one member of the Kafka consumer group (scale up to partitions)
  StageWorker       claims rows in its stage's states with a lease and runs that
                    stage's LangGraph graph; a crashed worker's lease expires and
                    another worker resumes the run from its checkpoint
  ReviewWorker      a person: opens review tasks and answers them
"""
from __future__ import annotations

import base64
import json
import random
import threading
import uuid
from collections import Counter

from langgraph.types import Command

from app.agents.graphs import SimulatedCrash
from app.domain.model import sha256
from app.ports import Conflict, InvoiceRepository, ObjectStore, ReviewerPolicy


class Metrics:
    def __init__(self):
        self._c, self._lock = Counter(), threading.Lock()

    def inc(self, key: str, n: int = 1):
        with self._lock:
            self._c[key] += n

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._c)


class Worker(threading.Thread):
    idle_s = 2.0  # simulated seconds to wait when there is nothing to do

    def __init__(self, name: str, clock, stop: threading.Event, metrics: Metrics):
        super().__init__(name=name, daemon=True)
        self.clock, self.stop, self.metrics = clock, stop, metrics
        self.errors: list[str] = []

    def run(self):
        while not self.stop.is_set():
            try:
                busy = self.step()
            except Exception as e:  # never let one bad job kill the pool
                self.errors.append(f"{type(e).__name__}: {e}")
                self.metrics.inc(f"{self.name.split('-')[0]}_unexpected_errors")
                busy = False
            if not busy:
                self.clock.sleep(self.idle_s, self.stop)

    def step(self) -> bool:
        raise NotImplementedError


class IngestWorker(Worker):
    def __init__(self, name, clock, stop, metrics, consumer, repo: InvoiceRepository, objects: ObjectStore,
                 crash_prob: float, seed: int):
        super().__init__(name, clock, stop, metrics)
        self.consumer, self.repo, self.objects = consumer, repo, objects
        self.crash_prob, self.rnd = crash_prob, random.Random(seed)

    def step(self) -> bool:
        records = self.consumer.poll(20)
        for rec in records:
            try:
                msg = json.loads(rec.value)
                pdf = base64.b64decode(msg["pdf_b64"])
            except Exception as e:  # poison message: keep it, never drop it
                self.repo.dead_letter(rec, repr(e))
                self.metrics.inc("dead_lettered")
                self.consumer.commit(rec)
                continue
            ref = self.objects.put(pdf)                                  # 1. durable bytes
            if not self.repo.insert_received(sha256(pdf), ref, rec):     # 2. the row
                self.metrics.inc("redeliveries_collapsed")
            if self.rnd.random() < self.crash_prob:                      # died before the offset commit
                self.metrics.inc("consumer_crashes")
                self.consumer.restart()
                return True
            self.consumer.commit(rec)                                    # 3. ack last
        return bool(records)


class StageWorker(Worker):
    def __init__(self, name, clock, stop, metrics, stage: str, states, graph, repo: InvoiceRepository,
                 lease_s: float):
        super().__init__(name, clock, stop, metrics)
        self.stage, self.states, self.graph, self.repo, self.lease_s = stage, states, graph, repo, lease_s

    def step(self) -> bool:
        row = self.repo.claim(self.states, self.name, self.lease_s, self.clock.now())
        if not row:
            return False
        self.metrics.inc(f"{self.stage}_jobs")
        thread, inp = row.get("graph_thread"), {"invoice_id": row["id"], "trace": []}
        if thread and thread.startswith(f"{self.stage}:") and self.graph.get_state(self._cfg(thread)).next:
            inp = None                                   # an earlier worker died mid-run: resume it
            self.metrics.inc(f"{self.stage}_resumed_from_checkpoint")
        else:
            thread = f"{self.stage}:{row['id']}:{uuid.uuid4().hex[:8]}"
            self.repo.set_graph_thread(row["id"], thread)
        try:
            self.graph.invoke(inp, self._cfg(thread))
        except SimulatedCrash:
            self.metrics.inc(f"{self.stage}_worker_crashes")
            return True                                  # process "died": keep the lease, let it expire
        except Conflict:
            self.metrics.inc(f"{self.stage}_conflicts")  # someone else moved the row; drop the job
        self.repo.release(row["id"], self.name)
        return True

    @staticmethod
    def _cfg(thread: str) -> dict:
        return {"configurable": {"thread_id": thread}}


class ReviewWorker(Worker):
    """A person in the review queue. Two kinds of work, answered first:
    1. answer an open task (think, decide, resume the paused graph)
    2. open a task for a newly flagged invoice (start the graph; it pauses for a person)"""

    def __init__(self, name, clock, stop, metrics, graph, repo, policy: ReviewerPolicy, think_s: float,
                 lease_s: float):
        super().__init__(name, clock, stop, metrics)
        self.graph, self.repo, self.policy, self.think_s, self.lease_s = graph, repo, policy, think_s, lease_s

    def step(self) -> bool:
        task = self.repo.claim_review(self.name, self.lease_s, self.clock.now())
        if task:
            row = self.repo.get(task["invoice_id"])
            self.clock.sleep(self.think_s)
            decision = self.policy.decide(row, json.loads(task["reasons_json"] or "[]"))
            decision["reviewer"] = self.name
            self.graph.invoke(Command(resume=decision), {"configurable": {"thread_id": row["graph_thread"]}})
            self.metrics.inc("reviews_decided")
            return True
        row = self.repo.claim(("NEEDS_REVIEW",), self.name, self.lease_s, self.clock.now())
        if not row:
            return False
        thread = f"review:{row['id']}:{uuid.uuid4().hex[:8]}"
        self.repo.set_graph_thread(row["id"], thread)
        self.repo.park(row["id"], until=1e12)           # held by a person; the pool must not re-claim it
        self.graph.invoke({"invoice_id": row["id"], "trace": []}, {"configurable": {"thread_id": thread}})
        self.repo.release(row["id"], self.name)         # now a reviewer may answer the task
        self.metrics.inc("reviews_opened")
        return True
