"""InvoiceRepository on SQLite, standing in for Postgres.

Same guarantees the Postgres version relies on:
  * unique doc hash                      -> layer-1 dedupe
  * partial unique index on business key -> the database refuses a second post
  * conditional updates on version       -> a stale worker cannot overwrite newer progress
  * atomic claim with a lease            -> each row goes to one worker; a dead worker's
                                            claim expires and another picks it up
In Postgres the claim is `SELECT ... FOR UPDATE SKIP LOCKED`; here it is a single
UPDATE ... RETURNING under SQLite's write lock. Each thread gets its own connection."""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Iterable, Optional

from app.domain.model import BEFORE_VALIDATION
from app.ports import Conflict, DuplicateBusinessKey, Record

SCHEMA = """
CREATE TABLE IF NOT EXISTS invoice(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_sha256 TEXT NOT NULL UNIQUE,
  source_topic TEXT, source_partition INT, source_offset INT, source_key TEXT,
  pdf_ref TEXT NOT NULL,
  state TEXT NOT NULL,
  version INT NOT NULL DEFAULT 0,
  extraction_attempts INT NOT NULL DEFAULT 0,
  next_attempt_at REAL NOT NULL DEFAULT 0,
  lease_owner TEXT, lease_until REAL,
  graph_thread TEXT,
  supplier_id TEXT, invoice_no_norm TEXT, total REAL, currency TEXT,
  fields_json TEXT, reasons_json TEXT, model_used TEXT,
  created_at REAL, updated_at REAL);
CREATE UNIQUE INDEX IF NOT EXISTS ux_business_key ON invoice(supplier_id, invoice_no_norm)
  WHERE state IN ('POSTING','POST_UNKNOWN','POSTED');
CREATE INDEX IF NOT EXISTS ix_claim ON invoice(state, next_attempt_at, id);
CREATE INDEX IF NOT EXISTS ix_sender ON invoice(source_key, id);
CREATE INDEX IF NOT EXISTS ix_key ON invoice(supplier_id, invoice_no_norm);

CREATE TABLE IF NOT EXISTS extraction_cache(
  doc_sha256 TEXT, model TEXT, prompt_version TEXT, raw TEXT, tokens_in INT, tokens_out INT,
  PRIMARY KEY(doc_sha256, model, prompt_version));

CREATE TABLE IF NOT EXISTS erp_outbox(
  invoice_id INT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE,
  payload TEXT NOT NULL, status TEXT NOT NULL, attempts INT NOT NULL DEFAULT 0, erp_doc_id TEXT);

CREATE TABLE IF NOT EXISTS review_task(
  invoice_id INT PRIMARY KEY, reasons_json TEXT, fields_json TEXT, status TEXT,
  decision TEXT, reviewer TEXT, lease_owner TEXT, lease_until REAL, created_at REAL, decided_at REAL);

CREATE TABLE IF NOT EXISTS invoice_event(
  id INTEGER PRIMARY KEY AUTOINCREMENT, invoice_id INT, from_state TEXT, to_state TEXT,
  actor TEXT, reason TEXT, at REAL);

CREATE TABLE IF NOT EXISTS ingest_dlq(source_topic TEXT, source_partition INT, source_offset INT,
  error TEXT, raw BLOB, PRIMARY KEY(source_topic, source_partition, source_offset));
"""


def _dumps(o) -> str:
    return json.dumps(o, sort_keys=True, default=str)


class SqliteInvoiceRepository:
    def __init__(self, path: Path, clock):
        self.path, self.clock = str(path), clock
        self._local = threading.local()
        self._db().executescript(SCHEMA)

    # ------------------------------------------------------------ plumbing
    def _db(self) -> sqlite3.Connection:
        c = getattr(self._local, "conn", None)
        if c is None:
            c = sqlite3.connect(self.path, isolation_level=None, timeout=60)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA busy_timeout=60000")
            c.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = c
        return c

    def _tx(self, fn):
        db = self._db()
        db.execute("BEGIN IMMEDIATE")
        try:
            out = fn(db)
            db.execute("COMMIT")
            return out
        except Exception:
            db.execute("ROLLBACK")
            raise

    def _event(self, db, inv_id, frm, to, actor, reason):
        db.execute("INSERT INTO invoice_event(invoice_id, from_state, to_state, actor, reason, at)"
                   " VALUES(?,?,?,?,?,?)", (inv_id, frm, to, actor, reason, self.clock.now()))

    def query(self, sql: str, args: tuple = ()) -> list[dict]:
        return [dict(r) for r in self._db().execute(sql, args)]

    # ------------------------------------------------------------ ingest
    def insert_received(self, doc_sha: str, pdf_ref: str, rec: Record) -> bool:
        def tx(db):
            cur = db.execute(
                "INSERT INTO invoice(doc_sha256, source_topic, source_partition, source_offset, source_key,"
                " pdf_ref, state, created_at, updated_at) VALUES(?,?,?,?,?,?, 'RECEIVED', ?, ?)"
                " ON CONFLICT(doc_sha256) DO NOTHING",
                (doc_sha, rec.topic, rec.partition, rec.offset, rec.key, pdf_ref, self.clock.now(), self.clock.now()))
            if cur.rowcount:
                self._event(db, cur.lastrowid, None, "RECEIVED", "ingest", f"{rec.topic}/{rec.partition}/{rec.offset}")
            return bool(cur.rowcount)
        return self._tx(tx)

    def dead_letter(self, rec: Record, error: str) -> None:
        self._db().execute("INSERT OR IGNORE INTO ingest_dlq VALUES(?,?,?,?,?)",
                           (rec.topic, rec.partition, rec.offset, error, rec.value))

    # ------------------------------------------------------------ reads
    def get(self, inv_id: int) -> dict:
        r = self._db().execute("SELECT * FROM invoice WHERE id=?", (inv_id,)).fetchone()
        return dict(r) if r else {}

    def same_business_key(self, supplier_id: str, inv_no_norm: str, exclude_id: int) -> list[dict]:
        return self.query("SELECT * FROM invoice WHERE supplier_id=? AND invoice_no_norm=? AND id<>? ORDER BY id",
                          (supplier_id, inv_no_norm, exclude_id))

    def earlier_from_same_sender(self, inv_id: int) -> list[dict]:
        """Earlier arrivals from the same sender that have not been validated yet."""
        q = ",".join("?" * len(BEFORE_VALIDATION))
        return self.query(f"SELECT b.id, b.state, b.extraction_attempts, b.fields_json FROM invoice a JOIN invoice b"
                          f" ON b.source_key = a.source_key AND b.id < a.id"
                          f" WHERE a.id=? AND b.state IN ({q})", (inv_id, *BEFORE_VALIDATION))

    def count_open(self) -> int:
        return self._db().execute("SELECT COUNT(*) FROM invoice WHERE state NOT IN ('POSTED','REJECTED','DUPLICATE')"
                                  ).fetchone()[0]

    def count_by_state(self) -> dict:
        return {r["state"]: r["n"] for r in self.query("SELECT state, COUNT(*) n FROM invoice GROUP BY state")}

    # ------------------------------------------------------------ work distribution
    def claim(self, states: Iterable[str], worker: str, lease_s: float, now: float) -> Optional[dict]:
        states = tuple(states)
        q = ",".join("?" * len(states))
        r = self._db().execute(
            f"UPDATE invoice SET lease_owner=?, lease_until=? WHERE id = ("
            f"  SELECT id FROM invoice WHERE state IN ({q}) AND next_attempt_at<=?"
            f"  AND (lease_until IS NULL OR lease_until<?) ORDER BY next_attempt_at, id LIMIT 1)"
            f" RETURNING *", (worker, now + lease_s, *states, now, now)).fetchone()
        return dict(r) if r else None

    def release(self, inv_id: int, worker: str) -> None:
        self._db().execute("UPDATE invoice SET lease_owner=NULL, lease_until=NULL WHERE id=? AND lease_owner=?",
                           (inv_id, worker))

    # ------------------------------------------------------------ state machine
    def transition(self, inv_id: int, from_states: set[str], to: str, actor: str, reason: str = "",
                   **fields: Any) -> dict:
        def tx(db):
            row = db.execute("SELECT state, version FROM invoice WHERE id=?", (inv_id,)).fetchone()
            if row["state"] not in from_states:
                raise Conflict(f"invoice {inv_id} is {row['state']}, expected {sorted(from_states)}")
            sets = ["state=?", "version=version+1", "updated_at=?"] + [f"{k}=?" for k in fields]
            cur = db.execute(f"UPDATE invoice SET {', '.join(sets)} WHERE id=? AND version=?",
                             [to, self.clock.now(), *fields.values(), inv_id, row["version"]])
            if cur.rowcount != 1:
                raise Conflict(f"invoice {inv_id} changed underneath us")
            self._event(db, inv_id, row["state"], to, actor, reason)
        self._tx(tx)
        return self.get(inv_id)

    def park(self, inv_id: int, until: float) -> None:
        """Hide a row from its pool until `until` (used while a person holds it)."""
        self._db().execute("UPDATE invoice SET next_attempt_at=? WHERE id=?", (until, inv_id))

    def set_graph_thread(self, inv_id: int, thread: str) -> None:
        self._db().execute("UPDATE invoice SET graph_thread=? WHERE id=?", (thread, inv_id))

    # ------------------------------------------------------------ extraction cache
    def cached_extraction(self, doc_sha, model, prompt_version):
        r = self._db().execute("SELECT * FROM extraction_cache WHERE doc_sha256=? AND model=? AND prompt_version=?",
                               (doc_sha, model, prompt_version)).fetchone()
        return dict(r) if r else None

    def cache_extraction(self, doc_sha, model, prompt_version, raw, tokens_in, tokens_out):
        self._db().execute("INSERT OR REPLACE INTO extraction_cache VALUES(?,?,?,?,?,?)",
                           (doc_sha, model, prompt_version, raw, tokens_in, tokens_out))

    # ------------------------------------------------------------ outbox
    def begin_posting(self, inv_id: int, from_states: set[str], key: str, payload: dict) -> None:
        """VALIDATED/APPROVED -> POSTING plus the outbox row, in ONE transaction."""
        def tx(db):
            row = db.execute("SELECT state, version FROM invoice WHERE id=?", (inv_id,)).fetchone()
            if row["state"] not in from_states:
                raise Conflict(row["state"])
            db.execute("UPDATE invoice SET state='POSTING', version=version+1, updated_at=? WHERE id=? AND version=?",
                       (self.clock.now(), inv_id, row["version"]))
            db.execute("INSERT INTO erp_outbox(invoice_id, idempotency_key, payload, status) VALUES(?,?,?, 'PENDING')",
                       (inv_id, key, _dumps(payload)))
            self._event(db, inv_id, row["state"], "POSTING", "poster", key)
        try:
            self._tx(tx)
        except sqlite3.IntegrityError as e:
            raise DuplicateBusinessKey(str(e)) from e

    def outbox(self, inv_id: int):
        r = self._db().execute("SELECT * FROM erp_outbox WHERE invoice_id=?", (inv_id,)).fetchone()
        return dict(r) if r else None

    def outbox_mark_sending(self, inv_id: int) -> None:
        self._db().execute("UPDATE erp_outbox SET status='SENDING', attempts=attempts+1 WHERE invoice_id=?", (inv_id,))

    def outbox_done(self, inv_id: int, erp_doc_id: str) -> None:
        self._db().execute("UPDATE erp_outbox SET status='DONE', erp_doc_id=? WHERE invoice_id=?", (erp_doc_id, inv_id))

    # ------------------------------------------------------------ review
    def open_review(self, inv_id: int, reasons: list[str], fields: Optional[dict]) -> None:
        self._db().execute("INSERT OR IGNORE INTO review_task(invoice_id, reasons_json, fields_json, status, created_at)"
                           " VALUES(?,?,?, 'OPEN', ?)", (inv_id, json.dumps(reasons), _dumps(fields), self.clock.now()))

    def has_open_review(self, inv_id: int) -> bool:
        return self._db().execute("SELECT 1 FROM review_task WHERE invoice_id=? AND status='OPEN'", (inv_id,)).fetchone() is not None

    def claim_review(self, worker: str, lease_s: float, now: float):
        r = self._db().execute(
            "UPDATE review_task SET lease_owner=?, lease_until=? WHERE invoice_id = ("
            " SELECT t.invoice_id FROM review_task t JOIN invoice i ON i.id = t.invoice_id"
            " WHERE t.status='OPEN' AND (t.lease_until IS NULL OR t.lease_until<?)"
            " AND (i.lease_owner IS NULL OR i.lease_until<?)"   # its graph has finished pausing
            " ORDER BY t.created_at LIMIT 1) RETURNING *", (worker, now + lease_s, now, now)).fetchone()
        return dict(r) if r else None

    def close_review(self, inv_id: int, decision: str, reviewer: str) -> None:
        self._db().execute("UPDATE review_task SET status='DONE', decision=?, reviewer=?, decided_at=?,"
                           " lease_owner=NULL WHERE invoice_id=?", (decision, reviewer, self.clock.now(), inv_id))
