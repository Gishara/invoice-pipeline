"""Mock ERP with its own database. Every created document is a payment obligation.

Failure modes: an error before anything happened, and a timeout AFTER the post
landed (we never see the response). By default it does not dedupe on external
reference, the worst case, so the poster must look a reference up before retrying."""
from __future__ import annotations

import random
import sqlite3
import threading

from app.ports import ErpError, ErpTimeout


class MockErp:
    def __init__(self, path: str, chaos, seed: int = 11):
        self.db = sqlite3.connect(path, isolation_level=None, check_same_thread=False, timeout=60)
        self.db.execute("""CREATE TABLE IF NOT EXISTS ap_invoice(
            erp_doc_id INTEGER PRIMARY KEY AUTOINCREMENT, external_ref TEXT, supplier_id TEXT,
            invoice_no TEXT, invoice_date TEXT, currency TEXT, total REAL)""")
        self.chaos, self.rnd = chaos, random.Random(seed)
        self._lock = threading.Lock()  # one external system; calls are serialised on its side
        self.calls = self.timeouts = self.errors = self.lookups = 0

    def post(self, external_ref: str, payload: dict) -> str:
        with self._lock:
            self.calls += 1
            if self.rnd.random() < self.chaos.erp_error_before_commit:
                self.errors += 1
                raise ErpError("503 Service Unavailable")
            if self.chaos.erp_idempotent and (hit := self._lookup(external_ref)):
                return hit
            cur = self.db.execute(
                "INSERT INTO ap_invoice(external_ref, supplier_id, invoice_no, invoice_date, currency, total)"
                " VALUES(?,?,?,?,?,?)", (external_ref, payload["supplier_id"], payload["invoice_number"],
                                         payload["invoice_date"], payload["currency"], payload["total"]))
            doc = f"ERP-{cur.lastrowid:07d}"
            if self.rnd.random() < self.chaos.erp_timeout_after_commit:
                self.timeouts += 1
                raise ErpTimeout("read timed out after 30s")  # it IS posted
            return doc

    def lookup(self, external_ref: str):
        with self._lock:
            self.lookups += 1
            return self._lookup(external_ref)

    def _lookup(self, ref):
        r = self.db.execute("SELECT erp_doc_id FROM ap_invoice WHERE external_ref=? ORDER BY erp_doc_id LIMIT 1",
                            (ref,)).fetchone()
        return f"ERP-{r[0]:07d}" if r else None

    def stats(self) -> dict:
        return {"calls": self.calls, "timeouts_after_commit": self.timeouts,
                "errors_before_commit": self.errors, "lookups": self.lookups}
