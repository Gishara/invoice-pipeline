"""Simulated AP clerk. Reads the invoice (here: the ground truth for its PDF) and
decides. In production this is the review UI; the ReviewerPolicy port is the seam."""
from __future__ import annotations

import json

from app.config import DATA_DIR


class GroundTruthReviewer:
    def __init__(self):
        self.truth = {t["doc_sha256"]: t for t in json.loads((DATA_DIR / "ground_truth.json").read_text())}

    def decide(self, invoice: dict, reasons: list[str]) -> dict:
        t = self.truth.get(invoice["doc_sha256"])
        if t is None:
            return {"decision": "reject", "note": "not an invoice"}
        if t["expected_final"] == "POSTED":
            return {"decision": "approve", "fields": t["fields"]}
        if t["expected_final"] == "DUPLICATE":
            return {"decision": "duplicate"}
        return {"decision": "reject"}
