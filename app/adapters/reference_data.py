"""Vendor master and purchase orders, read through a TTL cache.

In production these come from the ERP or an MDM service, and every invoice reads
them several times. They change rarely, so a short TTL (minutes) cuts that load
to almost nothing while a bank-detail change still takes effect quickly."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from app.services.cache import TTLCache


class JsonReferenceSource:
    """The 'remote' system of record. Counts reads so the cache's effect is visible."""

    def __init__(self, data_dir: Path):
        self.vendors = json.loads((data_dir / "vendor_master.json").read_text())
        self.pos = json.loads((data_dir / "purchase_orders.json").read_text())
        self.reads = 0

    def load_vendors(self) -> dict:
        self.reads += 1
        return self.vendors

    def load_po(self, po: str) -> Optional[dict]:
        self.reads += 1
        return self.pos.get(po)

    def load_po_numbers(self) -> set:
        self.reads += 1
        return set(self.pos)


class CachedReferenceData:
    def __init__(self, source: JsonReferenceSource, ttl_s: float = 300):
        self.src, self.cache = source, TTLCache(ttl_s)

    def _vendors(self) -> dict:
        def build():
            v = self.src.load_vendors()
            return {"by_id": v, "by_tax": {x["tax_id"]: x for x in v.values()},
                    "by_name": {x["name"].lower(): x for x in v.values()}}
        return self.cache.get_or_load("vendors", build)

    def vendor(self, supplier_id: str):
        return self._vendors()["by_id"].get(supplier_id)

    def vendor_by_tax_id(self, tax_id: str):
        return self._vendors()["by_tax"].get(tax_id or "")

    def vendor_by_name(self, name: str):
        return self._vendors()["by_name"].get((name or "").lower())

    def purchase_order(self, po_number: str):
        return self.cache.get_or_load(("po", po_number), lambda: self.src.load_po(po_number))

    def is_po_number(self, s: str) -> bool:
        return s in self.cache.get_or_load("po_numbers", self.src.load_po_numbers)

    def stats(self) -> dict:
        return {**self.cache.stats(), "source_reads": self.src.reads}
