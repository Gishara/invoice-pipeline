"""Domain types: invoice states, the extraction schema, and identity helpers.
No I/O here; everything else depends on this module, it depends on nothing."""
from __future__ import annotations

import hashlib
import re
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class S(str, Enum):
    RECEIVED = "RECEIVED"
    EXTRACTING = "EXTRACTING"
    EXTRACTED = "EXTRACTED"
    VALIDATED = "VALIDATED"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    APPROVED = "APPROVED"
    POSTING = "POSTING"
    POST_UNKNOWN = "POST_UNKNOWN"
    POSTED = "POSTED"
    REJECTED = "REJECTED"
    DUPLICATE = "DUPLICATE"


TERMINAL = {S.POSTED.value, S.REJECTED.value, S.DUPLICATE.value}
BEFORE_VALIDATION = {S.RECEIVED.value, S.EXTRACTING.value, S.EXTRACTED.value}

# Which pool owns which states. A pool only ever claims rows in its own states.
STAGE_STATES = {
    "extraction": (S.RECEIVED.value, S.EXTRACTING.value, S.EXTRACTED.value),
    "review": (S.NEEDS_REVIEW.value,),
    "posting": (S.VALIDATED.value, S.APPROVED.value, S.POSTING.value, S.POST_UNKNOWN.value),
}


class LineItem(BaseModel):
    sku: str
    description: str
    quantity: float
    unit_price: float
    line_total: float


class InvoiceExtraction(BaseModel):
    supplier_name: str
    supplier_tax_id: Optional[str] = None
    invoice_number: Optional[str] = None
    invoice_date: str = Field(description="ISO date YYYY-MM-DD")
    currency: str = Field(min_length=3, max_length=3)
    po_number: Optional[str] = None
    iban: Optional[str] = None
    lines: list[LineItem]
    subtotal: float
    tax: float
    total: float


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def norm_invoice_no(s: str | None) -> str:
    """INV-00123, inv 123 and INV/0123 all normalise to INV123."""
    if not s:
        return ""
    s = re.sub(r"[^A-Za-z0-9]", "", s.upper())
    m = re.match(r"^([A-Z]*)(0*)(\d+)([A-Z0-9]*)$", s)
    return f"{m.group(1)}{int(m.group(3))}{m.group(4)}" if m else s


def idempotency_key(supplier_id: str, invoice_no_norm: str) -> str:
    return sha256(f"{supplier_id}|{invoice_no_norm}".encode())[:32]
