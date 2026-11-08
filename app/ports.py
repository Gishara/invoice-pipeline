"""Ports: the interfaces the application depends on. Adapters implement them.

Services and agents import only from here and from app.domain, never from a
concrete adapter, so Postgres, Kafka, a real ERP or another model provider can
be swapped in without touching business logic (dependency inversion)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Optional, Protocol, runtime_checkable


# ---------------------------------------------------------------- messaging
@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: str
    value: bytes


class MessageConsumer(Protocol):
    def poll(self, max_records: int) -> list[Record]: ...
    def commit(self, rec: Record) -> None: ...
    def restart(self) -> None: ...


# ---------------------------------------------------------------- storage
class ObjectStore(Protocol):
    def put(self, data: bytes) -> str: ...
    def get(self, ref: str) -> bytes: ...


class Conflict(Exception):
    """A conditional update matched nothing: another worker moved the row first."""


class DuplicateBusinessKey(Exception):
    """The database refused a second posting of the same supplier + invoice number."""


class InvoiceRepository(Protocol):
    # ingest
    def insert_received(self, doc_sha: str, pdf_ref: str, rec: Record) -> bool: ...
    def dead_letter(self, rec: Record, error: str) -> None: ...
    # reads
    def get(self, inv_id: int) -> dict: ...
    def same_business_key(self, supplier_id: str, inv_no_norm: str, exclude_id: int) -> list[dict]: ...
    def earlier_from_same_sender(self, inv_id: int) -> list[dict]: ...
    def count_open(self) -> int: ...
    # work distribution
    def claim(self, states: Iterable[str], worker: str, lease_s: float, now: float) -> Optional[dict]: ...
    def release(self, inv_id: int, worker: str) -> None: ...
    # state machine
    def transition(self, inv_id: int, from_states: set[str], to: str, actor: str, reason: str = "",
                   **fields: Any) -> dict: ...
    def set_graph_thread(self, inv_id: int, thread: str) -> None: ...
    def park(self, inv_id: int, until: float) -> None: ...
    # extraction cache
    def cached_extraction(self, doc_sha: str, model: str, prompt_version: str) -> Optional[dict]: ...
    def cache_extraction(self, doc_sha: str, model: str, prompt_version: str, raw: str,
                         tokens_in: int, tokens_out: int) -> None: ...
    # outbox
    def begin_posting(self, inv_id: int, from_states: set[str], key: str, payload: dict) -> None: ...
    def outbox(self, inv_id: int) -> Optional[dict]: ...
    def outbox_mark_sending(self, inv_id: int) -> None: ...
    def outbox_done(self, inv_id: int, erp_doc_id: str) -> None: ...
    # review
    def open_review(self, inv_id: int, reasons: list[str], fields: Optional[dict]) -> None: ...
    def claim_review(self, worker: str, lease_s: float, now: float) -> Optional[dict]: ...
    def close_review(self, inv_id: int, decision: str, reviewer: str) -> None: ...
    def has_open_review(self, inv_id: int) -> bool: ...


# ---------------------------------------------------------------- reference data
class ReferenceData(Protocol):
    def vendor_by_tax_id(self, tax_id: str) -> Optional[dict]: ...
    def vendor_by_name(self, name: str) -> Optional[dict]: ...
    def vendor(self, supplier_id: str) -> Optional[dict]: ...
    def purchase_order(self, po_number: str) -> Optional[dict]: ...
    def is_po_number(self, s: str) -> bool: ...


# ---------------------------------------------------------------- model
@dataclass
class Completion:
    raw: str
    model: str
    tokens_in: int
    tokens_out: int
    latency_s: float
    stop_reason: str = "end_turn"
    cached_tokens: int = 0


class ProviderError(Exception):
    pass


class ProviderTimeout(ProviderError):
    pass


class RateLimited(ProviderError):
    def __init__(self, retry_after: float):
        super().__init__(f"429 retry after {retry_after}s")
        self.retry_after = retry_after


class ServerError(ProviderError):
    pass


class ProviderUnavailable(Exception):
    """The gateway gave up for now: retries exhausted or every circuit open."""


@runtime_checkable
class ModelClient(Protocol):
    name: str

    def complete(self, system: str, prompt: str, document_text: str, attempt_key: str) -> Completion: ...


class TextModel(Protocol):
    """What the extraction service needs: something that turns a prompt into a completion."""

    def complete(self, system: str, prompt: str, document_text: str, attempt_key: str) -> Completion: ...


# ---------------------------------------------------------------- ERP
class ErpTimeout(Exception):
    pass


class ErpError(Exception):
    pass


class ErpClient(Protocol):
    def post(self, external_ref: str, payload: dict) -> str: ...
    def lookup(self, external_ref: str) -> Optional[str]: ...


# ---------------------------------------------------------------- people
class ReviewerPolicy(Protocol):
    def decide(self, invoice: dict, reasons: list[str]) -> dict: ...


class CrashInjector(Protocol):
    def maybe_crash(self, point: str) -> None: ...
