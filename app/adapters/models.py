"""Model clients and the registry that builds them from models.json.

Add a provider by adding a class with the ModelClient shape and one line in
build_model(); nothing else changes (open/closed)."""
from __future__ import annotations

import itertools
import json
import random
import re
import time
from datetime import datetime

from app.config import ROOT
from app.ports import Completion, ProviderTimeout, RateLimited, ServerError


def load_registry() -> dict:
    reg = json.loads((ROOT / "models.json").read_text())
    reg.pop("_comment", None)
    return reg


# ---------------------------------------------------------------- simulated
def _num(s: str) -> float:
    s = s.strip()
    if re.search(r",\d{2}$", s):
        s = s.replace(".", "").replace(",", ".")
    return float(s.replace(",", ""))


def _date(s: str) -> str:
    for f in ("%d %b %Y", "%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), f).date().isoformat()
        except ValueError:
            pass
    return s.strip()


def reference_read(text: str) -> dict:
    """What a perfect model returns for the dummy layout. Simulated models start here
    and then make mistakes at their configured rates."""
    g = lambda pat: (m.group(1).strip() if (m := re.search(pat, text)) else None)
    lines = [{"sku": m.group(1), "description": m.group(2), "quantity": float(m.group(3)),
              "unit_price": _num(m.group(4)), "line_total": _num(m.group(5))}
             for m in re.finditer(r"LINE \d+ \| (\S+) \| (.+?) \| ([\d.]+) \| ([\d.,]+) \| ([\d.,]+)", text)]
    inv = g(r"Invoice No: (.*)")
    return {"supplier_name": g(r"Supplier: (.*)"), "supplier_tax_id": g(r"Tax ID: (.*)"),
            "invoice_number": None if inv in (None, "N/A", "") else inv,
            "invoice_date": _date(g(r"Invoice Date: (.*)") or ""), "currency": g(r"Currency: (\w{3})"),
            "po_number": g(r"PO Number: (.*)"), "iban": g(r"IBAN: (\S+)"), "lines": lines,
            "subtotal": _num(g(r"Subtotal: ([\d.,]+)") or "0"), "tax": _num(g(r"Tax: ([\d.,]+)") or "0"),
            "total": _num(g(r"Total Due: ([\d.,]+)") or "0")}


class SimulatedModel:
    """Behaves like a provider: latency, timeouts, 429s, 5xx, prompt caching of the
    static prefix, and extraction mistakes at the rates in models.json."""

    def __init__(self, name: str, spec: dict, clock, seed: int):
        self.name, self.spec, self.clock, self.seed = name, spec, clock, seed
        self.n = spec["noise"]
        self._calls = itertools.count()
        self._prefix_cached = False

    def complete(self, system: str, prompt: str, document_text: str, attempt_key: str) -> Completion:
        call = next(self._calls)
        r = random.Random(f"{self.seed}|{self.name}|{attempt_key}|{call}")
        n, lat_mean = self.n, self.spec["latency_s"]
        prefix_tokens = len(system) // 4
        tin = prefix_tokens + len(prompt) // 4 + 400
        cached = prefix_tokens if self._prefix_cached else 0
        x = r.random()
        if x < n["timeout"]:
            self.clock.sleep(30)  # our client-side timeout
            raise ProviderTimeout("request timed out after 30s")
        x -= n["timeout"]
        if x < n["rate_limit"]:
            self.clock.sleep(0.2)
            raise RateLimited(retry_after=r.choice([2, 5, 10, 30]))
        x -= n["rate_limit"]
        if x < n["server_error"]:
            self.clock.sleep(1)
            raise ServerError("529 overloaded")
        lat = max(0.5, r.gauss(lat_mean, lat_mean / 3))
        self.clock.sleep(lat)
        self._prefix_cached = True
        d = reference_read(document_text)
        if r.random() < n["field_error"]:
            kind = r.choice(["total", "date", "tax_id", "qty", "currency", "subtotal"])
            if kind == "total":
                d["total"] = round(d["total"] * r.choice([10, 0.1]) if r.random() < .5 else d["total"] + 9, 2)
            elif kind == "date":
                y, m, dd = d["invoice_date"].split("-")
                d["invoice_date"] = f"{y}-{dd}-{m}" if int(dd) <= 12 else f"{int(y) - 1}-{m}-{dd}"
            elif kind == "tax_id" and d["supplier_tax_id"]:
                t = list(d["supplier_tax_id"]); i = r.randrange(2, len(t))
                t[i] = str((int(t[i]) + 1) % 10) if t[i].isdigit() else "X"
                d["supplier_tax_id"] = "".join(t)
            elif kind == "qty" and d["lines"]:
                d["lines"][0]["quantity"] += 1
            elif kind == "currency":
                d["currency"] = {"EUR": "USD", "USD": "EUR", "GBP": "EUR"}.get(d["currency"], "USD")
            else:
                d["subtotal"] = round(d["subtotal"] + 1, 2)
        if r.random() < n["invoice_no_from_po"]:
            d["invoice_number"] = d["po_number"]
        if r.random() < n["drop_line"] and len(d["lines"]) > 1:
            d["lines"].pop()
        raw, stop = json.dumps(d), "end_turn"
        if r.random() < n["bad_json"]:
            raw = r.choice([f"Here is the extracted invoice:\n{raw}", raw.replace('"total"', "total"),
                            raw[:-1] + ",}", json.dumps({**d, "lines": "see attached"})])
        elif r.random() < n["truncated"]:
            raw, stop = raw[: int(len(raw) * 0.6)], "max_tokens"
        return Completion(raw, self.name, tin, len(raw) // 4 + 20, lat, stop, cached)


# ---------------------------------------------------------------- real providers
class LangChainModel:
    """A real chat model through LangChain. The system prompt (instructions + schema)
    is marked for provider prompt caching, so repeated calls pay for it once."""

    def __init__(self, name: str, spec: dict):
        self.name, self.spec = name, spec
        if spec["provider"] == "anthropic":
            from langchain_anthropic import ChatAnthropic
            self.llm = ChatAnthropic(model=spec["model_id"], max_tokens=4096, timeout=90, max_retries=0)
        elif spec["provider"] == "openai":
            from langchain_openai import ChatOpenAI  # OpenAI caches long prefixes automatically
            self.llm = ChatOpenAI(model=spec["model_id"], timeout=90, max_retries=0,
                                  model_kwargs={"response_format": {"type": "json_object"}})
        else:
            raise ValueError(spec["provider"])

    def complete(self, system: str, prompt: str, document_text: str, attempt_key: str) -> Completion:
        from langchain_core.messages import HumanMessage, SystemMessage
        sys_msg = (SystemMessage(content=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}])
                   if self.spec["provider"] == "anthropic" else SystemMessage(system))
        t0 = time.time()
        try:
            msg = self.llm.invoke([sys_msg, HumanMessage(prompt)])
        except Exception as e:
            s = f"{type(e).__name__} {e}".lower()
            if "rate" in s or "429" in s:
                raise RateLimited(retry_after=10) from e
            if "timeout" in s:
                raise ProviderTimeout(str(e)) from e
            if any(c in s for c in ("500", "502", "503", "529", "overload")):
                raise ServerError(str(e)) from e
            raise
        u = getattr(msg, "usage_metadata", None) or {}
        meta = msg.response_metadata or {}
        stop = meta.get("stop_reason") or meta.get("finish_reason", "")
        cached = (u.get("input_token_details") or {}).get("cache_read", 0)
        text = msg.content if isinstance(msg.content, str) else json.dumps(msg.content)
        return Completion(text, self.name, u.get("input_tokens", 0), u.get("output_tokens", 0), time.time() - t0,
                          "max_tokens" if stop in ("max_tokens", "length") else "end_turn", cached)


def build_model(name: str, clock, seed: int = 1):
    reg = load_registry()
    if name not in reg:
        raise SystemExit(f"unknown model {name!r}; choose from {', '.join(reg)}")
    spec = reg[name]
    if spec["provider"] == "simulated":
        return SimulatedModel(name, spec, clock, seed)
    return LangChainModel(name, spec)
