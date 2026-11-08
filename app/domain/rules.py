"""Business rules. Each rule is a small class with one job; the validator runs
whatever list it is given. Adding a rule means adding a class to default_rules(),
not editing the validator (open/closed). Rules are pure: they read the extracted
fields and reference data, and return reasons. No rule writes anything."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Protocol

from app.config import Policy
from app.ports import ReferenceData


@dataclass
class RuleContext:
    fields: dict
    vendor: Optional[dict]
    ref: ReferenceData
    policy: Policy


class Rule(Protocol):
    name: str

    def check(self, ctx: RuleContext) -> list[str]: ...


def pattern_regex(pat: str) -> re.Pattern:
    out, i = "", 0
    for m in re.finditer(r"\{(\w)(?::0(\d)d)?\}", pat):
        out += re.escape(pat[i:m.start()])
        out += r"\d{4}" if m.group(1) == "y" else (rf"\d{{{m.group(2)}}}" if m.group(2) else r"\d+")
        i = m.end()
    return re.compile("^" + out + re.escape(pat[i:]) + "$")


def match_supplier(f: dict, ref: ReferenceData) -> tuple[Optional[dict], list[str]]:
    v = ref.vendor_by_tax_id(f.get("supplier_tax_id") or "")
    if v:
        same = v["name"].lower() == (f.get("supplier_name") or "").lower()
        return v, [] if same else ["supplier_name_differs_from_vendor_master"]
    v = ref.vendor_by_name(f.get("supplier_name") or "")
    return (v, ["supplier_tax_id_mismatch"]) if v else (None, ["unknown_supplier"])


class InvoiceNumberRule:
    name = "invoice_number"

    def check(self, ctx):
        no, f = ctx.fields.get("invoice_number"), ctx.fields
        if not no:
            return ["invoice_number_missing"]
        if no == f.get("po_number") or ctx.ref.is_po_number(no) or no.upper().startswith(("PO-", "ORD-")):
            return ["invoice_number_is_an_order_number"]
        if ctx.vendor and not pattern_regex(ctx.vendor["invoice_no_pattern"]).match(no):
            return [f"invoice_number_format_unexpected_for_supplier ({no})"]
        return []


class ArithmeticRule:
    name = "arithmetic"

    def check(self, ctx):
        f, tol, r = ctx.fields, ctx.policy.money_tolerance, []
        for i, li in enumerate(f["lines"], 1):
            if abs(li["quantity"] * li["unit_price"] - li["line_total"]) > tol:
                r.append(f"line_{i}_qty_x_price_ne_total")
        if abs(sum(li["line_total"] for li in f["lines"]) - f["subtotal"]) > tol * max(1, len(f["lines"])):
            r.append("lines_do_not_sum_to_subtotal")
        if abs(f["subtotal"] + f["tax"] - f["total"]) > tol:
            r.append("subtotal_plus_tax_ne_total")
        return r


class ItemsMatchPoRule:
    name = "items_match_po"

    def check(self, ctx):
        po = ctx.ref.purchase_order(ctx.fields.get("po_number") or "")
        if not po:
            return ["po_not_found"]
        if ctx.vendor and po["supplier_id"] != ctx.vendor["supplier_id"]:
            return ["po_belongs_to_another_supplier"]
        by_sku, r = {l["sku"]: l for l in po["lines"]}, []
        for li in ctx.fields["lines"]:
            pl = by_sku.get(li["sku"])
            if not pl:
                r.append(f"items_mismatch: {li['sku']} not on {po['po_number']}")
            elif li["quantity"] > pl["quantity"]:
                r.append(f"items_mismatch: {li['sku']} qty {li['quantity']:g} > PO {pl['quantity']}")
            elif abs(li["unit_price"] - pl["unit_price"]) > ctx.policy.price_tolerance * pl["unit_price"]:
                r.append(f"items_mismatch: {li['sku']} price {li['unit_price']} vs PO {pl['unit_price']}")
        return r


class CurrencyRule:
    name = "currency"

    def check(self, ctx):
        v, cur = ctx.vendor, ctx.fields["currency"]
        return [f"currency_mismatch: {cur} vs supplier {v['currency']}"] if v and cur != v["currency"] else []


class DateRule:
    name = "date"

    def check(self, ctx):
        try:
            d = date.fromisoformat(ctx.fields["invoice_date"])
        except ValueError:
            return ["date_unparseable"]
        today = date.fromisoformat(ctx.policy.today)
        ok = today - timedelta(days=ctx.policy.max_invoice_age_days) <= d <= today
        return [] if ok else ["date_out_of_range"]


class BankDetailsRule:
    name = "bank_details"

    def check(self, ctx):
        v, iban = ctx.vendor, ctx.fields.get("iban")
        return ["bank_details_differ_from_vendor_master (possible fraud)"] if v and iban and iban != v["iban"] else []


class AmountLimitRule:
    name = "amount_limit"

    def check(self, ctx):
        t, lim = ctx.fields["total"], ctx.policy.auto_post_limit
        return [f"above_auto_post_limit ({t:.2f} > {lim:.0f})"] if t > lim else []


def default_rules() -> list[Rule]:
    return [InvoiceNumberRule(), ArithmeticRule(), ItemsMatchPoRule(), CurrencyRule(), DateRule(),
            BankDetailsRule(), AmountLimitRule()]
