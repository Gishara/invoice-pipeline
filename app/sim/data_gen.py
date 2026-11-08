"""Dummy data: a vendor master, purchase orders, and supplier invoice PDFs with
labelled defects. Writes data/vendor_master.json, data/purchase_orders.json,
data/pdfs/*.pdf and data/ground_truth.json."""
from __future__ import annotations

import io
import json
import random
import shutil
from datetime import date, timedelta

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from app.config import DATA_DIR, Policy
from app.domain.model import norm_invoice_no, sha256

BUYER, TODAY = Policy().buyer, Policy().today

SUPPLIERS = [
    # id, name, tax id, currency, number locale, invoice-number style
    ("V001", "Acme Industrial Supplies Ltd", "GB123456789", "GBP", "en", "ACME-{y}-{n:05d}"),
    ("V002", "Brightline Logistics BV", "NL812345678B01", "EUR", "eu", "BL{n:06d}"),
    ("V003", "Ceylon Spice Traders (Pvt) Ltd", "LK114567890", "USD", "en", "CST/{y}/{n:04d}"),
    ("V004", "Delta Office GmbH", "DE298765432", "EUR", "eu", "RE-{n:07d}"),
    ("V005", "Evergreen Packaging Inc", "US941234567", "USD", "en", "EVP{n:05d}"),
    ("V006", "Fjord Components AS", "NO987654321MVA", "EUR", "eu", "FC-{y}{n:04d}"),
    ("V007", "Granite IT Services Ltd", "GB987654321", "GBP", "en", "GIT-{n:06d}"),
    ("V008", "Harbor Freight Partners LLC", "US871234560", "USD", "iso", "HFP-{y}-{n:04d}"),
    ("V009", "Iberia Textiles SL", "ESB12345678", "EUR", "eu", "IT{y}{n:05d}"),
    ("V010", "Jade Electronics Pte Ltd", "SG201912345K", "USD", "en", "JE{n:06d}"),
]
UNKNOWN_SUPPLIER = ("V999", "Quickbuy Trading FZE", "AE100200300400", "USD", "en", "QT{n:05d}")
TAX_RATE = {"GBP": 0.20, "EUR": 0.21, "USD": 0.0}

PRODUCTS = [
    ("Steel bracket 40mm", 2.35), ("Pallet wrap 500m", 18.90), ("Cardboard box A3", 0.85),
    ("Laser toner black", 64.00), ("Network switch 24p", 410.00), ("Safety gloves (pair)", 3.10),
    ("Ceylon cinnamon 1kg", 22.50), ("Freight - container leg", 1850.00), ("Consulting hour", 120.00),
    ("Office chair ergonomic", 289.00), ("LED panel 60x60", 39.95), ("Cotton fabric roll", 145.00),
    ("Hard drive 4TB", 96.00), ("Label printer roll", 7.40), ("Forklift hire (day)", 340.00),
]

SCENARIOS = [  # name, weight, expected final state
    ("clean", 0.68, "POSTED"),
    ("over_limit", 0.03, "POSTED"),          # clean but large: a person approves
    ("items_mismatch", 0.07, "REJECTED"),
    ("wrong_invoice_no", 0.06, "REJECTED"),
    ("arithmetic_error", 0.03, "REJECTED"),
    ("unknown_supplier", 0.02, "REJECTED"),
    ("bank_details_changed", 0.02, "REJECTED"),
    ("dup_resend", 0.07, "DUPLICATE"),       # same invoice, new PDF
    ("dup_conflict", 0.02, "REJECTED"),      # same number, different amount
]


def fmt_money(x: float, loc: str) -> str:
    s = f"{x:,.2f}"
    if loc == "eu":
        s = s.replace(",", "_").replace(".", ",").replace("_", ".")
    return s


def fmt_date(d: date, loc: str) -> str:
    if loc == "eu":
        return d.strftime("%d.%m.%Y")
    if loc == "iso":
        return d.isoformat()
    return d.strftime("%d %b %Y")


def iban_for(sid: str, rnd: random.Random | None = None) -> str:
    if rnd:
        return f"GB{rnd.randint(10, 99)}FRAU{rnd.randint(10**13, 10**14 - 1)}"
    base = int(sid[1:]) * 7919
    return f"NL{10 + base % 89}BANK0{base:09d}"


def render_pdf(f: dict, loc: str, address: str, footer: str) -> bytes:
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4, invariant=1)
    y = 800

    def line(text, size=10, dy=16, x=50):
        nonlocal y
        c.setFont("Helvetica-Bold" if size > 11 else "Helvetica", size)
        c.drawString(x, y, text)
        y -= dy

    line("INVOICE", 18, 28)
    line(f"Supplier: {f['supplier_name']}")
    line(f"Address: {address}")
    line(f"Tax ID: {f['supplier_tax_id']}", dy=24)
    line(f"Invoice No: {f['invoice_number']}")
    line(f"Invoice Date: {fmt_date(date.fromisoformat(f['invoice_date']), loc)}")
    line(f"PO Number: {f['po_number']}")
    line(f"Currency: {f['currency']}")
    line(f"Bill To: {BUYER}, Keizersgracht 100, Amsterdam", dy=26)
    line("LINE | SKU | Description | Qty | Unit Price | Line Total", 9)
    for i, li in enumerate(f["lines"], 1):
        line(f"LINE {i} | {li['sku']} | {li['description']} | {li['quantity']} | "
             f"{fmt_money(li['unit_price'], loc)} | {fmt_money(li['line_total'], loc)}", 9, 14)
    y -= 10
    line(f"Subtotal: {fmt_money(f['subtotal'], loc)}")
    line(f"Tax: {fmt_money(f['tax'], loc)}")
    line(f"Total Due: {fmt_money(f['total'], loc)} {f['currency']}", 12, 26)
    line(f"Pay to IBAN: {f['iban']}")
    line("Payment terms: 30 days net.", 9)
    if footer:
        line(footer, 9)
    c.showPage()
    c.save()
    return buf.getvalue()


def generate(n: int = 300, seed: int = 7) -> dict:
    rnd = random.Random(seed)
    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
    (DATA_DIR / "pdfs").mkdir(parents=True)

    vendors, pos = {}, {}
    for sid, name, tax, cur, loc, pat in SUPPLIERS:
        vendors[sid] = {"supplier_id": sid, "name": name, "tax_id": tax, "currency": cur,
                        "iban": iban_for(sid), "invoice_no_pattern": pat, "locale": loc,
                        "address": f"{rnd.randint(1, 250)} Commerce Road, Unit {rnd.randint(1, 40)}"}
    for k in range(80):
        sid = SUPPLIERS[k % len(SUPPLIERS)][0]
        po_no = f"PO-{450000 + k * 37}"
        items = rnd.sample(range(len(PRODUCTS)), rnd.randint(2, 5))
        pos[po_no] = {"po_number": po_no, "supplier_id": sid, "lines": [
            {"sku": f"SKU-{i:03d}", "description": PRODUCTS[i][0],
             "quantity": rnd.choice([10, 20, 50, 100, 200]) if PRODUCTS[i][1] < 100 else rnd.randint(2, 12),
             "unit_price": PRODUCTS[i][1]} for i in items]}
    po_by_supplier: dict[str, list] = {}
    for po in pos.values():
        po_by_supplier.setdefault(po["supplier_id"], []).append(po)

    counters = {sid: rnd.randint(100, 900) for sid in vendors}
    today = date.fromisoformat(TODAY)
    truth, originals = [], []

    def new_invoice(sid, scenario):
        v = vendors.get(sid)
        if v is None:  # unknown supplier
            _, name, tax, cur, loc, pat = UNKNOWN_SUPPLIER
            v = {"name": name, "tax_id": tax, "currency": cur, "iban": "AE070331234567890123456",
                 "invoice_no_pattern": pat, "locale": loc, "address": "Plot 12, Free Zone"}
            po = rnd.choice(list(pos.values()))
        else:
            po = rnd.choice(po_by_supplier[sid])
        counters[sid] = counters.get(sid, 500) + rnd.randint(1, 4)
        inv_no = v["invoice_no_pattern"].format(y=2026, n=counters[sid])
        lines = []
        for pl in rnd.sample(po["lines"], rnd.randint(1, len(po["lines"]))):
            qty = rnd.randint(1, pl["quantity"])
            if scenario == "over_limit":
                qty = pl["quantity"]
            lines.append({"sku": pl["sku"], "description": pl["description"],
                          "quantity": qty, "unit_price": pl["unit_price"]})
        if scenario == "over_limit":
            lines.append({"sku": "SKU-007", "description": PRODUCTS[7][0], "quantity": 30,
                          "unit_price": PRODUCTS[7][1]})
            po = {**po, "lines": po["lines"] + [{"sku": "SKU-007", "description": PRODUCTS[7][0],
                                                 "quantity": 30, "unit_price": PRODUCTS[7][1]}]}
            pos[po["po_number"]] = po
        if scenario == "items_mismatch":
            kind = rnd.choice(["price", "qty", "sku"])
            li = lines[0]
            if kind == "price":
                li["unit_price"] = round(li["unit_price"] * 1.08 + 0.01, 2)
            elif kind == "qty":
                li["quantity"] = next(p["quantity"] for p in po["lines"] if p["sku"] == li["sku"]) + 5
            else:
                extra = rnd.choice([i for i in range(len(PRODUCTS))
                                    if f"SKU-{i:03d}" not in {p['sku'] for p in po['lines']}])
                lines.append({"sku": f"SKU-{extra:03d}", "description": PRODUCTS[extra][0],
                              "quantity": 3, "unit_price": PRODUCTS[extra][1]})
        if scenario == "wrong_invoice_no":
            inv_no = rnd.choice([po["po_number"], f"{counters[sid]}", "N/A", f"ORD-{counters[sid]}"])
        for li in lines:
            li["line_total"] = round(li["quantity"] * li["unit_price"], 2)
        subtotal = round(sum(li["line_total"] for li in lines), 2)
        tax = round(subtotal * TAX_RATE[v["currency"]], 2)
        total = round(subtotal + tax, 2)
        if scenario == "arithmetic_error":
            total = round(total + rnd.choice([10, 100, 0.9, 1000]), 2)
        f = {"supplier_name": v["name"], "supplier_tax_id": v["tax_id"], "invoice_number": inv_no,
             "invoice_date": (today - timedelta(days=rnd.randint(1, 40))).isoformat(),
             "currency": v["currency"], "po_number": po["po_number"],
             "iban": iban_for("x", rnd) if scenario == "bank_details_changed" else v["iban"],
             "lines": lines, "subtotal": subtotal, "tax": tax, "total": total}
        return f, v

    weights = [w for _, w, _ in SCENARIOS]
    for i in range(n):
        scenario, _, expected = rnd.choices(SCENARIOS, weights)[0]
        if scenario.startswith("dup") and not originals:
            scenario, expected = "clean", "POSTED"
        footer = ""
        if scenario in ("dup_resend", "dup_conflict"):
            orig = rnd.choice(originals)
            f = json.loads(json.dumps(orig["fields"]))
            v = vendors[orig["supplier_id"]]
            sid = orig["supplier_id"]
            footer = rnd.choice(["COPY - resent at supplier request", "Reminder: payment overdue",
                                 "Duplicate copy"])
            if scenario == "dup_conflict":
                f["lines"][0]["quantity"] += 1
                li = f["lines"][0]
                li["line_total"] = round(li["quantity"] * li["unit_price"], 2)
                f["subtotal"] = round(sum(x["line_total"] for x in f["lines"]), 2)
                f["tax"] = round(f["subtotal"] * TAX_RATE[f["currency"]], 2)
                f["total"] = round(f["subtotal"] + f["tax"], 2)
        else:
            sid = "V999" if scenario == "unknown_supplier" else rnd.choice(list(vendors))
            f, v = new_invoice(sid, scenario)
        pdf = render_pdf(f, v["locale"], v["address"], footer)
        doc_sha = sha256(pdf)
        (DATA_DIR / "pdfs" / f"{i:05d}.pdf").write_bytes(pdf)
        rec = {"seq": i, "file": f"{i:05d}.pdf", "doc_sha256": doc_sha, "scenario": scenario,
               "expected_final": expected, "supplier_id": sid if sid in vendors else None,
               "business_key": f"{sid}|{norm_invoice_no(f['invoice_number'])}", "fields": f}
        truth.append(rec)
        if scenario == "clean":
            originals.append(rec)

    (DATA_DIR / "vendor_master.json").write_text(json.dumps(vendors, indent=1))
    (DATA_DIR / "purchase_orders.json").write_text(json.dumps(pos, indent=1))
    (DATA_DIR / "ground_truth.json").write_text(json.dumps(truth, indent=1))
    counts: dict[str, int] = {}
    for t in truth:
        counts[t["scenario"]] = counts.get(t["scenario"], 0) + 1
    return {"invoices": n, "scenarios": counts}
