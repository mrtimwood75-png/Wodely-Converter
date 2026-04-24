from __future__ import annotations

import os
import re
import json
from collections import defaultdict
from datetime import date, datetime, time
from html import escape
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import quote
import xml.etree.ElementTree as ET

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Delivery to Wodely", layout="wide")

APP_VERSION = "2026-04-24-v33-two-step-wodely-task-list"

OUTPUT_COLUMNS = [
    "COD (money)",
    "Service Time",
    "Merchant",
    "Source",
    "OrderID",
    "Customer Account",
    "Recipient Name",
    "Phone",
    "Email",
    "Address",
    "Delivery Date",
    "Delivery Window",
    "Sales Person",
    "SKU",
    "Description",
    "Qty",
    "Notes",
]

EXCLUDED_TRANSFORMA_SKUS = {"ZHEADING", "ZDELIVERY", "ZDISCOUNT", "ZDESIGNREBATE"}
EXCLUDED_BOCONCEPT_SKUS = {"99912", "99920"}

STYLE = """
<style>
    #MainMenu, footer, header {visibility: hidden;}
    .stApp {background: #f5f2ec;}
    .block-container {max-width: 1500px; padding-top: 1.2rem; padding-bottom: 2rem;}
    .panel {background: white; border: 1px solid #e3ddd3; border-radius: 18px; padding: 18px;}
    .muted {color: #666; font-size: 0.92rem;}
</style>
"""


# -----------------------
# generic helpers
# -----------------------
def get_setting(name: str, default: str = "") -> str:
    env_value = os.getenv(name)
    if env_value is not None:
        return str(env_value).strip()

    try:
        return str(st.secrets.get(name, default)).strip()
    except Exception:
        return str(default).strip()


def get_sent_orders_file() -> Path:
    configured = get_setting("WODELY_SENT_ORDERS_FILE")
    if configured:
        return Path(configured)
    return Path("wodely_sent_orders.csv")


def load_sent_order_ids() -> set[str]:
    path = get_sent_orders_file()
    if not path.exists():
        return set()

    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return set()

    if "OrderID" not in df.columns:
        return set()

    return set(df["OrderID"].astype(str).str.strip().str.lower())


def record_sent_order(order_id: str, source: str, recipient_name: str, response_body: Any) -> None:
    path = get_sent_orders_file()
    row = pd.DataFrame([{
        "OrderID": clean(order_id),
        "Source": clean(source),
        "Recipient Name": clean(recipient_name),
        "PushedAt": datetime.now().isoformat(timespec="seconds"),
        "WodelyResponse": json.dumps(response_body, default=str),
    }])

    if path.exists():
        try:
            existing = pd.read_csv(path, dtype=str).fillna("")
            combined = pd.concat([existing, row], ignore_index=True)
        except Exception:
            combined = row
    else:
        combined = row

    combined["_key"] = combined["OrderID"].astype(str).str.strip().str.lower()
    combined = combined.drop_duplicates(subset=["_key"], keep="last").drop(columns=["_key"])
    combined.to_csv(path, index=False)


def clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return " ".join(str(value).replace("\xa0", " ").split()).strip(" ,\t\r\n")


def clean_multiline(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    lines = [clean(line) for line in text.split("\n")]
    return "\n".join([line for line in lines if line])


def first_non_blank(values: list[Any]) -> str:
    for value in values:
        text = clean(value)
        if text:
            return text
    return ""


def to_float(value: Any) -> float:
    text = clean(value)
    if not text:
        return 0.0
    text = text.replace(" ", "")
    if "," in text and "." in text:
        text = text.replace(",", "")
    else:
        text = text.replace(".", "").replace(",", ".") if re.search(r"\d+,\d+$", text) else text.replace(",", "")
    try:
        return float(text)
    except Exception:
        return 0.0


def to_int(value: Any) -> int:
    return int(round(to_float(value)))


def first_money_field(record: dict[str, Any], field_names: list[str]) -> tuple[str, float] | tuple[str, None]:
    for field_name in field_names:
        if field_name in record and clean(record.get(field_name)) != "":
            return field_name, to_float(record.get(field_name))
    return "", None


def calculate_order_total_from_lines(lines: list[dict[str, Any]]) -> float:
    total = 0.0

    for line in lines:
        qty = to_float(line.get("QTYORD"))
        price_ex_gst = to_float(line.get("PRICE"))
        gst_each = to_float(line.get("GSTDOLL"))
        total += qty * (price_ex_gst + gst_each)

    return round(total, 2)


def calculate_cod_amount(header: dict[str, Any], lines: list[dict[str, Any]], payments_total: float = 0.0) -> float:
    # Options API documentation says sales order line total inc GST is:
    # QTYORD * (PRICE + GSTDOLL)
    # Payments are stored in DRTRAN / DRHIST as TYPE = "PA" with SALESORDER = order number.
    line_total = calculate_order_total_from_lines(lines)
    header_total = to_float(header.get("TOTAMOUNT"))

    order_total = line_total if line_total > 0 else header_total
    return max(round(order_total - payments_total, 2), 0.0)


def join_non_blank(parts: list[Any], sep: str = ", ") -> str:
    vals = [clean(p) for p in parts if clean(p)]
    return sep.join(vals)


def parse_date_text(value: Any) -> datetime | None:
    text = clean(value)
    if not text:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return pd.to_datetime(text, dayfirst=True).to_pydatetime()
    except Exception:
        return None


def format_display_date(value: Any) -> str:
    dt = parse_date_text(value)
    return dt.strftime("%d/%m/%Y") if dt else clean(value)


def build_after_before_datetime(delivery_date: Any, delivery_window: Any) -> tuple[str | None, str | None]:
    dt = parse_date_text(delivery_date)
    if dt is None:
        return None, None

    window = clean(delivery_window).lower()
    base_date = dt.date()

    if not window:
        return (
            datetime.combine(base_date, time(0, 0)).isoformat(),
            datetime.combine(base_date, time(23, 59)).isoformat(),
        )

    if "-" in window:
        left, right = [part.strip() for part in window.split("-", 1)]

        def parse_clock(part: str) -> time | None:
            candidates = [part, part.upper().replace(".", ""), part.upper().replace(" ", "")]
            for candidate in candidates:
                for fmt in ("%H:%M", "%H%M", "%I:%M%p", "%I%p", "%I:%M %p", "%I %p"):
                    try:
                        return datetime.strptime(candidate, fmt).time()
                    except ValueError:
                        continue
            return None

        start_time = parse_clock(left)
        end_time = parse_clock(right)
        if start_time and end_time:
            return (
                datetime.combine(base_date, start_time).isoformat(),
                datetime.combine(base_date, end_time).isoformat(),
            )

    if "am" in window or "morning" in window:
        return (
            datetime.combine(base_date, time(8, 0)).isoformat(),
            datetime.combine(base_date, time(12, 0)).isoformat(),
        )

    if "pm" in window or "afternoon" in window:
        return (
            datetime.combine(base_date, time(12, 0)).isoformat(),
            datetime.combine(base_date, time(17, 0)).isoformat(),
        )

    return (
        datetime.combine(base_date, time(0, 0)).isoformat(),
        datetime.combine(base_date, time(23, 59)).isoformat(),
    )


def empty_preview_df() -> pd.DataFrame:
    return pd.DataFrame(columns=OUTPUT_COLUMNS)


def normalize_preview_schema(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    alias_groups = {
        "OrderID": ["Order ID", "Order Id", "Order No", "Order Number", "Sales Order", "Sales order", "ORDNO", "order_no"],
        "Customer Account": ["CustomerAccount", "Customer Code", "Account", "ACCDE"],
        "Recipient Name": ["Recipient", "RecipientName", "Customer", "Customer Name", "DELNAME", "CUSTNAME"],
        "Phone": ["Mobile", "Mobile phone", "Telephone", "MOBILE"],
        "Email": ["EMAIL", "E-mail"],
        "Address": ["Delivery Address", "DELADDRESS", "Full Address"],
        "Delivery Date": ["BOOKOUT", "Tour date", "Tour Date"],
        "Delivery Window": ["DELIVTIME", "Delivery Time"],
        "Sales Person": ["SalesPerson", "AREA"],
        "SKU": ["Stock Code", "StockCode", "STOCKCODE", "Item number", "Item Code"],
        "Description": ["DESC", "MEMODESC", "OVERDESC", "Item name"],
        "Qty": ["Quantity", "QTYORD"],
        "Notes": ["Note", "DELNOTE"],
    }
    stripped_map = {str(c).strip(): c for c in df.columns}
    rename_map: dict[Any, str] = {}
    for canonical, aliases in alias_groups.items():
        if canonical in df.columns:
            continue
        for alias in aliases:
            if alias in df.columns:
                rename_map[alias] = canonical
                break
            if alias in stripped_map:
                rename_map[stripped_map[alias]] = canonical
                break
    if rename_map:
        df = df.rename(columns=rename_map)
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df.columns = [str(c).strip() for c in df.columns]
    return df


def ensure_order_id(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_preview_schema(df)
    if "OrderID" not in df.columns:
        df["OrderID"] = ""
    df["OrderID"] = df["OrderID"].fillna("").astype(str).str.strip()
    return df


def prepare_preview_df(df: pd.DataFrame) -> pd.DataFrame:
    df = ensure_order_id(df.copy())
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df["COD (money)"] = pd.to_numeric(df["COD (money)"], errors="coerce").fillna(0.0)
    df["Service Time"] = pd.to_numeric(df["Service Time"], errors="coerce").fillna(0).astype(int)
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0).astype(int)
    for col in [c for c in OUTPUT_COLUMNS if c not in {"COD (money)", "Service Time", "Qty"}]:
        df[col] = df[col].fillna("").astype(str)
    df = df[OUTPUT_COLUMNS].copy()
    return df.reset_index(drop=True)


def append_preview(existing_df: pd.DataFrame | None, new_df: pd.DataFrame) -> pd.DataFrame:
    new_df = prepare_preview_df(new_df)

    if existing_df is None or existing_df.empty:
        combined = new_df.reset_index(drop=True)
    else:
        combined = pd.concat([existing_df, new_df], ignore_index=True)

    combined = prepare_preview_df(combined)

    # Prevent duplicate preview rows when the same source is added more than once.
    # Keep the first version so any manual edits already made in the preview are preserved.
    dedupe_cols = ["Source", "OrderID", "SKU", "Description", "Qty"]
    for col in dedupe_cols:
        if col not in combined.columns:
            combined[col] = ""

    combined["_dedupe_key"] = (
        combined["Source"].fillna("").astype(str).str.strip().str.lower()
        + "|"
        + combined["OrderID"].fillna("").astype(str).str.strip().str.lower()
        + "|"
        + combined["SKU"].fillna("").astype(str).str.strip().str.lower()
        + "|"
        + combined["Description"].fillna("").astype(str).str.strip().str.lower()
        + "|"
        + combined["Qty"].fillna("").astype(str).str.strip()
    )

    combined = combined.drop_duplicates(subset=["_dedupe_key"], keep="first").drop(columns=["_dedupe_key"])
    return prepare_preview_df(combined)


# -----------------------
# BoConcept TXT parser
# -----------------------
def bc_looks_like_order_id(value: str) -> bool:
    txt = clean(value)
    if not txt or len(txt) > 40 or " " in txt or not any(ch.isdigit() for ch in txt):
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9\-/]*", txt))



def bc_split_blocks(text: str) -> list[str]:
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # A true customer/order block starts with the customer code line.
    # In this report the address may continue over the next lines, and
    # "Packinglist - Order" appears a few lines later.
    # Do not treat rows like Australia, Location, Receipt, or Total volume as block starts.
    lines = text.splitlines(keepends=True)
    starts: list[int] = []
    pos = 0

    for idx, line in enumerate(lines):
        stripped = line.strip()

        if not stripped:
            pos += len(line)
            continue

        first_field = stripped.split("\t", 1)[0].strip().lower()

        if first_field in {"australia", "location", "receipt", "total volume"}:
            pos += len(line)
            continue

        if not re.match(r"^[A-Za-z0-9][A-Za-z0-9\-/]{1,40}\t", line.strip()):
            pos += len(line)
            continue

        lookahead = "".join(lines[idx: idx + 6])
        if re.search(r"Packing\s*list\s*-\s*Order|Packinglist\s*-\s*Order", lookahead, re.I):
            starts.append(pos)

        pos += len(line)

    if not starts:
        return []

    blocks: list[str] = []

    for i, start in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(text)
        block = text[start:end].strip()

        if re.search(r"Packing\s*list\s*-\s*Order|Packinglist\s*-\s*Order", block, re.I):
            blocks.append(block)

    return blocks

def bc_field(block: str, label: str, next_labels: list[str]) -> str:
    next_part = "|".join(re.escape(x) for x in next_labels)
    pattern = rf"{re.escape(label)}\t+(.+?)(?=\t+(?:{next_part})\t+|$)"
    match = re.search(pattern, block, re.I | re.S)
    return clean(match.group(1)) if match else ""



def bc_tokens(block: str) -> list[str]:
    raw_tokens = block.replace("\r", "").replace("\n", "\t").split("\t")
    return [clean(tok) for tok in raw_tokens if clean(tok)]


def bc_extract_header(block: str) -> dict[str, str]:
    tokens = bc_tokens(block)

    order_no = ""
    customer_account = ""
    customer_name = ""
    sales_person = ""
    delivery_date = ""
    phone = ""
    address = ""

    if tokens:
        first = tokens[0]
        if re.fullmatch(r"[A-Za-z]-\d+", first):
            customer_account = first
    if len(tokens) > 1:
        customer_name = tokens[1]

    # Header/address extraction must stop before "Australia".
    # The first part of the block is normally:
    # customer_code<TAB>customer_name<TAB>address line 1\naddress line 2\nAustralia<TAB>Telephone...
    pre_aus = re.split(r"\bAustralia\b", block, maxsplit=1, flags=re.I)[0]
    header_lines = [line for line in pre_aus.splitlines() if clean(line)]

    address_lines: list[str] = []
    if header_lines:
        first_parts = [clean(x) for x in header_lines[0].split("\t") if clean(x)]
        if len(first_parts) >= 3:
            customer_account = first_parts[0]
            customer_name = first_parts[1]
            address_lines.append(first_parts[2])
        elif len(first_parts) >= 2:
            customer_account = first_parts[0]
            customer_name = first_parts[1]

        for extra_line in header_lines[1:]:
            extra = clean(extra_line)
            if extra and not re.search(r"Packing\s*list\s*-\s*Order|Packinglist\s*-\s*Order|Location\s+Pallet", extra, re.I):
                address_lines.append(extra)

    address = ", ".join(address_lines)

    for i, tok in enumerate(tokens):
        low = tok.lower()
        if low == "sales order" and i + 1 < len(tokens) and bc_looks_like_order_id(tokens[i + 1]):
            order_no = tokens[i + 1]
        elif low == "recipient" and i + 1 < len(tokens):
            sales_person = tokens[i + 1]
        elif low == "tour date" and i + 1 < len(tokens):
            delivery_date = format_display_date(tokens[i + 1])
        elif low == "customer account" and i + 1 < len(tokens):
            customer_account = tokens[i + 1]
        elif low == "name" and i + 1 < len(tokens):
            customer_name = tokens[i + 1]
        elif low == "telephone" and i + 1 < len(tokens):
            if re.search(r"\d", tokens[i + 1]):
                phone = tokens[i + 1]
        elif low == "mobile phone" and i + 1 < len(tokens) and not phone:
            if re.search(r"\d", tokens[i + 1]):
                phone = tokens[i + 1]

    if not bc_looks_like_order_id(order_no):
        m = re.search(r"Sales\s*order\s*[:\-]?\s*([A-Za-z0-9\-/]*\d[A-Za-z0-9\-/]*)", block, re.I)
        if m:
            order_no = clean(m.group(1))

    return {
        "order_no": clean(order_no),
        "customer_account": clean(customer_account),
        "customer_name": clean(customer_name),
        "sales_person": clean(sales_person),
        "delivery_date": clean(delivery_date),
        "phone": clean(phone),
        "email": "",
        "address": clean(address),
    }

def bc_extract_note(block: str) -> str:
    lines = block.splitlines()
    total_idx = None
    for i, line in enumerate(lines):
        if re.search(r"^\s*Total volume\s+Total weight", line, re.I):
            total_idx = i
            break
    if total_idx is None or total_idx == 0:
        return ""
    for i in range(total_idx - 1, -1, -1):
        candidate = clean(lines[i])
        if candidate:
            return candidate
    return ""


def bc_extract_items(block: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    lines = block.splitlines()
    table_started = False
    for line in lines:
        if "Location\tPallet ID\tQuantity\tUnit\tItem number\tItem name" in line:
            table_started = True
            continue
        if not table_started:
            continue
        if re.search(r"^\s*Total volume", line):
            break
        parts = line.split("\t")
        if len(parts) < 6:
            continue
        qty = parts[2].strip() if len(parts) > 2 else ""
        unit = parts[3].strip().lower() if len(parts) > 3 else ""
        sku = parts[4].strip() if len(parts) > 4 else ""
        desc = parts[5].strip() if len(parts) > 5 else ""
        sales_order = parts[7].strip() if len(parts) > 7 else ""
        if unit != "pcs" or not sku or sku in EXCLUDED_BOCONCEPT_SKUS:
            continue
        items.append({
            "sku": clean(sku),
            "description": clean(desc),
            "qty": to_int(qty),
            "sales_order": clean(sales_order),
        })
    return items


def debug_boconcept_txt(uploaded_file) -> dict[str, Any]:
    uploaded_file.seek(0)
    raw = uploaded_file.read()
    uploaded_file.seek(0)

    if isinstance(raw, bytes):
        if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
            text = raw.decode("utf-16", errors="replace")
        else:
            for enc in ["utf-8-sig", "utf-8", "cp1252", "latin-1"]:
                try:
                    text = raw.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                text = raw.decode("latin-1", errors="replace")
    else:
        text = str(raw)

    blocks = bc_split_blocks(text)
    item_counts = [len(bc_extract_items(block)) for block in blocks]
    order_ids = []
    for block in blocks:
        header = bc_extract_header(block)
        items = bc_extract_items(block)
        order_id = clean(header.get("order_no"))
        if not order_id:
            order_id = clean(next((item.get("sales_order") for item in items if clean(item.get("sales_order"))), ""))
        if order_id:
            order_ids.append(order_id)

    return {
        "file_bytes": len(raw) if isinstance(raw, bytes) else len(str(raw)),
        "text_chars": len(text),
        "contains_packinglist_order": bool(re.search(r"Packing\s*list\s*-\s*Order|Packinglist\s*-\s*Order", text, re.I)),
        "blocks_found": len(blocks),
        "item_counts_per_block": item_counts,
        "total_items_found": sum(item_counts),
        "order_ids_found": order_ids,
        "first_300_chars": text[:300],
    }


def parse_boconcept_txt(uploaded_file) -> pd.DataFrame:
    uploaded_file.seek(0)
    raw = uploaded_file.read()

    if isinstance(raw, bytes):
        if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
            text = raw.decode("utf-16", errors="replace")
        else:
            for enc in ["utf-8-sig", "utf-8", "cp1252", "latin-1"]:
                try:
                    text = raw.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                text = raw.decode("latin-1", errors="replace")
    else:
        text = str(raw)

    if not text.strip():
        return empty_preview_df()
    blocks = bc_split_blocks(text)
    if not blocks:
        return empty_preview_df()

    rows: list[dict[str, Any]] = []
    for block in blocks:
        header = bc_extract_header(block)
        notes = bc_extract_note(block)
        items = bc_extract_items(block)
        order_id = clean(header.get("order_no"))
        if not order_id:
            order_id = clean(next((item.get("sales_order") for item in items if clean(item.get("sales_order"))), ""))
        if not order_id:
            continue
        for item in items:
            rows.append({
                "COD (money)": 0.0,
                "Service Time": 0,
                "Merchant": "BoConcept Adelaide",
                "Source": "BoConcept",
                "OrderID": order_id,
                "Customer Account": clean(header.get("customer_account")),
                "Recipient Name": clean(header.get("customer_name")),
                "Phone": clean(header.get("phone")),
                "Email": clean(header.get("email")),
                "Address": clean(header.get("address")),
                "Delivery Date": clean(header.get("delivery_date")),
                "Delivery Window": "",
                "Sales Person": clean(header.get("sales_person")),
                "SKU": clean(item.get("sku")),
                "Description": clean(item.get("description")),
                "Qty": to_int(item.get("qty")),
                "Notes": notes,
            })
    return prepare_preview_df(pd.DataFrame(rows)) if rows else empty_preview_df()


# -----------------------
# Options API / Transforma
# -----------------------
def options_to_date(iso_date: str) -> str:
    return datetime.strptime(iso_date, "%Y-%m-%d").strftime("%d/%m/%Y")


def build_request_xml(client_key: str, table_name: str, fields: list[str], conditions: list[tuple[str, str, str]], *, sort_by: str, max_records: int = 500, first_record: int = 1) -> str:
    fields_xml = "".join(f"<field>{escape(field)}</field>" for field in fields)
    conditions_xml = "".join(
        f'<condition field="{escape(field)}" type="{escape(cond_type)}">{escape(value)}</condition>'
        for field, cond_type, value in conditions
    )
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        f'<request clientKey="{escape(client_key)}">'
        f'<table name="{escape(table_name)}" sortBy="{escape(sort_by)}" maxRecords="{max_records}" firstRecord="{first_record}" requestType="">'
        f"<fields>{fields_xml}</fields>"
        f"<conditions>{conditions_xml}</conditions>"
        f"</table>"
        f"</request>"
    )


def extract_xml_from_response(text: str) -> str:
    cleaned = (text or "").lstrip("\ufeff\r\n\t ")
    xml_start = cleaned.find("<?xml")
    response_start = cleaned.find("<response")
    starts = [x for x in [xml_start, response_start] if x >= 0]
    if starts:
        cleaned = cleaned[min(starts):]
    return cleaned.strip()


def xml_field_object(record_el: ET.Element) -> dict[str, str]:
    result: dict[str, str] = {}
    for field in record_el.findall("./field"):
        name = field.attrib.get("name", "").strip()
        if name:
            result[name] = field.text or ""
    return result


def parse_table_records(response_root: ET.Element, table_name: str) -> list[dict[str, str]]:
    for table in response_root.findall("./table"):
        if table.attrib.get("name") == table_name:
            records_parent = table.find("./records")
            if records_parent is None:
                return []
            return [xml_field_object(record) for record in records_parent.findall("./record")]
    return []


def post_options_xml(xml_body: str) -> ET.Element:
    options_url = get_setting("OPTIONS_URL")
    client_key = get_setting("OPTIONS_CLIENT_KEY")
    if not options_url:
        raise RuntimeError("Missing OPTIONS_URL")
    if not client_key:
        raise RuntimeError("Missing OPTIONS_CLIENT_KEY")

    last_error: Exception | None = None
    last_response_text = ""
    for _ in range(3):
        try:
            response = requests.post(
                options_url,
                data=xml_body.encode("utf-8"),
                headers={
                    "Content-Type": "text/xml; charset=utf-8",
                    "Accept": "text/xml, application/xml, text/plain, */*",
                },
                timeout=60,
            )
            response.raise_for_status()
            raw_text = response.text
            last_response_text = raw_text
            if "routine maintenance" in raw_text.lower():
                raise RuntimeError("Options API is in routine maintenance mode.")
            xml_text = extract_xml_from_response(raw_text)
            if not xml_text:
                raise RuntimeError("Options API returned an empty response.")
            root = ET.fromstring(xml_text)
            errors = [clean(err.text) for err in root.findall("./error") if clean(err.text)]
            if errors:
                raise RuntimeError(" | ".join(errors))
            return root
        except Exception as exc:
            last_error = exc
    preview = last_response_text[:1500] if last_response_text else ""
    raise RuntimeError(f"Options API request failed after 3 attempts: {last_error}\n\nLast raw response preview:\n{preview}")


def fetch_lines_from_date(from_date: str) -> list[dict[str, str]]:
    client_key = get_setting("OPTIONS_CLIENT_KEY")
    xml_body = build_request_xml(
        client_key=client_key,
        table_name="DRSOLN",
        fields=["ORDNO", "ACCDE", "STOCKCODE", "DESC", "MEMODESC", "OVERDESC", "QTYORD", "PRICE", "GSTDOLL", "BOOKOUT", "DELIVTIME"],
        conditions=[("BOOKOUT", "greaterOrEqualTo", options_to_date(from_date))],
        sort_by="ORDNO",
        max_records=5000,
    )
    return parse_table_records(post_options_xml(xml_body), "DRSOLN")


def fetch_header(order_no: str) -> dict[str, str]:
    client_key = get_setting("OPTIONS_CLIENT_KEY")
    xml_body = build_request_xml(
        client_key=client_key,
        table_name="DRSOTR",
        fields=[
            "ORDNO",
            "ACCDE",
            "AREA",
            "CUSTNAME",
            "DELNAME",
            "CONTACT",
            "DEL1",
            "DEL2",
            "DEL3",
            "DEL4",
            "TOTAMOUNT",
            "PAID",
            "TEMPPAID",
            "IMESS",
            "NOTES",
            "SETUPNOTE",
            "MAINTNOTE",
            "DELNOTE",
        ],
        conditions=[("ORDNO", "equals", clean(order_no))],
        sort_by="ORDNO",
        max_records=1,
    )
    records = parse_table_records(post_options_xml(xml_body), "DRSOTR")
    return records[0] if records else {}


def fetch_payments_for_order(order_no: str) -> float:
    client_key = get_setting("OPTIONS_CLIENT_KEY")
    order_no = clean(order_no)

    fields_xml = (
        "<field>SALESORDER</field>"
        "<field>ACCDE</field>"
        "<field>TRANDTE</field>"
        "<field>TYPE</field>"
        "<field>TOTAMOUNT</field>"
    )

    table_xml = ""
    for table_name in ["DRTRAN", "DRHIST"]:
        table_xml += (
            f'<table name="{table_name}" sortBy="SALESORDER" maxRecords="0" firstRecord="1" requestType="">'
            f"<fields>{fields_xml}</fields>"
            "<conditions>"
            f'<condition field="SALESORDER" type="equals">{escape(order_no)}</condition>'
            '<condition field="TYPE" type="equals">PA</condition>'
            "</conditions>"
            "</table>"
        )

    xml_body = (
        '<?xml version="1.0" encoding="utf-8"?>'
        f'<request clientKey="{escape(client_key)}">'
        f"{table_xml}"
        "</request>"
    )

    root = post_options_xml(xml_body)
    records = parse_table_records(root, "DRTRAN") + parse_table_records(root, "DRHIST")

    return round(sum(to_float(record.get("TOTAMOUNT")) for record in records), 2)


def fetch_contact(accde: str, contact_name: str = "") -> dict[str, str]:
    client_key = get_setting("OPTIONS_CLIENT_KEY")
    xml_body = build_request_xml(
        client_key=client_key,
        table_name="DRSCON",
        fields=["ACCDE", "NAME", "PHONE", "MOBILE", "EMAIL"],
        conditions=[("ACCDE", "equals", clean(accde))],
        sort_by="NAME",
        max_records=20,
    )
    records = parse_table_records(post_options_xml(xml_body), "DRSCON")
    if not records:
        return {}

    wanted = clean(contact_name).lower()

    if wanted:
        for record in records:
            if clean(record.get("NAME")).lower() == wanted:
                return record

    for record in records:
        if clean(record.get("MOBILE")):
            return record

    for record in records:
        if clean(record.get("PHONE")):
            return record

    return records[0]


def extract_phone_from_text(*values: Any) -> str:
    combined = " ".join(clean(value) for value in values if clean(value))

    # Australian mobile: 04xx xxx xxx, with optional spaces.
    mobile_match = re.search(r"\b04\d{2}\s?\d{3}\s?\d{3}\b", combined)
    if mobile_match:
        return re.sub(r"\s+", "", mobile_match.group(0))

    # Australian landline: 0[2378] xxxx xxxx, with optional spaces.
    landline_match = re.search(r"\b0[2378]\s?\d{4}\s?\d{4}\b", combined)
    if landline_match:
        return re.sub(r"\s+", "", landline_match.group(0))

    # Fallback: any 8 to 12 digit phone-like number.
    generic_match = re.search(r"\b\d[\d\s]{7,14}\d\b", combined)
    if generic_match:
        return re.sub(r"\s+", "", generic_match.group(0))

    return ""


def map_lines_to_preview_rows(order_no: str, lines: list[dict[str, str]], header: dict[str, str], contact: dict[str, str], payments_total: float = 0.0) -> list[dict[str, Any]]:
    if not lines:
        return []
    cod_amount = calculate_cod_amount(header, lines, payments_total)

    customer_account = clean(header.get("ACCDE")) or clean(lines[0].get("ACCDE"))
    recipient_name = clean(header.get("DELNAME")) or clean(header.get("CUSTNAME"))
    phone = first_non_blank([
        contact.get("MOBILE"),
        contact.get("PHONE"),
        extract_phone_from_text(
            header.get("CONTACT"),
            header.get("DELNOTE"),
            header.get("NOTES"),
            header.get("SETUPNOTE"),
            header.get("MAINTNOTE"),
            header.get("IMESS"),
        ),
    ])
    email = clean(contact.get("EMAIL"))
    full_address = join_non_blank([header.get("DEL1"), header.get("DEL2"), header.get("DEL3"), header.get("DEL4")])
    notes = "\n".join([p for p in [clean_multiline(header.get("DELNOTE")), clean_multiline(header.get("NOTES")), clean_multiline(header.get("SETUPNOTE")), clean_multiline(header.get("MAINTNOTE")), clean_multiline(header.get("IMESS"))] if p])

    rows: list[dict[str, Any]] = []
    kept_index = 0
    for line in lines:
        sku = clean(line.get("STOCKCODE")).upper()
        if not sku or sku in EXCLUDED_TRANSFORMA_SKUS:
            continue
        description = clean(line.get("DESC")) or clean(line.get("MEMODESC")) or clean(line.get("OVERDESC")) or sku
        rows.append({
            "COD (money)": cod_amount if kept_index == 0 else 0.0,
            "Service Time": 0,
            "Merchant": "Transforma",
            "Source": "Transforma",
            "OrderID": clean(order_no),
            "Customer Account": customer_account,
            "Recipient Name": recipient_name,
            "Phone": phone,
            "Email": email,
            "Address": full_address,
            "Delivery Date": format_display_date(line.get("BOOKOUT")),
            "Delivery Window": clean(line.get("DELIVTIME")),
            "Sales Person": clean(header.get("AREA")),
            "SKU": sku,
            "Description": description,
            "Qty": to_int(line.get("QTYORD")),
            "Notes": notes,
        })
        kept_index += 1
    return rows


def fetch_transforma_options_preview(from_date: str | None = None) -> pd.DataFrame:
    from_date = clean(from_date) or date.today().strftime("%Y-%m-%d")
    lines = fetch_lines_from_date(from_date)
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for line in lines:
        order_no = clean(line.get("ORDNO"))
        if order_no:
            grouped[order_no].append(line)

    preview_rows: list[dict[str, Any]] = []
    contact_cache: dict[str, dict[str, str]] = {}
    for order_no in sorted(grouped.keys()):
        order_lines = grouped[order_no]
        header = fetch_header(order_no)
        if not header:
            continue
        accde = clean(header.get("ACCDE")) or clean(order_lines[0].get("ACCDE"))
        contact_key = f"{accde}|{clean(header.get('CONTACT'))}"
        if contact_key not in contact_cache:
            contact_cache[contact_key] = fetch_contact(accde, header.get("CONTACT")) if accde else {}

        payments_total = fetch_payments_for_order(order_no)
        preview_rows.extend(
            map_lines_to_preview_rows(
                order_no,
                order_lines,
                header,
                contact_cache.get(contact_key, {}),
                payments_total,
            )
        )
    return prepare_preview_df(pd.DataFrame(preview_rows)) if preview_rows else empty_preview_df()


# -----------------------
# Wodely push
# -----------------------
def get_group_value(group: pd.DataFrame, *columns: str) -> str:
    for column in columns:
        if column in group.columns:
            value = first_non_blank(group[column].tolist())
            if value:
                return value
    return ""


def build_packages_from_group(group: pd.DataFrame, order_id: str) -> list[dict[str, Any]]:
    packages: list[dict[str, Any]] = []
    for _, row in group.iterrows():
        sku = first_non_blank([row.get("SKU"), row.get("Item Code")])
        description = clean(row.get("Description"))
        qty = to_int(row.get("Qty"))
        if not sku and not description:
            continue
        package = {
            "productId": sku,
            "productDesc": description,
            "orderId": clean(order_id),
            "quantity": qty,
            "price": 0,
        }
        packages.append({k: v for k, v in package.items() if v not in ("", None)})
    return packages


def prune_none(data: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in data.items():
        if value is None:
            continue
        if isinstance(value, str) and value == "":
            continue
        cleaned[key] = value
    return cleaned


def build_wodely_payloads(df: pd.DataFrame) -> list[dict[str, Any]]:
    prepared = prepare_preview_df(df.copy())
    if "OrderID" not in prepared.columns:
        raise RuntimeError("Preview data does not contain OrderID")
    if prepared["OrderID"].astype(str).str.strip().eq("").any():
        raise RuntimeError("One or more rows are missing OrderID")

    payloads: list[dict[str, Any]] = []
    for order_id, group in prepared.groupby("OrderID", sort=True):
        order_id = clean(order_id)
        cod_amount = to_float(get_group_value(group, "COD (money)"))
        service_time = str(to_int(get_group_value(group, "Service Time")))
        after_dt, before_dt = build_after_before_datetime(
            get_group_value(group, "Delivery Date"),
            get_group_value(group, "Delivery Window"),
        )
        merchant_name = clean(get_group_value(group, "Merchant"))
        merchant_id_map = {
            "BoConcept Adelaide": "954bc139-aade-4a41-8e8f-23b5c50b1cc2",
            "Transforma": "8644fbc2-0420-4b54-8521-64c02a341949",
        }
        merchant_id = merchant_id_map.get(merchant_name, merchant_name)

        recipient_name = get_group_value(group, "Recipient Name")
        recipient_email = get_group_value(group, "Email")
        recipient_phone = get_group_value(group, "Phone")
        destination_address = get_group_value(group, "Address")
        sales_order_notes = get_group_value(group, "Notes")
        sales_person = get_group_value(group, "Sales Person")
        customer_code = get_group_value(group, "Customer Account")
        task_desc = " - ".join([part for part in [merchant_name, order_id, recipient_name] if clean(part)])
        packages = build_packages_from_group(group, order_id)

        payload = prune_none({
            "taskDesc": task_desc,
            "externalKey": order_id,
            "externalId": order_id,
            "merchantId": merchant_id,
            "afterDateTime": after_dt,
            "beforeDateTime": before_dt,
            "destinationAddress": destination_address,
            "recipientName": recipient_name,
            "recipientEmail": recipient_email or None,
            "recipientPhone": recipient_phone or None,
            "serviceTime": service_time,
            "amountDue": cod_amount,
            "tag1": sales_order_notes or None,
            "tag2": customer_code or None,
            "tag4": sales_person or None,
            "packages": packages,
        })

        if not payload.get("merchantId"):
            raise RuntimeError(f"Order {order_id} is missing merchantId")
        if not payload.get("destinationAddress"):
            raise RuntimeError(f"Order {order_id} is missing destinationAddress")
        if not payload.get("recipientName"):
            raise RuntimeError(f"Order {order_id} is missing recipientName")
        if not payload.get("packages"):
            raise RuntimeError(f"Order {order_id} has no packages")
        payloads.append(payload)
    return payloads




def get_wodely_task_create_url() -> str:
    explicit = get_setting("WODELY_TASK_CREATE_URL")
    if explicit:
        return explicit.rstrip("/")
    return "https://api.wodely.com/v2/tasks"


def get_wodely_task_list_url() -> str:
    explicit = get_setting("WODELY_TASK_SEARCH_URL") or get_setting("WODELY_TASK_LIST_URL")
    if explicit:
        return explicit.rstrip("/")

    # Wodely support/docs indicate existing task lookup should use the search endpoint.
    return "https://api.wodely.com/v2/tasks/search"


def get_wodely_headers() -> dict[str, str]:
    api_key = get_setting("WODELY_API_KEY")
    if not api_key:
        raise RuntimeError("Missing WODELY_API_KEY. Add it to Streamlit Secrets or environment variables.")

    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Basic {api_key}",
    }


def extract_task_records(body: Any) -> list[dict[str, Any]]:
    if isinstance(body, list):
        return [x for x in body if isinstance(x, dict)]

    if not isinstance(body, dict):
        return []

    for key in ["tasks", "data", "items", "results", "records", "rows"]:
        value = body.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]

    if any(k in body for k in ["id", "taskId", "externalId", "externalKey", "packages", "taskDesc"]):
        return [body]

    return []


def response_last_id(body: Any, tasks: list[dict[str, Any]]) -> str:
    if isinstance(body, dict):
        for key in ["lastId", "lastID", "last_id", "nextLastId", "next_last_id"]:
            value = clean(body.get(key))
            if value:
                return value

    if tasks:
        last_task = tasks[-1]
        for key in ["id", "taskId", "taskID", "_id"]:
            value = clean(last_task.get(key))
            if value:
                return value

    return ""


def task_status_text(task: dict[str, Any]) -> str:
    parts = []
    for field in [
        "status",
        "taskStatus",
        "taskStatusName",
        "statusName",
        "task_status",
        "task_status_name",
        "taskStatusText",
        "task_status_text",
        "state",
        "taskState",
        "task_state",
    ]:
        value = clean(task.get(field))
        if value:
            parts.append(value)

    return " ".join(parts).lower()


def task_is_cancelled(task: dict[str, Any]) -> bool:
    status_text = task_status_text(task)
    if "cancel" in status_text:
        return True

    status_id = clean(
        task.get("taskStatusId")
        or task.get("statusId")
        or task.get("task_status_id")
        or task.get("status_id")
    ).lower()

    return status_id in {"cancelled", "canceled"}


def task_matches_order_id(task: dict[str, Any], order_id: str) -> bool:
    order_id = clean(order_id).lower()

    if not order_id:
        return False

    for field in [
        "externalId",
        "externalID",
        "external_id",
        "externalKey",
        "external_key",
    ]:
        if clean(task.get(field)).lower() == order_id:
            return True

    packages = task.get("packages")
    if isinstance(packages, list):
        for package in packages:
            if not isinstance(package, dict):
                continue

            for field in ["orderId", "orderID", "order_id"]:
                if clean(package.get(field)).lower() == order_id:
                    return True

    for value in task.values():
        if isinstance(value, dict) and task_matches_order_id(value, order_id):
            return True
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict) and task_matches_order_id(item, order_id):
                    return True

    return False


def build_wodely_list_payload(order_id: str, *, completed: bool = False, last_id: str = "") -> dict[str, Any]:
    payload: dict[str, Any] = {
        "externalId": clean(order_id),
        "limit": 100,
    }

    if completed:
        payload.update({
            "taskStatusId": "50",
            "startDateTime": "2000-01-01T00:00:00Z",
            "endDateTime": "2099-12-31T23:59:59Z",
        })

    if last_id:
        payload["lastId"] = last_id

    return payload


def list_wodely_tasks_by_external_id(order_id: str, *, completed: bool = False, progress_area=None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    url = get_wodely_task_list_url()
    headers = get_wodely_headers()

    found: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    last_id = ""
    seen_last_ids: set[str] = set()

    for page_no in range(1, 26):
        payload = build_wodely_list_payload(order_id, completed=completed, last_id=last_id)

        if progress_area is not None:
            status_label = "completed" if completed else "active/current"
            progress_area.write(f"Checking Wodely {status_label} tasks for External ID {order_id}, page {page_no}")

        try:
            response = requests.post(url, json=payload, headers=headers, timeout=45)
        except Exception as exc:
            errors.append({"url": url, "payload": payload, "error": str(exc)})
            break

        try:
            body = response.json()
        except Exception:
            body = {"raw": response.text}

        if response.status_code >= 400:
            errors.append({
                "url": url,
                "payload": payload,
                "status_code": response.status_code,
                "response": body,
            })
            break

        tasks = extract_task_records(body)

        if progress_area is not None:
            progress_area.write(f"Wodely returned {len(tasks)} task record(s) on this page")

        if not tasks:
            break

        for task in tasks:
            if task_is_cancelled(task):
                continue

            if task_matches_order_id(task, order_id):
                found.append(task)

        next_last_id = response_last_id(body, tasks)

        if not next_last_id or next_last_id in seen_last_ids:
            break

        seen_last_ids.add(next_last_id)
        last_id = next_last_id

    return found, errors


def wodely_task_exists(order_id: str, progress_area=None) -> tuple[bool, str, list[dict[str, Any]]]:
    active_tasks, active_errors = list_wodely_tasks_by_external_id(
        order_id,
        completed=False,
        progress_area=progress_area,
    )

    if active_tasks:
        return True, f"found {len(active_tasks)} active/current non-cancelled task(s)", active_errors

    completed_tasks, completed_errors = list_wodely_tasks_by_external_id(
        order_id,
        completed=True,
        progress_area=progress_area,
    )

    if completed_tasks:
        return True, f"found {len(completed_tasks)} completed non-cancelled task(s)", active_errors + completed_errors

    return False, "no non-cancelled task found for this External ID", active_errors + completed_errors


def extract_order_ids_from_task(task: dict[str, Any]) -> set[str]:
    order_ids: set[str] = set()

    if task_is_cancelled(task):
        return order_ids

    for field in [
        "externalId",
        "externalID",
        "external_id",
        "externalKey",
        "external_key",
        "orderId",
        "orderID",
        "order_id",
    ]:
        value = clean(task.get(field))
        if value:
            order_ids.add(value.lower())

    packages = task.get("packages")
    if isinstance(packages, list):
        for package in packages:
            if not isinstance(package, dict):
                continue

            for field in ["orderId", "orderID", "order_id"]:
                value = clean(package.get(field))
                if value:
                    order_ids.add(value.lower())

    return order_ids


def build_wodely_bulk_search_payload(*, completed: bool = False, last_id: str = "") -> dict[str, Any]:
    payload: dict[str, Any] = {
        "limit": 100,
    }

    if completed:
        payload.update({
            "taskStatusId": "50",
            "startDateTime": "2000-01-01T00:00:00Z",
            "endDateTime": "2099-12-31T23:59:59Z",
        })

    if last_id:
        payload["lastId"] = last_id

    return payload


def list_existing_wodely_tasks(progress_area=None) -> dict[str, Any]:
    url = get_wodely_task_list_url()
    headers = get_wodely_headers()

    all_tasks: list[dict[str, Any]] = []
    non_cancelled_tasks: list[dict[str, Any]] = []
    existing_order_ids: set[str] = set()
    errors: list[dict[str, Any]] = []

    for completed in [False, True]:
        last_id = ""
        seen_last_ids: set[str] = set()
        status_label = "completed" if completed else "active/current"

        for page_no in range(1, 51):
            payload = build_wodely_bulk_search_payload(completed=completed, last_id=last_id)

            if progress_area is not None:
                progress_area.write(f"Listing Wodely {status_label} tasks: page {page_no}")

            try:
                response = requests.post(url, json=payload, headers=headers, timeout=45)
            except Exception as exc:
                errors.append({"url": url, "payload": payload, "error": str(exc)})
                break

            try:
                body = response.json()
            except Exception:
                body = {"raw": response.text}

            if response.status_code >= 400:
                errors.append({
                    "url": url,
                    "payload": payload,
                    "status_code": response.status_code,
                    "response": body,
                })
                break

            tasks = extract_task_records(body)

            if progress_area is not None:
                progress_area.write(f"Returned {len(tasks)} task record(s)")

            if not tasks:
                break

            for task in tasks:
                all_tasks.append(task)

                if task_is_cancelled(task):
                    continue

                non_cancelled_tasks.append(task)
                existing_order_ids.update(extract_order_ids_from_task(task))

            next_last_id = response_last_id(body, tasks)

            if not next_last_id or next_last_id in seen_last_ids:
                break

            seen_last_ids.add(next_last_id)
            last_id = next_last_id

    return {
        "endpoint": url,
        "all_task_count": len(all_tasks),
        "non_cancelled_task_count": len(non_cancelled_tasks),
        "existing_order_ids": sorted(existing_order_ids),
        "existing_order_id_count": len(existing_order_ids),
        "errors": errors[:10],
        "error_count": len(errors),
        "sample_tasks": all_tasks[:3],
    }


def push_preview_to_wodely(
    df: pd.DataFrame,
    existing_order_ids: set[str],
    progress_area=None,
) -> dict[str, Any]:
    url = get_wodely_task_create_url()
    headers = get_wodely_headers()

    payloads = build_wodely_payloads(df)
    successes: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    seen_in_this_push: set[str] = set()
    existing_order_ids = {clean(order_id).lower() for order_id in existing_order_ids if clean(order_id)}

    if progress_area is not None:
        progress_area.write(f"Prepared {len(payloads)} task(s)")
        progress_area.write(f"Loaded {len(existing_order_ids)} existing Wodely order id(s) for duplicate checking")

    for idx, payload in enumerate(payloads, start=1):
        order_id = clean(payload.get("externalKey"))
        order_key = order_id.lower()

        if progress_area is not None:
            progress_area.write(f"Processing {idx}/{len(payloads)}: {order_id}")

        if order_key in seen_in_this_push:
            skipped.append({
                "index": idx,
                "orderId": order_id,
                "status_code": "SKIPPED",
                "response": "Duplicate within this push batch",
            })
            if progress_area is not None:
                progress_area.write(f"Skipped {order_id}: duplicate within this push batch")
            continue

        seen_in_this_push.add(order_key)

        if order_key in existing_order_ids:
            skipped.append({
                "index": idx,
                "orderId": order_id,
                "status_code": "SKIPPED",
                "response": "Already exists in listed Wodely non-cancelled tasks",
            })
            if progress_area is not None:
                progress_area.write(f"Skipped {order_id}: already exists in Wodely task list")
            continue

        if progress_area is not None:
            progress_area.write(f"Creating {order_id} in Wodely")

        response = requests.post(url, json=payload, headers=headers, timeout=120)
        try:
            body = response.json()
        except Exception:
            body = {"raw": response.text}

        row = {
            "index": idx,
            "orderId": order_id,
            "status_code": response.status_code,
            "response": body,
        }

        if response.status_code >= 400:
            failures.append(row)
            if progress_area is not None:
                progress_area.write(f"Failed {order_id}: HTTP {response.status_code}")
        else:
            successes.append(row)
            existing_order_ids.add(order_key)
            record_sent_order(
                order_id=order_id,
                source=clean(payload.get("merchantId")),
                recipient_name=clean(payload.get("recipientName")),
                response_body=body,
            )
            if progress_area is not None:
                progress_area.write(f"Created {order_id}")

    if failures:
        raise RuntimeError(
            f"Wodely push failed for {len(failures)} of {len(payloads)} task(s). "
            f"Endpoint: {url}. Failures: {failures}. Skipped: {skipped}"
        )

    return {
        "ok": True,
        "endpoint": url,
        "payload_count": len(payloads),
        "created_count": len(successes),
        "skipped_count": len(skipped),
        "existing_wodely_order_count": len(existing_order_ids),
        "successes": successes,
        "skipped": skipped,
        "payload_preview": payloads[:3],
    }


# -----------------------
# Streamlit UI
# -----------------------
st.markdown(STYLE, unsafe_allow_html=True)
st.title("Delivery to Wodely")
st.caption("BoConcept Packing List - Order TXT + Transforma Options API -> preview -> CSV export -> Wodely create with duplicate protection")

if "preview_df" not in st.session_state:
    st.session_state.preview_df = empty_preview_df()
if "push_result" not in st.session_state:
    st.session_state.push_result = None
if "wodely_existing_order_ids" not in st.session_state:
    st.session_state.wodely_existing_order_ids = set()
if "wodely_task_list_result" not in st.session_state:
    st.session_state.wodely_task_list_result = None

left, right = st.columns([1, 1])

with left:
    st.markdown("### BoConcept")
    bc_file = st.file_uploader("Packing List - Order TXT", type=["txt"], key="bc_txt")

with right:
    st.markdown("### Transforma")
    default_date = date.today().strftime("%Y-%m-%d")
    from_date = st.date_input("Pull Options bookings from", value=date.today(), format="YYYY-MM-DD")

button_left, button_right = st.columns([1, 1])

with button_left:
    if st.button("Add BoConcept to preview", use_container_width=True):
        try:
            if bc_file is None:
                raise RuntimeError("Upload a BoConcept TXT file first.")
            bc_df = parse_boconcept_txt(bc_file)
            if bc_df.empty:
                diagnostics = debug_boconcept_txt(bc_file)
                raise RuntimeError(f"No BoConcept rows found in the uploaded file. Diagnostics: {json.dumps(diagnostics, indent=2)}")
            st.session_state.preview_df = append_preview(st.session_state.preview_df, bc_df)
            st.success(f"Added {len(bc_df)} BoConcept rows.")
        except Exception as exc:
            st.error(str(exc))

with button_right:
    if st.button("Add Transforma API to preview", use_container_width=True):
        try:
            tf_df = fetch_transforma_options_preview(from_date.strftime("%Y-%m-%d") if from_date else default_date)
            if tf_df.empty:
                raise RuntimeError("No Transforma rows returned from Options.")
            st.session_state.preview_df = append_preview(st.session_state.preview_df, tf_df)
            st.success(f"Added {len(tf_df)} Transforma rows.")
        except Exception as exc:
            st.error(str(exc))

st.markdown("### Preview")
preview_df = prepare_preview_df(st.session_state.preview_df.copy())

summary_cols = st.columns(4)
summary_cols[0].metric("Rows", len(preview_df))
summary_cols[1].metric("Orders", preview_df["OrderID"].astype(str).str.strip().replace("", pd.NA).dropna().nunique() if not preview_df.empty else 0)
summary_cols[2].metric("BoConcept rows", int((preview_df["Source"] == "BoConcept").sum()) if not preview_df.empty else 0)
summary_cols[3].metric("Transforma rows", int((preview_df["Source"] == "Transforma").sum()) if not preview_df.empty else 0)

edited = st.data_editor(
    preview_df,
    use_container_width=True,
    height=520,
    num_rows="dynamic",
    key="preview_editor",
    column_config={
        "COD (money)": st.column_config.NumberColumn(format="%.2f"),
        "Service Time": st.column_config.NumberColumn(format="%d"),
        "Qty": st.column_config.NumberColumn(format="%d"),
    },
)
st.session_state.preview_df = prepare_preview_df(edited)

actions = st.columns([1, 1, 1, 1, 1])
with actions[0]:
    if st.button("Clear preview", use_container_width=True):
        st.session_state.preview_df = empty_preview_df()
        st.session_state.push_result = None
        st.rerun()

with actions[1]:
    csv_bytes = prepare_preview_df(st.session_state.preview_df.copy()).to_csv(index=False).encode("utf-8-sig")
    st.download_button("Download CSV", data=csv_bytes, file_name="delivery_preview.csv", mime="text/csv", use_container_width=True)

with actions[2]:
    if st.button("Show Wodely payload", use_container_width=True, disabled=preview_df.empty):
        try:
            payloads = build_wodely_payloads(st.session_state.preview_df.copy())
            st.session_state.push_result = {"preview_only": True, "payload_count": len(payloads), "payload_preview": payloads[:3]}
        except Exception as exc:
            st.session_state.push_result = {"error": str(exc)}

with actions[3]:
    if st.button("List Existing Wodely Tasks", use_container_width=True):
        process_box = st.container(border=True)
        process_box.markdown("### Wodely Task Listing")
        try:
            result = list_existing_wodely_tasks(progress_area=process_box)
            st.session_state.wodely_task_list_result = result
            st.session_state.wodely_existing_order_ids = set(result.get("existing_order_ids", []))
            process_box.success(
                f"Loaded {result.get('existing_order_id_count', 0)} existing non-cancelled order id(s)."
            )
        except Exception as exc:
            st.session_state.wodely_task_list_result = {"error": str(exc)}
            st.session_state.wodely_existing_order_ids = set()
            process_box.error(str(exc))
            st.error(str(exc))

with actions[4]:
    list_ready = bool(st.session_state.wodely_existing_order_ids) or st.session_state.wodely_task_list_result is not None
    if st.button("Push to Wodely", use_container_width=True, type="primary", disabled=preview_df.empty or not list_ready):
        process_box = st.container(border=True)
        process_box.markdown("### Push Process")
        try:
            st.session_state.push_result = push_preview_to_wodely(
                st.session_state.preview_df.copy(),
                existing_order_ids=set(st.session_state.wodely_existing_order_ids),
                progress_area=process_box,
            )
            process_box.success("Process complete.")
            st.success("Pushed to Wodely.")
        except Exception as exc:
            st.session_state.push_result = {"error": str(exc)}
            process_box.error(str(exc))
            st.error(str(exc))

if st.session_state.wodely_task_list_result is not None:
    st.markdown("### Existing Wodely Tasks")
    list_result = st.session_state.wodely_task_list_result
    if isinstance(list_result, dict) and "error" in list_result:
        st.error(list_result["error"])
    else:
        existing_ids = sorted(st.session_state.wodely_existing_order_ids)
        st.write({
            "endpoint": list_result.get("endpoint"),
            "all_task_count": list_result.get("all_task_count"),
            "non_cancelled_task_count": list_result.get("non_cancelled_task_count"),
            "existing_order_id_count": len(existing_ids),
            "error_count": list_result.get("error_count"),
        })
        st.dataframe(pd.DataFrame({"Existing Wodely OrderID": existing_ids}), use_container_width=True, height=220)

with st.expander("Diagnostics", expanded=False):
    st.write("App version:", APP_VERSION)
    st.write("Merchant mapping:", {"BoConcept Adelaide": "954bc139-aade-4a41-8e8f-23b5c50b1cc2", "Transforma": "8644fbc2-0420-4b54-8521-64c02a341949"})
    st.write("Preview columns:", list(st.session_state.preview_df.columns))
    st.write("Wodely task create endpoint:", get_wodely_task_create_url())
    st.write("Wodely task search endpoint:", get_wodely_task_list_url())
    st.write("Sent-orders register:", str(get_sent_orders_file()))
    st.write("Sent orders recorded for audit only:", len(load_sent_order_ids()))
    st.write("Loaded Wodely existing order IDs:", len(st.session_state.wodely_existing_order_ids))
    if not st.session_state.preview_df.empty:
        st.write("Blank OrderID rows:", int(st.session_state.preview_df["OrderID"].astype(str).str.strip().eq("").sum()))
        st.write("Orders in preview:", sorted([x for x in st.session_state.preview_df["OrderID"].astype(str).str.strip().unique().tolist() if x]))
    if st.button("Clear sent-orders audit register", use_container_width=True):
        sent_file = get_sent_orders_file()
        if sent_file.exists():
            sent_file.unlink()
        st.success("Sent-orders audit register cleared.")
        st.rerun()

if st.session_state.push_result is not None:
    st.markdown("### Result")
    st.json(st.session_state.push_result)
