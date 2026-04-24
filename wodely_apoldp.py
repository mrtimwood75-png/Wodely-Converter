from __future__ import annotations

import os
import re
import json
from collections import defaultdict
from datetime import date, datetime, time
from html import escape
from io import BytesIO
from typing import Any
import xml.etree.ElementTree as ET

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Delivery to Wodely", layout="wide")

APP_VERSION = "2026-04-24-v4-boconcept-parser-diagnostics"

OUTPUT_COLUMNS = [
    "COD (money)",
    "Service Time",
    "Merchant",
    "Source",
    "OrderID",
    "Customer Account",
    "Recipient Name",
    "Contact Name",
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
    return str(st.secrets.get(name, os.getenv(name, default))).strip()


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
        "Contact Name": ["Contact", "ContactName", "NAME"],
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
        return new_df.reset_index(drop=True)
    combined = pd.concat([existing_df, new_df], ignore_index=True)
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

    if "Australia" in tokens:
        aus_idx = tokens.index("Australia")
        addr_parts = []
        start_idx = 2 if customer_account and customer_name else 0
        for tok in tokens[start_idx:aus_idx]:
            if tok.lower() in {"telephone", "mobile phone"}:
                break
            addr_parts.append(tok)
        address = ", ".join([p for p in addr_parts if p])

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
        "contact_name": clean(customer_name),
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
                "Contact Name": clean(header.get("contact_name")),
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
        fields=["ORDNO", "ACCDE", "STOCKCODE", "DESC", "MEMODESC", "OVERDESC", "QTYORD", "BOOKOUT", "DELIVTIME"],
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
        fields=["ORDNO", "ACCDE", "AREA", "CUSTNAME", "DELNAME", "CONTACT", "DEL1", "DEL2", "DEL3", "DEL4", "TOTAMOUNT", "PAID", "TEMPPAID", "IMESS", "NOTES", "SETUPNOTE", "MAINTNOTE", "DELNOTE"],
        conditions=[("ORDNO", "equals", clean(order_no))],
        sort_by="ORDNO",
        max_records=1,
    )
    records = parse_table_records(post_options_xml(xml_body), "DRSOTR")
    return records[0] if records else {}


def fetch_contact(accde: str) -> dict[str, str]:
    client_key = get_setting("OPTIONS_CLIENT_KEY")
    xml_body = build_request_xml(
        client_key=client_key,
        table_name="DRSCON",
        fields=["ACCDE", "NAME", "MOBILE", "EMAIL"],
        conditions=[("ACCDE", "equals", clean(accde))],
        sort_by="ACCDE",
        max_records=1,
    )
    records = parse_table_records(post_options_xml(xml_body), "DRSCON")
    return records[0] if records else {}


def map_lines_to_preview_rows(order_no: str, lines: list[dict[str, str]], header: dict[str, str], contact: dict[str, str]) -> list[dict[str, Any]]:
    if not lines:
        return []
    total_amount = to_float(header.get("TOTAMOUNT"))
    paid = to_float(header.get("PAID"))
    temp_paid = to_float(header.get("TEMPPAID"))
    cod_amount = max(total_amount - paid - temp_paid, 0.0)

    customer_account = clean(header.get("ACCDE")) or clean(lines[0].get("ACCDE"))
    recipient_name = clean(header.get("DELNAME")) or clean(header.get("CUSTNAME"))
    contact_name = clean(contact.get("NAME")) or clean(header.get("CONTACT"))
    phone = clean(contact.get("MOBILE"))
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
            "Contact Name": contact_name,
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
        if accde not in contact_cache:
            contact_cache[accde] = fetch_contact(accde) if accde else {}
        preview_rows.extend(map_lines_to_preview_rows(order_no, order_lines, header, contact_cache.get(accde, {})))
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
        recipient_name = get_group_value(group, "Recipient Name", "Customer")
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
            "merchantId": merchant_name,
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


def push_preview_to_wodely(df: pd.DataFrame) -> dict[str, Any]:
    url = get_wodely_task_create_url()
    api_key = get_setting("WODELY_API_KEY")
    if not api_key:
        raise RuntimeError("Missing WODELY_API_KEY")

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Basic {api_key}",
    }

    payloads = build_wodely_payloads(df)
    successes: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for idx, payload in enumerate(payloads, start=1):
        response = requests.post(url, json=payload, headers=headers, timeout=120)
        try:
            body = response.json()
        except Exception:
            body = {"raw": response.text}

        row = {
            "index": idx,
            "orderId": payload.get("externalKey"),
            "status_code": response.status_code,
            "response": body,
        }

        if response.status_code >= 400:
            failures.append(row)
        else:
            successes.append(row)

    if failures:
        raise RuntimeError(
            f"Wodely push failed for {len(failures)} of {len(payloads)} task(s). "
            f"Endpoint: {url}. Failures: {failures}"
        )

    return {
        "ok": True,
        "endpoint": url,
        "payload_count": len(payloads),
        "successes": successes,
        "payload_preview": payloads[:3],
    }


# -----------------------
# Streamlit UI
# -----------------------
st.markdown(STYLE, unsafe_allow_html=True)
st.title("Delivery to Wodely")
st.caption("BoConcept packing list TXT + Transforma Options API -> one preview -> CSV export -> Wodely bulk create")

if "preview_df" not in st.session_state:
    st.session_state.preview_df = empty_preview_df()
if "push_result" not in st.session_state:
    st.session_state.push_result = None

left, right = st.columns([1, 1])

with left:
    st.markdown("### BoConcept")
    bc_file = st.file_uploader("Packing list TXT", type=["txt"], key="bc_txt")
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

with right:
    st.markdown("### Transforma")
    default_date = date.today().strftime("%Y-%m-%d")
    from_date = st.date_input("Pull Options bookings from", value=date.today(), format="YYYY-MM-DD")
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
    num_rows="dynamic",
    key="preview_editor",
    column_config={
        "COD (money)": st.column_config.NumberColumn(format="%.2f"),
        "Service Time": st.column_config.NumberColumn(format="%d"),
        "Qty": st.column_config.NumberColumn(format="%d"),
    },
)
st.session_state.preview_df = prepare_preview_df(edited)

actions = st.columns([1, 1, 1, 1])
with actions[0]:
    if st.button("Clear preview", use_container_width=True):
        st.session_state.preview_df = empty_preview_df()
        st.session_state.push_result = None
        st.rerun()
with actions[1]:
    csv_bytes = prepare_preview_df(st.session_state.preview_df.copy()).to_csv(index=False).encode("utf-8-sig")
    st.download_button("Download CSV", data=csv_bytes, file_name="delivery_preview.csv", mime="text/csv", use_container_width=True)
with actions[2]:
    if st.button("Show Wodely payload", use_container_width=True):
        try:
            payloads = build_wodely_payloads(st.session_state.preview_df.copy())
            st.session_state.push_result = {"preview_only": True, "payload_count": len(payloads), "payload_preview": payloads[:3]}
        except Exception as exc:
            st.session_state.push_result = {"error": str(exc)}
with actions[3]:
    if st.button("Push to Wodely", use_container_width=True, type="primary"):
        try:
            st.session_state.push_result = push_preview_to_wodely(st.session_state.preview_df.copy())
            st.success("Pushed to Wodely.")
        except Exception as exc:
            st.session_state.push_result = {"error": str(exc)}
            st.error(str(exc))

with st.expander("Diagnostics", expanded=False):
    st.write("App version:", APP_VERSION)
    st.write("Preview columns:", list(st.session_state.preview_df.columns))
    if not st.session_state.preview_df.empty:
        st.write("Blank OrderID rows:", int(st.session_state.preview_df["OrderID"].astype(str).str.strip().eq("").sum()))
        st.write("Orders in preview:", sorted([x for x in st.session_state.preview_df["OrderID"].astype(str).str.strip().unique().tolist() if x]))

if st.session_state.push_result is not None:
    st.markdown("### Result")
    st.json(st.session_state.push_result)
