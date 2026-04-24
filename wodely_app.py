```python
# =========================
# WODELY DELIVERY APP v27
# =========================

import streamlit as st
import pandas as pd
import requests
import json
import re
from datetime import datetime

APP_VERSION = "2026-04-24-v27-final-merchant-fix"

# -----------------------
# HELPERS
# -----------------------

def clean(value):
    return str(value or "").strip()

def to_float(value):
    try:
        return float(value)
    except:
        return 0.0

# -----------------------
# WODELY CONFIG
# -----------------------

def get_wodely_url():
    return "https://api.wodely.com/v2/tasks"

def get_headers():
    return {
        "Content-Type": "application/json",
        "Authorization": f"Basic {st.secrets['WODELY_API_KEY']}"
    }

# -----------------------
# MERCHANT MAPPING (FIXED)
# -----------------------

def map_merchant_id(merchant_name):
    merchant_name = clean(merchant_name)

    if merchant_name == "BoConcept Adelaide":
        return "954bc139-aade-4a41-8e8f-23b5c50b1cc2"

    if merchant_name == "Transforma":
        return "8644fbc2-0420-4b54-8521-64c02a341949"

    return merchant_name  # fallback

# -----------------------
# DUPLICATE PREVENTION (LOCAL REGISTER ONLY — RELIABLE)
# -----------------------

SENT_FILE = "sent_orders.csv"

def load_sent():
    try:
        df = pd.read_csv(SENT_FILE)
        return set(df["OrderID"].astype(str))
    except:
        return set()

def save_sent(order_id):
    try:
        df = pd.read_csv(SENT_FILE)
    except:
        df = pd.DataFrame(columns=["OrderID"])

    if order_id not in df["OrderID"].values:
        df.loc[len(df)] = [order_id]
        df.to_csv(SENT_FILE, index=False)

# -----------------------
# BUILD PAYLOAD
# -----------------------

def build_payload(row):

    order_id = clean(row["OrderID"])
    merchant_name = clean(row["Merchant"])
    merchant_id = map_merchant_id(merchant_name)

    payload = {
        "taskDesc": f"{merchant_name} - {order_id} - {row['Recipient Name']}",
        "externalKey": order_id,
        "externalId": order_id,
        "merchantId": merchant_id,
        "recipientName": row["Recipient Name"],
        "recipientPhone": row["Phone"],
        "destinationAddress": row["Address"],
        "afterDateTime": row["After"],
        "beforeDateTime": row["Before"],
        "amountDue": to_float(row["COD"]),
        "packages": [
            {
                "productId": "GEN",
                "productDesc": row["Description"],
                "orderId": order_id,
                "quantity": int(row["Qty"]),
                "price": to_float(row["Price"])
            }
        ]
    }

    return payload

# -----------------------
# PUSH TO WODELY
# -----------------------

def push(df):

    sent = load_sent()
    results = []

    for _, row in df.iterrows():

        order_id = clean(row["OrderID"])

        if order_id in sent:
            results.append((order_id, "SKIPPED (already sent)"))
            continue

        payload = build_payload(row)

        res = requests.post(
            get_wodely_url(),
            headers=get_headers(),
            json=payload
        )

        if res.status_code == 200:
            save_sent(order_id)
            results.append((order_id, "CREATED"))
        else:
            results.append((order_id, f"ERROR {res.status_code}"))

    return results

# -----------------------
# UI
# -----------------------

st.title("Delivery to Wodely")
st.caption(APP_VERSION)

uploaded = st.file_uploader("Upload CSV")

if uploaded:
    df = pd.read_csv(uploaded)

    st.write("Preview", df)

    if st.button("Push to Wodely"):
        result = push(df)
        st.write(result)
```
