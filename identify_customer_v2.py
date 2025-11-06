import pandas as pd
import json
import phonenumbers
from fuzzywuzzy import fuzz
from itertools import combinations
from datetime import datetime

# ======== CONFIG ==========
INPUT_FILE = "process/process_orders_2025-10-01_to_2025-10-30.json"
OUTPUT_FILE = "mongodb_customer_profiles.json"
FUZZY_THRESHOLD = 90
# ==========================

# ---- Helper Functions ----
def normalize_phone(phone):
    """Normalize phone numbers to +66 E.164 format (Thai numbers)."""
    try:
        parsed = phonenumbers.parse(str(phone), "TH")
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        return None
    return None


def fuzzy_group(df, column, prefix, threshold=FUZZY_THRESHOLD):
    """Fuzzy group records by column text similarity."""
    records = df[[column]].dropna().reset_index()
    for i, j in combinations(records.index, 2):
        val1, val2 = records.loc[i, column], records.loc[j, column]
        score = fuzz.token_sort_ratio(val1, val2)
        if score >= threshold:
            uid = f"{prefix}_{records.loc[i, column][:10]}"
            df.loc[[records.loc[i, 'index'], records.loc[j, 'index']], 'unique_id'] = uid


# ---- Load Data ----
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)
df = pd.json_normalize(data)

# ---- Normalize Phones ----
if 'phone' in df.columns:
    df['normalized_phone'] = df['phone'].apply(normalize_phone)
elif 'shipping_phone' in df.columns:
    df['normalized_phone'] = df['shipping_phone'].apply(normalize_phone)
else:
    df['normalized_phone'] = None

# ---- Initialize Unique ID ----
df['unique_id'] = None

# ---- Step 1: Shopee Platform ID ----
if 'order_from' in df.columns:
    df.loc[df['order_from'] == 16, 'unique_id'] = df.get('shopee_info.shopee_user_id', None)

# ---- Step 2: Member ID / Extern Member ID ----
if 'member_id' in df.columns:
    df.loc[(df['order_from'] != 16) & df['member_id'].notnull(), 'unique_id'] = df['member_id']
if 'extern_member_id' in df.columns:
    df.loc[(df['order_from'] != 16) & df['unique_id'].isnull() & df['extern_member_id'].notnull(), 'unique_id'] = df['extern_member_id']

# ---- Step 3: Phone Match ----
for phone, group in df[df['unique_id'].isnull()].groupby('normalized_phone'):
    if phone and len(group) > 1:
        uid = f"phone_{phone}"
        df.loc[df['normalized_phone'] == phone, 'unique_id'] = uid

# ---- Step 4: Email Match ----
if 'email' in df.columns:
    for email, group in df[df['unique_id'].isnull()].groupby('email'):
        if email and len(group) > 1:
            uid = f"email_{email}"
            df.loc[df['email'] == email, 'unique_id'] = uid

# ---- Step 5: Address Fuzzy Match ----
address_fields = ['shipping_address_1', 'shipping_subdistrict', 'shipping_district', 'shipping_province', 'shipping_zipcode']
for col in address_fields:
    if col not in df.columns:
        df[col] = ''
df['address_composite'] = df[address_fields].fillna('').agg(' '.join, axis=1)
fuzzy_group(df[df['unique_id'].isnull()], 'address_composite', 'addr')

# ---- Step 6: Name Fuzzy Match ----
df['name_composite'] = (
    df.get('firstname', '') + ' ' + df.get('lastname', '')
).fillna('').str.strip()
fuzzy_group(df[df['unique_id'].isnull()], 'name_composite', 'name')

# ---- Step 7: Assign Remaining Unique IDs ----
remaining = df['unique_id'].isnull().sum()
df.loc[df['unique_id'].isnull(), 'unique_id'] = [
    f"cust_{i}" for i in range(remaining)
]

# ---- Build MongoDB Customer Profiles ----
customer_profiles = []

for uid, group in df.groupby('unique_id'):
    cluster = group.to_dict('records')
    sources = []

    for o in cluster:
        # 1️⃣ Social array
        social_data = o.get("social")
        if isinstance(social_data, list):
            for s in social_data:
                sources.append({
                    "channel_id": o.get("channel_id"),
                    "platform": s.get("platform"),
                    "channel_name": s.get("channel_name"),
                    "wsis_id": s.get("wsis_id"),
                    "platform_id": s.get("social_id"),
                })

        # 2️⃣ Shopee
        sh = o.get("shopee_info") or {}
        if isinstance(sh, dict) and sh.get("shopee_user_id"):
            sources.append({
                "channel_id": o.get("channel_id"),
                "platform": "SHOPEE",
                "channel_name": sh.get("shopee_user_name"),
                "wsis_id": None,
                "platform_id": str(sh.get("shopee_user_id")),
            })

        # 3️⃣ Lazada
        if o.get("order_from") == 12:
            laz = o.get("lazada_info") or {}
            sources.append({
                "channel_id": o.get("channel_id"),
                "platform": "LAZADA",
                "channel_name": laz.get("lazada_user_name"),
                "wsis_id": None,
                "platform_id": str(laz.get("lazada_user_id")),
            })

        # 4️⃣ LINE Shopping
        if o.get("order_from") == 21:
            line = o.get("line_info") or {}
            sources.append({
                "channel_id": o.get("channel_id"),
                "platform": "LINE",
                "channel_name": line.get("line_user_name"),
                "wsis_id": None,
                "platform_id": str(line.get("line_user_id")),
            })

    # Remove duplicates in sources (by platform + platform_id)
    unique_sources = []
    seen = set()
    for s in sources:
        key = (s.get("platform"), s.get("platform_id"))
        if key not in seen:
            seen.add(key)
            unique_sources.append(s)

    customer = {
        "_id": uid,
        "full_name": group['name_composite'].dropna().unique().tolist(),
        "phones": group['normalized_phone'].dropna().unique().tolist(),
        "emails": group['email'].dropna().unique().tolist() if 'email' in df.columns else [],
        "sources": unique_sources,
        "rfm": {},
        "tags": [],
        "orders": group[['order_id', 'order_date', 'grand_total', 'order_from']].dropna().to_dict('records') if all(
            c in group.columns for c in ['order_id', 'order_date', 'grand_total', 'order_from']
        ) else [],
        "created_date": datetime.utcnow().isoformat(),
        "modified_date": datetime.utcnow().isoformat(),
        "provider_id": str(group['provider_id'].dropna().unique()[0]) if 'provider_id' in df.columns and not group['provider_id'].dropna().empty else None
    }

    customer_profiles.append(customer)

# ---- Export to MongoDB JSON ----
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(customer_profiles, f, ensure_ascii=False, indent=2)

print(f"✅ Customer Single View created successfully: {OUTPUT_FILE}")
