import json
import pandas as pd
import uuid
from datetime import datetime, date

# === CONFIG ===
INPUT_FILE = "crm_customer_profiles.json"
OUTPUT_FILE = "crm_customer_profiles_updated.json"
HISTORY_FILE = "rfm_history.json"

# === LOAD DATA ===
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    profiles = json.load(f)

snapshot_date = date.today()
data_list = []


def classify_segment(row):
    r = row['r_score']
    fm = (row['f_score'] + row['m_score']) / 2

    if r == 1 and 0 < fm < 2.0:
        return "Lost"
    elif r == 1 and 2.0 <= fm < 4.0:
        return "At Risk"
    elif r == 1 and fm >= 4.0:
        return "Can't Lose"

    elif r == 2 and 0 < fm < 1.0:
        return "Lost"
    elif r == 2 and 1.0 <= fm < 2.0:
        return "Hibernating"
    elif r == 2 and fm >= 2.0:
        return "At Risk"

    elif r == 3 and 0 < fm < 2.0:
        return "About to Sleep"
    elif r == 3 and 2.0 <= fm < 3.0:
        return "Need Attention"
    elif r == 3 and fm >= 3.0:
        return "Loyal Customers"

    elif r == 4 and 0 < fm < 1.0:
        return "Promising"
    elif r == 4 and 1.0 <= fm < 3.0:
        return "Potential Loyalists"
    elif r == 4 and fm >= 3.0:
        return "Loyal Customers"

    elif r == 5 and 0 < fm < 1.0:
        return "New Customers"
    elif r == 5 and 1.0 <= fm < 3.0:
        return "Potential Loyalists"
    elif r == 5 and 3.0 <= fm < 4.0:
        return "Loyal Customers"
    elif r == 5 and fm >= 4.0:
        return "Champions"

    return "Unclassified"


# === AGGREGATE RFM METRICS ===
for cust in profiles:
    orders = cust.get("orders", [])
    total_orders = len(orders)
    total_amount = sum(order.get("grand_total", 0) for order in orders)

    if total_orders > 0:
        latest_order_date = max(datetime.fromisoformat(
            order["order_date"]).date() for order in orders)
        recency_days = (snapshot_date - latest_order_date).days
        latest_order_str = latest_order_date.isoformat()
    else:
        recency_days = float('inf')
        latest_order_str = None

    data_list.append({
        "_id": cust.get("_id"),
        "latest_order_date": latest_order_date,
        "total_orders": total_orders,
        "total_amount": total_amount,
        "recency_days": recency_days
    })

# === CREATE DATAFRAME ===
df = pd.DataFrame(data_list)

# === SAFE QCUT FUNCTION ===

def safe_qcut(series, q, labels, reverse=False):
    try:
        scores = pd.qcut(series, q=q, labels=labels, duplicates='drop')
        if reverse:
            # flip 5→1, 4→2, etc.
            scores = scores.cat.rename_categories(lambda x: 6 - int(x))
        return scores.astype(int)
    except ValueError:
        return pd.Series([1] * len(series))  # fallback score if qcut fails


# === R Score Function === 

def get_r_score(days_since_last_order: int) -> int:
    if days_since_last_order <= 7:
        return 5  # Reordered very quickly (frequent buyer)
    elif days_since_last_order <= 30:
        return 4  # On-time repurchase
    elif days_since_last_order <= 60:
        return 3  # Slightly overdue
    elif days_since_last_order <= 90:
        return 2  # Significantly overdue
    else:
        return 1  # Very delayed (likely churned)


# === APPLY SCORES ===
df['r_score'] = df['recency_days'].apply(get_r_score)
df['f_score'] = safe_qcut(df['total_orders'], 5, labels=[1, 2, 3, 4, 5])
df['m_score'] = safe_qcut(df['total_amount'], 5, labels=[1, 2, 3, 4, 5])

# === SEGMENTATION ===
df['segment'] = df.apply(classify_segment, axis=1)


# === UPDATE PROFILES + HISTORY ===
rfm_history = []
now_str = datetime.now().isoformat()

for cust in profiles:
    cid = cust.get("_id")
    row = df[df['_id'] == cid].iloc[0]

    cust['rfm'] = {
        "latest_order_date": row['latest_order_date'].strftime("%Y-%m-%d %H:%M:%S"),
        "total_amount": row['total_amount'],
        "total_orders": int(row['total_orders']),
        # "r_score": int(row['r_score'].item()),
        # "f_score": int(row['f_score'].item()),
        # "m_score": int(row['m_score'].item()),
        "segment": row['segment'],
        "snapshot_date": snapshot_date.isoformat()
    }

    rfm_history.append({
        "_id": str(uuid.uuid4()),
        "customer_id": cid,
        "snapshot_date": snapshot_date.isoformat(),
        "r_score": int(row['r_score'].item()),
        "f_score": int(row['f_score'].item()),
        "m_score": int(row['m_score'].item()),
        "segment": row['segment'],
        "created_date": now_str,
        "modified_date": now_str
    })

# === SAVE OUTPUTS ===
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(profiles, f, ensure_ascii=False, indent=2)

with open(HISTORY_FILE, "w", encoding="utf-8") as f:
    json.dump(rfm_history, f, ensure_ascii=False, indent=2)

print("✅ RFM segmentation and history saved.")
