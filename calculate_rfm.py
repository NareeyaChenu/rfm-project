import json
from datetime import datetime
import pandas as pd

# --- Load JSON file ---
with open("crm_data.json", "r", encoding="utf-8") as f:
    data = json.load(f)

orders = pd.DataFrame(data["crm_orders"])
customers = pd.DataFrame(data["crm_customers"])

# --- Preprocess order_date into datetime ---
orders["order_date"] = pd.to_datetime(orders["order_date"])

# --- Set reference date for recency ---
reference_date = orders["order_date"].max() + pd.Timedelta(days=1)

# --- Group orders by customer to compute R, F, M ---
rfm = orders.groupby("customer_id").agg({
    "order_date": [
        lambda x: (reference_date - x.max()).days,  # recency in days
        "count"                                     # frequency
    ],
    "total_amount": "sum"                           # monetary
})

rfm.columns = ["recency_days", "frequency", "monetary_sum"]
rfm = rfm.reset_index()

# --- Create buckets (e.g. score 1-5) ---
num_buckets = 5

# Recency (smaller = better)
rfm["recency_score"] = pd.qcut(rfm["recency_days"], q=num_buckets, labels=False, duplicates="drop")
rfm["recency_score"] = (num_buckets - rfm["recency_score"]).astype(int)

# Frequency (higher = better)
rfm["frequency_score"] = pd.qcut(
    rfm["frequency"], q=num_buckets, labels=False, duplicates="drop"
).astype(int)

# Monetary (higher = better)
rfm["monetary_score"] = pd.qcut(
    rfm["monetary_sum"], q=num_buckets, labels=False, duplicates="drop"
).astype(int)

# --- Select only the needed columns ---
rfm_result = rfm[["customer_id", "recency_score", "frequency_score", "monetary_score"]]

# --- Save as JSON ---
rfm_result.to_json("crm_rfm_scores.json", orient="records", indent=2, force_ascii=False)

print("Saved JSON file: crm_rfm_scores.json")
print(rfm_result.head())
