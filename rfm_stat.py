import json
import pandas as pd
from datetime import datetime

# Load customer profiles (already deduplicated)
with open("crm_customer_profiles.json", "r", encoding="utf-8") as f:
    customers = json.load(f)

rows = []
for c in customers:
    for o in c["orders"]:
        rows.append({
            "customer_id": c["customer_id"],
            "order_id": o["order_id"],
            "grand_total": float(o["grand_total"]),
            "created_date": datetime.strptime(o["created_date"], "%Y-%m-%d %H:%M:%S")
        })

df = pd.DataFrame(rows)

# --- RFM calculations ---
today = datetime.now()

# Recency (days since last order)
recency = df.groupby("customer_id")["created_date"].max().reset_index()
recency["recency_days"] = (today - recency["created_date"]).dt.days

# Frequency (number of orders)
frequency = df.groupby("customer_id")["order_id"].count().reset_index(name="frequency")

# Monetary (sum of grand_total)
monetary = df.groupby("customer_id")["grand_total"].sum().reset_index(name="monetary")

# Merge all
rfm = recency.merge(frequency, on="customer_id").merge(monetary, on="customer_id")

# Quantile scoring (1–5)
rfm["R_score"] = pd.qcut(rfm["recency_days"], 5, labels=[5,4,3,2,1]).astype(int)
rfm["F_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1,2,3,4,5]).astype(int)
rfm["M_score"] = pd.qcut(rfm["monetary"], 5, labels=[1,2,3,4,5]).astype(int)

# Combine into RFM code
rfm["RFM"] = rfm["R_score"].astype(str) + rfm["F_score"].astype(str) + rfm["M_score"].astype(str)

print(rfm.head())
