import json
import pandas as pd

# --- Load the saved RFM JSON file ---
with open("crm_rfm_scores.json", "r", encoding="utf-8") as f:
    rfm_data = json.load(f)

rfm = pd.DataFrame(rfm_data)

# --- Define segmentation logic ---
def segment_customer(row):
    r, f, m = row["recency_score"], row["frequency_score"], row["monetary_score"]

    if r >= 4 and f >= 4 and m >= 4:
        return "Champions"
    elif r >= 3 and f >= 3:
        return "Loyal Customers"
    elif r >= 4 and f <= 2:
        return "Recent Customers"
    elif f >= 4:
        return "Frequent Buyers"
    elif m >= 4:
        return "Big Spenders"
    elif r <= 2 and f <= 2 and m <= 2:
        return "At Risk"
    else:
        return "Others"

# --- Apply segmentation ---
rfm["segment"] = rfm.apply(segment_customer, axis=1)

# --- Display segmentation result ---
print(rfm[["customer_id", "recency_score", "frequency_score", "monetary_score", "segment"]])

# --- Save segmentation back to JSON if needed ---
rfm[["customer_id", "recency_score", "frequency_score", "monetary_score", "segment"]].to_json(
    "crm_rfm_segments.json", orient="records", indent=4
)
