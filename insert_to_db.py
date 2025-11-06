import json
from pymongo import MongoClient
from datetime import datetime

# ===== CONFIG =====
JSON_FILE = "crm_customer_profiles_updated.json"
MONGO_URI = "mongodb://192.168.49.2:30017/?authSource=admin"   # change if needed
DB_NAME = "report_db"
COLLECTION_NAME = "crm_customer_profiles"
# ==================

# ---- Connect to MongoDB ----
# ---- Helper function: convert date strings ----


def parse_date(date_str):
    """Try converting string to datetime (UTC)."""
    if not date_str:
        return None
    try:
        # Try multiple formats (customize as needed)
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str.split(".")[0], fmt)
            except ValueError:
                continue
        return None
    except Exception:
        return None


# ---- Connect to MongoDB ----
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# ---- Load JSON file ----
with open(JSON_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

# ---- Convert order_date fields ----
for customer in data:
    for order in customer.get("orders", []):
        order_date = order.get("order_date")
        if isinstance(order_date, str):
            order["order_date"] = parse_date(order_date)

    customer["rfm"]["latest_order_date"] = parse_date(customer["rfm"]["latest_order_date"])
    customer["rfm"]["snapshot_date"] = parse_date(customer["rfm"]["snapshot_date"])
    now = datetime.utcnow()
    customer["created_date"] = now
    customer["modified_date"] = now

# ---- Insert into MongoDB ----
result = collection.insert_many(data)
print(
    f"✅ Inserted {len(result.inserted_ids)} customer profiles into MongoDB collection '{COLLECTION_NAME}'.")
