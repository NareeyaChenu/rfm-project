import json
from datetime import datetime, timezone
import uuid

INPUT_FILE = "crm_customer_profiles.json"
RFM_HISTORY_FILE = "rfm_history.json"

# --- RFM scoring thresholds ---
def get_r_score(days_since_last_order: float) -> int:
    if days_since_last_order <= 7: return 5
    elif days_since_last_order <= 30: return 4
    elif days_since_last_order <= 90: return 3
    elif days_since_last_order <= 180: return 2
    else: return 1

def get_f_score(total_orders: int) -> int:
    if total_orders >= 10: return 5
    elif total_orders >= 5: return 4
    elif total_orders >= 3: return 3
    elif total_orders >= 2: return 2
    else: return 1

def get_m_score(total_amount: float) -> int:
    if total_amount >= 10000: return 5
    elif total_amount >= 5000: return 4
    elif total_amount >= 2000: return 3
    elif total_amount >= 1000: return 2
    else: return 1


# --- BentoWeb Segmentation Logic ---
def get_segment(r, f, m):
    if r >= 4 and f >= 4 and m >= 4:
        return "Champion"
    elif r >= 3 and f >= 4:
        return "Loyal Customers"
    elif r >= 4 and 2 <= f <= 3:
        return "Potential Loyalist"
    elif r >= 4 and f <= 2 and m <= 2:
        return "Promising"
    elif r == 5 and f == 1:
        return "New Customers"
    elif r == 3 and 2 <= f <= 3:
        return "Need Attention"
    elif r in (2,3) and f >= 2:
        return "About to Sleep"
    elif r <= 2 and f >= 4 and m >= 4:
        return "Can't Lose"
    elif r <= 2 and f >= 3:
        return "At Risk"
    elif r in (1,2) and f in (1,2):
        return "Hibernating"
    elif r == 1 and f == 1:
        return "Lost"
    else:
        return "Regular Customer"


def calculate_rfm():
    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"❌ File not found: {INPUT_FILE}")
        return

    now = datetime.now(timezone.utc)
    rfm_history = []

    if isinstance(data, dict):
        data = [data]

    for customer in data:
        orders = customer.get("orders", [])
        if not orders:
            continue

        order_dates, total_amount = [], 0.0
        for order in orders:
            try:
                order_date = datetime.strptime(order["order_date"], "%Y-%m-%d %H:%M:%S")
            except Exception:
                try:
                    order_date = datetime.fromisoformat(order["order_date"].replace("Z", "+00:00"))
                except Exception:
                    continue
            order_dates.append(order_date)
            total_amount += float(order.get("grand_total", 0.0))

        if not order_dates:
            continue

        latest_order = max(order_dates)
        days_since_last = (now - latest_order.replace(tzinfo=timezone.utc)).days
        total_orders = len(orders)

        r, f, m = (
            get_r_score(days_since_last),
            get_f_score(total_orders),
            get_m_score(total_amount)
        )
        segment = get_segment(r, f, m)

        customer["rfm"] = {
            "latest_order_date": latest_order.strftime("%Y-%m-%d %H:%M:%S"),
            "total_amount": round(total_amount, 2),
            "total_orders": total_orders,
            "r_score": r,
            "f_score": f,
            "m_score": m,
            "segment": segment,
            "snapshot_date": now.strftime("%Y-%m-%d %H:%M:%S")
        }

        # create rfm_history record
        history_entry = {
            "_id": str(uuid.uuid4()),
            "customer_id": customer.get("_id"),
            "snapshot_date": now.strftime("%Y-%m-%d %H:%M:%S"),
            "r_score": r,
            "f_score": f,
            "m_score": m,
            "segment": segment,
            "created_date": now.strftime("%Y-%m-%d %H:%M:%S"),
            "modified_date": now.strftime("%Y-%m-%d %H:%M:%S")
        }
        rfm_history.append(history_entry)

    with open(INPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    

    with open(RFM_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(rfm_history, f, ensure_ascii=False, indent=2)

    print(f"✅ BentoWeb RFM segmentation completed and saved to {INPUT_FILE}")
    print(f"📜 RFM history saved to {RFM_HISTORY_FILE}")



if __name__ == "__main__":
    calculate_rfm()
