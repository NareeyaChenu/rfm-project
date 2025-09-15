from rapidfuzz import fuzz
from collections import defaultdict
import itertools
import random
from datetime import datetime, timedelta
import json

def normalize_name(first, last):
    """Normalize full name for matching."""
    return f"{first.strip().lower()} {last.strip().lower()}"


# region cal fuzzy logic
def fuzzy_match_score(name1, name2, address1, address2):
    """Fuzzy match score using RapidFuzz WRatio."""
    name_score = fuzz.WRatio(name1, name2) / 100
    addr_score = fuzz.WRatio(address1.lower(), address2.lower()) / 100
    return (name_score + addr_score) / 2

# region identify the customer
def identify_customers(customers, fuzzy_threshold=0.85):
    unified = []
    visited = set()

    # --- Step 1: Index by phone number (exact match) ---
    phone_index = defaultdict(list)
    for idx, cust in enumerate(customers):
        if cust["phone_number"]:
            phone_index[cust["phone_number"]].append(idx)

    for phone, indices in phone_index.items():
        if len(indices) > 1:
            cluster = [customers[i] for i in indices]
            for i in indices:
                visited.add(i)
            unified.append(cluster)

    # --- Step 2: Fuzzy match on name + address (all customers) ---
    for i, j in itertools.combinations(range(len(customers)), 2):
        if i in visited or j in visited:
            continue
        c1, c2 = customers[i], customers[j]
        score = fuzzy_match_score(
            normalize_name(c1["first_name"], c1["last_name"]),
            normalize_name(c2["first_name"], c2["last_name"]),
            c1["address"], c2["address"]
        )
        if score >= fuzzy_threshold:
            unified.append([c1, c2])
            visited.add(i)
            visited.add(j)

    # --- Step 3: Remaining unique customers ---
    for i, cust in enumerate(customers):
        if i not in visited:
            unified.append([cust])

    return unified


# region mock data
# --- Mock Data (can scale to 300K+) ---
mock_orders = [
    # ✅ Clear Match Cases

    # Same phone, same name, same address
    {
        "order_id": "ORD1001",
        "first_name": "Nareeya",
        "last_name": "Chenu",
        "address": "123 Main Road, Bangkok",
        "phone_number": "0891234567",
        "psid": "fb_1001",
        "channel_name": "Messenger",
        "igsid": None,
        "platform": "Facebook",
        "line_id": None,
        "email": "nareeya@example.com"
    },
    {
        "order_id": "ORD1002",
        "first_name": "Nareeya",
        "last_name": "Chenu",
        "address": "123 Main Road, Bangkok",
        "phone_number": "0891234567",
        "psid": None,
        "channel_name": "LINE OA",
        "igsid": None,
        "platform": "LINE",
        "line_id": "line_1001",
        "email": None
    },

    # Same phone, slightly different name spelling
    {
        "order_id": "ORD2001",
        "first_name": "Somchai",
        "last_name": "Wong",
        "address": "55 Sukhumvit Rd, Bangkok",
        "phone_number": "0811111111",
        "psid": "fb_2001",
        "channel_name": "Messenger",
        "igsid": None,
        "platform": "Facebook",
        "line_id": None,
        "email": None
    },
    {
        "order_id": "ORD2002",
        "first_name": "Somchay",  # typo
        "last_name": "Wong",
        "address": "55 Sukhumvit Rd, Bangkok",
        "phone_number": "0811111111",
        "psid": None,
        "channel_name": "Instagram DM",
        "igsid": "ig_2001",
        "platform": "Instagram",
        "line_id": None,
        "email": None
    },

    # Same name and address, phone number missing
    {
        "order_id": "ORD3001",
        "first_name": "Alice",
        "last_name": "Brown",
        "address": "789 Oak Street, Chiang Mai",
        "phone_number": None,   # missing
        "psid": None,
        "channel_name": "Instagram DM",
        "igsid": "ig_3001",
        "platform": "Instagram",
        "line_id": None,
        "email": "alice@example.com"
    },
    {
        "order_id": "ORD3002",
        "first_name": "Alice",
        "last_name": "Brown",
        "address": "789 Oak Street, Chiang Mai",
        "phone_number": None,
        "psid": None,
        "channel_name": "LINE OA",
        "igsid": None,
        "platform": "LINE",
        "line_id": "line_3002",
        "email": "alice@example.com"
    },

    # Same email across platforms
    {
        "order_id": "ORD4001",
        "first_name": "David",
        "last_name": "Lee",
        "address": "222 River View, Chiang Rai",
        "phone_number": "0823334444",
        "psid": "fb_4001",
        "channel_name": "Messenger",
        "igsid": None,
        "platform": "Facebook",
        "line_id": None,
        "email": "david.lee@example.com"
    },
    {
        "order_id": "ORD4002",
        "first_name": "D. Lee",
        "last_name": "",
        "address": "222 River View, Chiang Rai",
        "phone_number": None,
        "psid": None,
        "channel_name": "Instagram DM",
        "igsid": "ig_4002",
        "platform": "Instagram",
        "line_id": None,
        "email": "david.lee@example.com"  # email is same key
    },

    # Same LINE ID reused
    {
        "order_id": "ORD5001",
        "first_name": "Ploy",
        "last_name": "Suwan",
        "address": "99 Rama 9 Rd, Bangkok",
        "phone_number": "0839999999",
        "psid": None,
        "channel_name": "LINE OA",
        "igsid": None,
        "platform": "LINE",
        "line_id": "line_5001",
        "email": None
    },
    {
        "order_id": "ORD5002",
        "first_name": "Ploy",
        "last_name": "S.",
        "address": "99 Rama IX Road, Bangkok",
        "phone_number": None,
        "psid": None,
        "channel_name": "LINE OA",
        "igsid": None,
        "platform": "LINE",
        "line_id": "line_5001",  # same line ID
        "email": None
    },

    # ⚠️ Fuzzy Match Cases

    # Name typo/nickname
    {
        "order_id": "ORD6001",
        "first_name": "Nareeya",
        "last_name": "Tansakul",
        "address": "12 Charoen Krung Rd, Bangkok",
        "phone_number": "0841112222",
        "psid": "fb_6001",
        "channel_name": "Messenger",
        "igsid": None,
        "platform": "Facebook",
        "line_id": None,
        "email": None
    },
    {
        "order_id": "ORD6002",
        "first_name": "Nareeya T.",  # short form
        "last_name": "",
        "address": "12 Charoen Krung Road, Bangkok",
        "phone_number": "0841112222",
        "psid": None,
        "channel_name": "LINE OA",
        "igsid": None,
        "platform": "LINE",
        "line_id": "line_6002",
        "email": None
    },

    # Phone number format variation
    {
        "order_id": "ORD7001",
        "first_name": "Arthit",
        "last_name": "Chan",
        "address": "77 Silom Rd, Bangkok",
        "phone_number": "0891234567",
        "psid": "fb_7001",
        "channel_name": "Messenger",
        "igsid": None,
        "platform": "Facebook",
        "line_id": None,
        "email": None
    },
    {
        "order_id": "ORD7002",
        "first_name": "Arthit",
        "last_name": "Chan",
        "address": "77 Silom Road, Bangkok",
        "phone_number": "089-123-4567",  # same but formatted
        "psid": None,
        "channel_name": "Instagram DM",
        "igsid": "ig_7002",
        "platform": "Instagram",
        "line_id": None,
        "email": None
    },

    # ❌ False Positive Risks

    # Common names with similar addresses
    {
        "order_id": "ORD8001",
        "first_name": "Somchai",
        "last_name": "S.",
        "address": "1 Rama 2 Rd, Bangkok",
        "phone_number": "0861111111",
        "psid": None,
        "channel_name": "LINE OA",
        "igsid": None,
        "platform": "LINE",
        "line_id": "line_8001",
        "email": None
    },
    {
        "order_id": "ORD8002",
        "first_name": "Somchai",
        "last_name": "S.",
        "address": "2 Rama 2 Rd, Bangkok",  # very close
        "phone_number": "0862222222",
        "psid": None,
        "channel_name": "LINE OA",
        "igsid": None,
        "platform": "LINE",
        "line_id": "line_8002",
        "email": None
    },

    # Shared phone number (family)
    {
        "order_id": "ORD9001",
        "first_name": "Mali",
        "last_name": "Thong",
        "address": "88 Rama 4 Rd, Bangkok",
        "phone_number": "0855555555",
        "psid": "fb_9001",
        "channel_name": "Messenger",
        "igsid": None,
        "platform": "Facebook",
        "line_id": None,
        "email": None
    },
    {
        "order_id": "ORD9002",
        "first_name": "Manop",
        "last_name": "Thong",
        "address": "88 Rama 4 Rd, Bangkok",
        "phone_number": "0855555555",  # shared family phone
        "psid": None,
        "channel_name": "LINE OA",
        "igsid": None,
        "platform": "LINE",
        "line_id": "line_9002",
        "email": None
    },

    # 🧩 Edge Cases

    # Changed phone but same name/address
    {
        "order_id": "ORD10001",
        "first_name": "Anan",
        "last_name": "Preecha",
        "address": "11 Sathorn Rd, Bangkok",
        "phone_number": "0810000000",
        "psid": "fb_10001",
        "channel_name": "Messenger",
        "igsid": None,
        "platform": "Facebook",
        "line_id": None,
        "email": None
    },
    {
        "order_id": "ORD10002",
        "first_name": "Anan",
        "last_name": "Preecha",
        "address": "11 Sathorn Road, Bangkok",
        "phone_number": "0820000000",  # changed
        "psid": None,
        "channel_name": "LINE OA",
        "igsid": None,
        "platform": "LINE",
        "line_id": "line_10002",
        "email": None
    },

    # Nickname vs full name
    {
        "order_id": "ORD11001",
        "first_name": "Pim",
        "last_name": "K.",
        "address": "5 Ladprao Rd, Bangkok",
        "phone_number": "0871111111",
        "psid": "fb_11001",
        "channel_name": "Messenger",
        "igsid": None,
        "platform": "Facebook",
        "line_id": None,
        "email": None
    },
    {
        "order_id": "ORD11002",
        "first_name": "Pimchanok",
        "last_name": "Kunlaya",
        "address": "5 Ladprao Road, Bangkok",
        "phone_number": "0871111111",
        "psid": None,
        "channel_name": "Instagram DM",
        "igsid": "ig_11002",
        "platform": "Instagram",
        "line_id": None,
        "email": None
    }
]


# region call func identify_customers
# --- Run Identification ---
clusters = identify_customers(mock_orders)


# --- Display Results ---
# for idx, cluster in enumerate(clusters, start=1):
#     print(f"\nUnified Customer {idx}:")
#     for c in cluster:
#         print(
#             f" - {c['first_name']} {c['last_name']} ({c['platform']}, {c['channel_name']})")



# --- Generate schema-based JSON ---
crm_customers = []
crm_orders = []

for cust_id, cluster in enumerate(clusters, start=1):
    customer_id = f"CUST{cust_id:04d}"
    channels = list({c["channel_name"] for c in cluster})
    tags = ["unified"] if len(cluster) > 1 else ["single"]

    # pick first record as main profile
    main = cluster[0]

    crm_customers.append({
        "customer_id": customer_id,
        "name": f"{main['first_name']} {main['last_name']}".strip(),
        "phone": main.get("phone_number"),
        "address": main.get("address"),
        "tags": tags,
        "channels": channels
    })

    # add orders
    for order in cluster:
        crm_orders.append({
            "order_id": order["order_id"],
            "customer_id": customer_id,
            "platform": order["platform"],
            "total_amount": round(random.uniform(100, 2000), 2),  # mock amount
            "order_date": (datetime(2025, 1, 1) + timedelta(days=random.randint(0, 200))).strftime("%Y-%m-%d")
        })

# --- Final JSON with schema ---
data_schema = {
    "crm_customers": crm_customers,
    "crm_orders": crm_orders
}

# --- Save to file ---
with open("crm_data.json", "w", encoding="utf-8") as f:
    json.dump(data_schema, f, indent=4, ensure_ascii=False)

print("✅ CRM data saved to crm_data.json")


