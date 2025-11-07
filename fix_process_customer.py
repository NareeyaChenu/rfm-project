#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
process_customer.py — deduplicate and unify customer profiles from order data.
"""
import argparse
import json
import re
from collections import Counter
from uuid import uuid5, NAMESPACE_URL
from difflib import SequenceMatcher

NOISE_KEYWORDS = {"shopee", "line shopping", "international", "ส่งต่างประเทศ"}
PROVIDER_ID = "9d68882715c24e71942e0a9d020fe963"

def normalize_phone(phone: str) -> str:
    if not phone or "*" in phone:
        return ""
    digits = re.sub(r"\D+", "", phone)
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("66"):
        digits = digits[2:]
    if digits.startswith("0"):
        digits = digits[1:]
    return digits

def to_e164_th(local_digits: str) -> str:
    return "+66" + local_digits if local_digits else ""

def normalize_email(email: str) -> str:
    email_norm = (email or "").strip().lower()
    return "" if email_norm in ("", "no@email.com") else email_norm

def _dedup_preserve(seq):
    seen, out = set(), []
    for x in seq:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

def phones_from_order(order):
    return _dedup_preserve([
        normalize_phone(p) for p in [
            order.get("phone", ""),
            order.get("shipping_phone", ""),
            (order.get("line_shopping_info") or {}).get("phoneNumber", "")
        ] if normalize_phone(p)
    ])

def emails_from_order(order):
    return _dedup_preserve([
        normalize_email(e) for e in [
            order.get("email", ""),
            order.get("shipping_email", ""),
            (order.get("line_shopping_info") or {}).get("email", "")
        ] if normalize_email(e)
    ])

def social_ids(order):
    return {s["social_id"] for s in (order.get("social") or []) if s.get("social_id")}

def shopee_user_id(order):
    s = order.get("shopee_info") or {}
    return str(s.get("shopee_user_id")) if s.get("shopee_user_id") else ""

def names_similar(name1: str, name2: str, threshold: float = 0.85) -> bool:
    if not name1 or not name2:
        return False
    
    # Normalize and remove common Thai and English prefixes
    def clean_name(name: str) -> str:
        name = name.strip().lower()
        # Remove common prefixes
        prefixes = [
            # Thai common prefixes
            r"^คุณ", r"^นาย", r"^นาง", r"^นางสาว", r"^น\.ส\.", r"^นส\.", r"^ด\.ช\.", r"^ด\.ญ\.",
            r"^เด็กชาย", r"^เด็กหญิง", r"^ว่าที่\s*ร\.", r"^พระ", r"^พระครู", r"^พระมหา",
            r"^หลวงพ่อ", r"^สมเด็จพระ", r"^หม่อม", r"^หม่อมราชวงศ์", r"^หม่อมหลวง",
            r"^ม\.ร\.ว\.", r"^ม\.ล\.",
            # Academic or military titles
            r"^ดร\.", r"^ศ\.", r"^รศ\.", r"^ผศ\.", r"^อ\.", r"^ร\.ต\.", r"^ร\.ต\.อ\.", r"^พ\.ต\.",
            r"^พ\.ต\.อ\.", r"^พล\.ต\.", r"^จ\.ส\.ต\.", r"^จ\.ส\.อ\.", r"^ส\.ต\.ต\.",
            # English prefixes
            r"^mr\.?", r"^mrs\.?", r"^miss", r"^ms\.?", r"^mx\.?", r"^dr\.?", r"^prof\.?",
            r"^rev\.?", r"^sir", r"^madam", r"^lady", r"^lt\.?", r"^capt\.?", r"^col\.?", r"^maj\.?", r"^gen\.?"
        ]

        for p in prefixes:
            name = re.sub(p, "", name, flags=re.IGNORECASE).strip()
        # Remove multiple spaces
        name = re.sub(r"\s+", " ", name)
        return name
    
    name1_clean = clean_name(name1)
    name2_clean = clean_name(name2)
    return SequenceMatcher(None, name1_clean.lower(), name2_clean.lower()).ratio() >= threshold

def addresses_similar(addr1: str, addr2: str, threshold: float = 0.85) -> bool:
    if not addr1 or not addr2:
        return False
    return SequenceMatcher(None, addr1.lower(), addr2.lower()).ratio() >= threshold

def build_address(order):
    parts = [
        order.get("shipping_address_1"),
        order.get("shipping_address_2"),
        order.get("shipping_subdistrict"),
        order.get("shipping_district"),
        order.get("shipping_province"),
        order.get("shipping_zipcode"),
    ]
    return ", ".join(p.strip() for p in parts if p and p.strip())

def build_names(order):
    names = []
    pairs = [
        (order.get("firstname"), order.get("lastname")),
        (order.get("shipping_firstname"), order.get("shipping_lastname")),
        ((order.get("lazada_info") or {}).get("customer_first_name"),
         (order.get("lazada_info") or {}).get("customer_last_name")),
        ((order.get("line_shopping_info") or {}).get("recipientName"), ""),
        ((order.get("shopee_info") or {}).get("shopee_user_name"), ""),
    ]
    for f, l in pairs:
        f, l = (f or "").strip(), (l or "").strip()
        name = " ".join(x for x in (f, l) if x)
        if name and "*" not in name and "-" not in name:
            names.append(name)
    return _dedup_preserve(names)

def tiered_match(a, b):
    if a.get("order_from") == 16 and b.get("order_from") == 16:
        return shopee_user_id(a) and shopee_user_id(a) == shopee_user_id(b)

    # Order from != 16
    if set(phones_from_order(a)) & set(phones_from_order(b)):
        return True

    wsis_a = {s.get("wsis_id") for s in (a.get("social") or []) if s.get("wsis_id")}
    wsis_b = {s.get("wsis_id") for s in (b.get("social") or []) if s.get("wsis_id")}
    if wsis_a & wsis_b:
        return True

    if social_ids(a) & social_ids(b):
        return True

    addr_a = build_address(a)
    addr_b = build_address(b)
    if addresses_similar(addr_a, addr_b):
        return True

    for n1 in build_names(a):
        for n2 in build_names(b):
            if names_similar(n1, n2):
                return True

    return False

def cluster_orders(orders):
    clusters = []
    for idx, order in enumerate(orders):
        print(f"Processing order index {idx}: {order.get('order_id') if isinstance(order, dict) else order}")
        for cluster_idx, cluster in enumerate(clusters):
            if any(tiered_match(order, other) for other in cluster):
                print(f"  → Found match in cluster {cluster_idx}")
                cluster.append(order)
                break
        else:
            print(f"  → No match found, creating new cluster {len(clusters)}")
            clusters.append([order])
    return clusters

def choose_best_name(names):
    if not names: return ""
    cnt = Counter(names)
    maxc = cnt.most_common(1)[0][1]
    cands = [n for n, c in cnt.items() if c == maxc]
    def rank(n): return (int(any(k in n.lower() for k in NOISE_KEYWORDS)), len(n), n)
    return sorted(cands, key=rank)[0]

def freq_choice(values):
    if not values: return ""
    cnt = Counter(values)
    maxc = cnt.most_common(1)[0][1]
    cands = [v for v, c in cnt.items() if c == maxc]
    return sorted(cands, key=lambda v: (-len(v), v))[0]

def derive_customer_id(cluster):
    for o in cluster:
        for k in ("member_id", "extern_member_id"):
            if o.get(k): return str(o[k])
        for s in (o.get("social") or []):
            if s.get("wsis_id"): return str(s["wsis_id"])
    parts = []
    for o in cluster:
        parts.extend(phones_from_order(o))
        parts.extend(emails_from_order(o))
    return str(uuid5(NAMESPACE_URL, "|".join(sorted(parts)) or "orderids:" + "|".join(str(o["order_id"]) for o in cluster)))

def cluster_to_profile(cluster):
    customer_id = derive_customer_id(cluster)
    orders_list = [{
        "order_id": o.get("order_id"),
        "order_from": o.get("order_from"),
        "order_date": o.get("created_date"),
        "products": [{"product_id": p.get("product_id"), "product_name": p.get("name"), "sku": p.get("sku")} for p in (o.get("products") or [])],
        "grand_total": float(o.get("grand_total", 0) or 0.0),
    } for o in cluster if o.get("order_id")]

    unique_orders = []
    seen_ids = set()
    for order in orders_list:
        order_id = order["order_id"]
        if order_id not in seen_ids:
            seen_ids.add(order_id)
            unique_orders.append(order)
        else:
            print(f"⚠️ Duplicate order_id {order_id} removed")

    phone_digits = [p for o in cluster for p in phones_from_order(o)]
    ph_counts = Counter(phone_digits)
    phone_sorted = [d for d, _ in ph_counts.most_common()]
    phone_numbers = [{"phone_number": to_e164_th(d), "is_primary": i == 0} for i, d in enumerate(phone_sorted)]

    e_all = [e for o in cluster for e in emails_from_order(o)]
    e_counts = Counter(e_all)
    email_sorted = [e for e, _ in e_counts.most_common()]
    emails_out = [{"email": e, "is_primary": i == 0} for i, e in enumerate(email_sorted)]

    names = [n for o in cluster for n in build_names(o)]
    addrs = [build_address(o) for o in cluster if build_address(o)]
    name = choose_best_name(names)
    address = freq_choice(addrs)

    sources = []
    for o in cluster:
        for s in (o.get("social") or []):
            sources.append({
                "channel_id": o.get("channel_id"),
                "platform": s.get("platform"),
                "channel_name": s.get("channel_name"),
                "wsis_id": s.get("wsis_id"),
                "social_name": s.get("social_name"),
                "social_id": str(s["social_id"]) if s.get("social_id") else None,
            })
        sh = o.get("shopee_info") or {}
        if sh.get("shopee_user_id"):
            sources.append({
                "channel_id": o.get("channel_id"),
                "platform": "SHOPEE",
                "channel_name": sh.get("shopee_user_name"),
                "wsis_id": None,
                "social_id": str(sh["shopee_user_id"])
            })
        if o.get("order_from") == 12:
            laz = o.get("lazada_info") or {}
            sources.append({
                "channel_id": o.get("channel_id"),
                "platform": "LAZADA",
                "channel_name": laz.get("lazada_user_name"),
                "wsis_id": None,
                "social_id": str(laz.get("lazada_user_id"))
            })
        if o.get("order_from") == 21:
            line = o.get("line_info") or {}
            sources.append({
                "channel_id": o.get("channel_id"),
                "platform": "LINE SHOPPING",
                "channel_name": line.get("line_user_name"),
                "wsis_id": None,
                "social_id": str(line.get("line_user_id"))
            })

    seen = set()
    unique_sources = []
    for s in sources:
        key = (s.get("platform"), s.get("social_id"))
        if key not in seen:
            seen.add(key)
            unique_sources.append(s)

    tags = _dedup_preserve([tag for o in cluster for tag in o.get("tags", [])])
    notes, note_ids = [], set()
    for o in cluster:
        for note in o.get("notes", []):
            if note.get("note_id") not in note_ids:
                n = dict(note)
                n.pop("note_id", None)
                notes.append(n)
                note_ids.add(note.get("note_id"))

    structured_addrs = [{
        "line1": (o.get("shipping_address_1") or "").strip(),
        "line2": (o.get("shipping_address_2") or "").strip(),
        "subdistrict": (o.get("shipping_subdistrict") or "").strip(),
        "district": (o.get("shipping_district") or "").strip(),
        "province": (o.get("shipping_province") or "").strip(),
        "zipcode": (o.get("shipping_zipcode") or "").strip(),
        "full": build_address(o)
    } for o in cluster]


    # Remove duplicates by checking only specific fields
    unique_addresses = []
    seen = set()

    for addr in structured_addrs:
        key = (
            addr.get("line1", "").strip(),
            addr.get("line2", "").strip(),
            addr.get("subdistrict", "").strip(),
            addr.get("district", "").strip(),
            addr.get("province", "").strip(),
            addr.get("zipcode", "").strip(),
        )
        if key not in seen:
            seen.add(key)
            unique_addresses.append(addr)

    return {
        "_id": customer_id,
        "full_name": name,
        "sources": unique_sources,
        "orders": unique_orders,
        "addresses": unique_addresses,
        "phones": [p["phone_number"] for p in phone_numbers],
        "emails": [p["email"] for p in emails_out],
        "tags": tags,
        "provider_id": PROVIDER_ID,
        "notes": notes
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="all_orders.json")
    ap.add_argument("--output", default="fix_crm_customer_profiles.json")
    args = ap.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        orders = json.load(f)
    


    print(f"total order to process {len(orders)}")

    clusters = cluster_orders(orders)
    profiles = [cluster_to_profile(c) for c in clusters]

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(profiles, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
