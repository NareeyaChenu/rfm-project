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

def strong_identifier_match(a, b):
    return (
        set(phones_from_order(a)) & set(phones_from_order(b))
        or set(emails_from_order(a)) & set(emails_from_order(b))
        or social_ids(a) & social_ids(b)
        or (shopee_user_id(a) and shopee_user_id(a) == shopee_user_id(b))
        or (a.get("member_id") and a.get("member_id") == b.get("member_id"))
        or (a.get("extern_member_id") and a.get("extern_member_id") == b.get("extern_member_id"))
    )

def cluster_orders(orders):
    clusters = []
    for order in orders:
        for cluster in clusters:
            if any(strong_identifier_match(order, other) for other in cluster):
                cluster.append(order)
                break
        else:
            clusters.append([order])
    # second pass merge
    merged = True
    while merged:
        merged = False
        for i, a in enumerate(clusters):
            for j in range(i + 1, len(clusters)):
                b = clusters[j]
                if any(strong_identifier_match(x, y) for x in a for y in b):
                    a.extend(b)
                    clusters.pop(j)
                    merged = True
                    break
            if merged:
                break
    return clusters

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
    unique_sources = []
    seen = set()
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

    return {
        "_id": customer_id,
        "full_name": name,
        "sources": unique_sources,
        "orders": orders_list,
        "addresses": structured_addrs,
        "phones": [p["phone_number"] for p in phone_numbers],
        "emails": [p["email"] for p in emails_out],
        "tags": tags,
        "provider_id": PROVIDER_ID,
        "notes": notes
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="test-order.json")
    ap.add_argument("--output", default="fix_crm_customer_profiles.json")
    args = ap.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        orders = json.load(f)

    clusters = cluster_orders(orders)
    profiles = [cluster_to_profile(c) for c in clusters]

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(profiles, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
