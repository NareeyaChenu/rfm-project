#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Identify & deduplicate customers from order rows using fuzzy logic.

Signals used:
- Phone (exact, normalized)
- Email (exact, normalized)
- Social ID (exact)
- Shopee userId (exact)
- Full name (fuzzy)
- Shipping address (fuzzy)

Output schema (per profile):
{
  "customer_id": "...",
  "name": "First Last",
  "address": "Full shipping address",
  "sources": [
    { "channel_id": "...", "platform": "...", "channel_name": "...",
        "wsis_id": "...", "platform_id": "..." }
  ],
  "orders": [{ "order_id": 123, "grand_total": "123.45" }],
  "phone_numbers": [{ "phone_number": "+66XXXXXXXXX", "is_primary": true }],
  "emails": [{ "email": "user@example.com", "is_primary": true }]
}

Usage:
    python identify_customers.py \
        --input orders_v2.json \
        --output crm_customer_profiles.json \
        --name-strong 90 --addr-strong 88 --score-threshold 5
"""

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from uuid import uuid5, NAMESPACE_URL

try:
    from rapidfuzz import fuzz  # preferred for speed/quality
except Exception:
    fuzz = None

# --- Normalization helpers ----------------------------------------------------

NOISE_KEYWORDS = {"shopee", "line shopping", "international", "ส่งต่างประเทศ"}
PROVIDER_ID = "9d68882715c24e71942e0a9d020fe963"


def normalize_phone(phone: str) -> str:
    """Return Thai local digits (no +66/0), ignore masked values."""
    if not phone:
        return ""
    if "*" in phone:
        return ""  # masked -> unusable
    digits = re.sub(r"\D+", "", phone)
    if not digits:
        return ""
    # handle international prefix first (e.g. 00..66..)
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("66"):
        digits = digits[2:]
    if digits.startswith("0"):
        digits = digits[1:]
    return digits


def to_e164_th(local_digits: str) -> str:
    """Convert Thai local digits to +66 E.164-like for output."""
    return "+66" + local_digits if local_digits else ""


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def _dedup_preserve(seq):
    seen, out = set(), []
    for x in seq:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

# --- Field extraction ---------------------------------------------------------


def build_names(order: dict) -> list[str]:
    """Collect plausible names from several sources; drop masked/placeholder forms."""
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
        f = (f or "").strip()
        l = (l or "").strip()
        name = " ".join(p for p in (f, l) if p)
        if not name:
            continue
        if any(token in name for token in ["*", "-"]):
            continue
        names.append(name)
    return _dedup_preserve(names)


def build_address(order: dict) -> str:
    parts = [
        order.get("shipping_address_1") or "",
        order.get("shipping_address_2") or "",
        order.get("shipping_subdistrict") or "",
        order.get("shipping_district") or "",
        order.get("shipping_province") or "",
        order.get("shipping_zipcode") or "",
    ]
    parts = [p.strip() for p in parts if p and p.strip()]
    return ", ".join(parts)


def phones_from_order(order: dict) -> list[str]:
    fields = [
        order.get("phone") or "",
        order.get("shipping_phone") or "",
        (order.get("line_shopping_info") or {}).get("phoneNumber") or "",
    ]
    return _dedup_preserve([normalize_phone(p) for p in fields if normalize_phone(p)])


def emails_from_order(order: dict) -> list[str]:
    fields = [
        order.get("email") or "",
        order.get("shipping_email") or "",
        (order.get("line_shopping_info") or {}).get("email") or "",
    ]
    return _dedup_preserve([normalize_email(e) for e in fields if normalize_email(e)])


def social_ids(order: dict) -> set[str]:
    ids = []
    for s in (order.get("social") or []):
        if s.get("social_id"):
            ids.append(s["social_id"])
    return set(ids)


def shopee_user_id(order: dict) -> str:
    s = order.get("shopee_info") or {}
    uid = s.get("shopee_user_id")
    return str(uid) if uid not in (None, "") else ""

# --- Similarity / matching ----------------------------------------------------


def ratio(a: str, b: str) -> int:
    if not a or not b:
        return 0
    if fuzz:
        return int(fuzz.WRatio(a, b))
    # Fallback to difflib if rapidfuzz is missing
    import difflib
    return int(round(difflib.SequenceMatcher(a=a.lower(), b=b.lower()).ratio() * 100))


def name_similarity(order_a: dict, order_b: dict) -> int:
    names_a, names_b = build_names(order_a), build_names(order_b)
    best = 0
    for na in names_a:
        for nb in names_b:
            best = max(best, ratio(na, nb))
    return best


def address_similarity(order_a: dict, order_b: dict) -> int:
    return ratio(build_address(order_a), build_address(order_b))


def strong_identifier_match(order_a: dict, order_b: dict) -> bool:
    # exact phone/email/social/shopee matches are strong
    if set(phones_from_order(order_a)) & set(phones_from_order(order_b)):
        return True
    if set(emails_from_order(order_a)) & set(emails_from_order(order_b)):
        return True
    if social_ids(order_a) & social_ids(order_b):
        return True
    sa, sb = shopee_user_id(order_a), shopee_user_id(order_b)
    if sa and sb and sa == sb:
        return True
    return False


def is_same_customer(order_a: dict,
                     order_b: dict,
                     name_strong: int = 90,
                     addr_strong: int = 88,
                     score_threshold: int = 5) -> bool:
    # 1) Fast path via strong identifiers
    if strong_identifier_match(order_a, order_b):
        return True

    # 2) Score by fuzzy name/address + weak signals (last-4 phone)
    score = 0
    ns = name_similarity(order_a, order_b)
    if ns >= name_strong:
        score += 3
    elif ns >= 85:
        score += 2

    ads = address_similarity(order_a, order_b)
    if ads >= addr_strong:
        score += 2
    elif ads >= 80:
        score += 1

    # weak: last-4 phone overlap
    last4_a = {p[-4:] for p in phones_from_order(order_a) if len(p) >= 4}
    last4_b = {p[-4:] for p in phones_from_order(order_b) if len(p) >= 4}
    if last4_a & last4_b:
        score += 1

    # strong combo (name+address) should always match
    if ns >= name_strong and ads >= addr_strong:
        return True

    return score >= score_threshold

# --- Clustering ---------------------------------------------------------------


def cluster_orders(orders, name_strong, addr_strong, score_threshold):
    clusters: list[list[dict]] = []
    for idx , order in enumerate(orders , 1):

        print(f"Process cluster order {idx}")
        placed = False
        for cluster in clusters:
            if any(
                is_same_customer(order, existing, name_strong,
                                 addr_strong, score_threshold)
                for existing in cluster
            ):
                cluster.append(order)
                placed = True
                break
        if not placed:
            clusters.append([order])
    return clusters

# --- Profile building ---------------------------------------------------------


def choose_best_name(names: list[str]) -> str:
    """Pick most frequent; on ties prefer cleaner names (no brand/noise), then shorter."""
    if not names:
        return ""
    cnt = Counter(names)
    maxc = cnt.most_common(1)[0][1]
    cands = [n for n, c in cnt.items() if c == maxc]

    def rank(n: str):
        noise = int(any(k in n.lower() for k in NOISE_KEYWORDS))
        return (noise, len(n), n)

    cands.sort(key=rank)
    return cands[0]


def freq_choice(values: list[str]) -> str:
    if not values:
        return ""
    cnt = Counter(values)
    maxc = cnt.most_common(1)[0][1]
    cands = [v for v, c in cnt.items() if c == maxc]
    # prefer longer address on tie (more complete)
    cands.sort(key=lambda v: (-len(v), v))
    return cands[0]


def derive_customer_id(cluster: list[dict]) -> str:
    """Prefer stable IDs if present; otherwise uuid5 over key attributes (stable across runs)."""
    for o in cluster:
        mid = o.get("member_id")
        if mid:
            return str(mid)
    for o in cluster:
        ext = o.get("extern_member_id")
        if ext:
            return str(ext)
    for o in cluster:
        for s in (o.get("social") or []):
            if s.get("wsis_id"):
                return str(s["wsis_id"])

    phones, emails, names, addrs = set(), set(), set(), set()
    for o in cluster:
        phones.update(phones_from_order(o))
        emails.update(emails_from_order(o))
        names.update(n.lower() for n in build_names(o))
        addr = build_address(o)
        if addr:
            addrs.add(addr)

    basis = "|".join(sorted(phones) + sorted(emails) +
                     sorted(names) + sorted(addrs))
    if not basis:
        basis = "orderids:" + "|".join(str(o["order_id"]) for o in cluster)
    return str(uuid5(NAMESPACE_URL, basis))


def cluster_to_profile(cluster: list[dict]) -> dict:
    customer_id = derive_customer_id(cluster)
    
    # Orders
    orders_list = [
        {
            "order_id": o.get("order_id"),
            "order_from": o.get("order_from"),
            "order_date": o.get("created_date"),
            "products": [
                {
                    "product_id": p.get("product_id"),
                    "product_name": p.get("name"),
                    "sku" : p.get("sku")
                }
                for p in (o.get("products") or [])
            ],
            "grand_total": float(o.get("grand_total", 0) or 0.0),
        }
        for o in cluster
        if o.get("order_id")  # include only valid orders
    ]

    # Phones
    phone_digits = []
    for o in cluster:
        phone_digits.extend(phones_from_order(o))
    ph_counts = Counter(phone_digits)
    phone_sorted = [d for d, _ in ph_counts.most_common()]
    phone_numbers = [{"phone_number": to_e164_th(d), "is_primary": i == 0} for i, d in enumerate(phone_sorted)]

    # Emails
    e_all = []
    for o in cluster:
        e_all.extend(emails_from_order(o))
    e_counts = Counter(e_all)
    email_sorted = [e for e, _ in e_counts.most_common()]
    emails_out = [{"email": e, "is_primary": i == 0} for i, e in enumerate(email_sorted)]

    # Name & Address
    names = []
    addrs = []
    for o in cluster:
        names.extend(build_names(o))
        addr = build_address(o)
        if addr:
            addrs.append(addr)
    name = choose_best_name(names)
    address = freq_choice(addrs)

    # Sources = Social + Shopee
    sources = []
    for idx, o in enumerate(cluster, 1):
        print(f"Processing cluster_to_profile {idx} of {len(cluster)}")
        for s in (o.get("social") or []):
            sources.append({
                "channel_id": o.get("channel_id"),
                "platform": s.get("platform"),
                "channel_name": s.get("channel_name"),
                "wsis_id": s.get("wsis_id"),
                "social_name": s.get("social_name"),
                "social_id": str(s["social_id"]) if s.get("social_id") is not None else None  # Social Id
            })
        sh = o.get("shopee_info") or {}
        if sh.get("shopee_user_id"):
            sources.append({
                "channel_id": o.get("channel_id"),
                "platform": "SHOPEE", 
                "channel_name": sh.get("shopee_user_name"),
                "wsis_id": None,
                "social_id": str(sh["shopee_user_id"]) if sh.get("shopee_user_id") is not None else None,  # Shopee userId
            })

        # 3️⃣ Lazada
        if o.get("order_from") == 12:
            laz = o.get("lazada_info") or {}
            sources.append({
                "channel_id": o.get("channel_id"),
                "platform": "LAZADA",
                "channel_name": laz.get("lazada_user_name"),
                "wsis_id": None,
                "social_id": str(laz["lazada_user_id"]) if laz.get("lazada_user_id") is not None else None,
            })

        # 4️⃣ LINE Shopping
        if o.get("order_from") == 21:
            line = o.get("line_info") or {}
            sources.append({
                "channel_id": o.get("channel_id"),
                "platform": "LINE SHOPPING",
                "channel_name": line.get("line_user_name"),
                "wsis_id": None,
                "social_id": str(line["line_user_id"]) if line.get("line_user_id") is not None else None,
            })
        unique_sources = []
        seen = set()
        for s in sources:
            key = (s.get("platform"), s.get("social_id"))
            if key not in seen:
                seen.add(key)
                unique_sources.append(s)

        sources = unique_sources

    # Tags (merge all unique tags across orders)
    tags = []
    for o in cluster:
        tags.extend(o.get("tags", []))
    tags = _dedup_preserve(tags)

    notes = []
    note_ids = set()

    for o in cluster:
        for note in o.get("notes", []):
            note_id = note.get("note_id")
            if note_id not in note_ids:
                # Create a copy to avoid modifying original data
                clean_note = dict(note)
                clean_note.pop("note_id", None)  # Remove note_id
                notes.append(clean_note)
                note_ids.add(note_id)
    structured_addrs = []
    for o in cluster:
        structured_addrs.append({
        "line1": (o.get("shipping_address_1") or "").strip(),
        "line2": (o.get("shipping_address_2") or "").strip(),
        "subdistrict": (o.get("shipping_subdistrict") or "").strip(),
        "district": (o.get("shipping_district") or "").strip(),
        "province": (o.get("shipping_province") or "").strip(),
        "zipcode": (o.get("shipping_zipcode") or "").strip(),
        "full": build_address(o),
        })
    # notes = _dedup_preserve(notes)

    return {
        "_id": customer_id,
        "full_name": name,
        "address": address,
        "sources": sources,
        "orders": orders_list,
        "addresses": structured_addrs,
        "phones": [p["phone_number"] for p in phone_numbers if "phone_number" in p],
        "emails": [p["email"] for p in emails_out if "email" in p],
        "tags": tags , # ✅ new
        "provider_id" : PROVIDER_ID ,
        "notes" : notes
    }

# --- Main ---------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Identify unique customers from orders JSON.")
    ap.add_argument("--input", "-i", default="all_orders.json", help="Path to input orders JSON.")
    ap.add_argument("--output", "-o", default="crm_customer_profiles.json", help="Path to write output JSON.")
    ap.add_argument("--name-strong", type=int, default=90, help="Name similarity threshold considered strong (0-100).")
    ap.add_argument("--addr-strong", type=int, default=88, help="Address similarity threshold considered strong (0-100).")
    ap.add_argument("--score-threshold", type=int, default=5, help="Score needed to treat two orders as same customer.")
    args = ap.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        orders = json.load(f)
    
    print(f"all order {len(orders)}")

    clusters = cluster_orders(orders, args.name_strong, args.addr_strong, args.score_threshold)
    profiles = [cluster_to_profile(c) for c in clusters]

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(profiles, f, ensure_ascii=False, indent=2)

    print(f"Orders: {len(orders)}")
    print(f"Customer clusters: {len(clusters)}")
    print(f"Wrote profiles -> {args.output}")

if __name__ == "__main__":
    main()
