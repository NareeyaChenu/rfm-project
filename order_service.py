
import json
from context.order_context import query_orders, count_document
from context.member_context import find_one
from context.member_tag_context import find_member_tag_list
from context.member_channel_context import find_list_member_channels
import pandas as pd
from jellyfish import jaro_winkler_similarity
import time
import random
import string
from datetime import datetime


start_date = "2025-10-01"
end_date = "2025-10-31"


def export_order():

    print("export order ...")
    filter = {
        '$and': [
            {"date_created": {'$gte': f'{start_date} 00:00:00'}},
            {"date_created": {'$lte': f'{end_date} 23:59:59'}}
        ],
        'order_status_id': {'$ne': 4},
        'order_from': {'$ne': 22},
    }

    print(filter)

    documents = count_document(filter)

    if (documents <= 0):
        print("No order to Excecute")
        return

    print(f"tatal order {documents}")
    list_order = []
    batch_size = 100
    total_fetched = 0
    skip = 0

    while True:

        orders = query_orders(filter, batch_size, skip)

        fetched_count = len(orders)
        if fetched_count == 0:
            break

        print(f"Processing batch {skip}: {fetched_count} orders")
        list_order.extend(orders)
        total_fetched += batch_size

        skip += batch_size
        if total_fetched >= documents:
            break
        time.sleep(2.5)

    with open(f"orders/orders_{start_date}_to_{end_date}.json", "w", encoding="utf-8") as f:
        json.dump(list_order, f, ensure_ascii=False, indent=4, default=str)


def normalize(text):
    return str(text).strip().lower().replace("-", "").replace(" ", "")


def fuzzy_match_customer(record1, record2):
    # Combine and normalize names
    name1 = normalize(record1.get('firstname', '') +
                      record1.get('lastname', ''))
    name2 = normalize(record2.get('firstname', '') +
                      record2.get('lastname', ''))

    # Normalize phone numbers
    phone1 = normalize(record1.get('phone', ''))
    phone2 = normalize(record2.get('phone', ''))

    # Combine and normalize addresses
    address1 = normalize(" ".join([
        record1.get('shipping_address_1', ''),
        record1.get('shipping_subdistrict', ''),
        record1.get('shipping_district', ''),
        record1.get('shipping_province', ''),
        record1.get('shipping_zipcode', '')
    ]))

    address2 = normalize(" ".join([
        record2.get('shipping_address_1', ''),
        record2.get('shipping_subdistrict', ''),
        record2.get('shipping_district', ''),
        record2.get('shipping_province', ''),
        record2.get('shipping_zipcode', '')
    ]))

    # Shopee ID exact match override
    if record1.get('shopee_info') and record2.get('shopee_info'):
        if record1['shopee_info'] == record2['shopee_info']:
            return 1.0  # Perfect match

    # Fuzzy similarity scores
    name_score = jaro_winkler_similarity(name1, name2)
    phone_score = jaro_winkler_similarity(phone1, phone2)
    address_score = jaro_winkler_similarity(address1, address2)

    # Weighted average score
    total_score = round((name_score * 0.4) +
                        (phone_score * 0.3) + (address_score * 0.3), 4)
    return total_score


def identify_customer():
    record_a = {
        "firstname": "Nareeya",
        "lastname": "Tansakul",
        "phone": "0891234567",
        "shipping_address_1": "123 Main Rd",
        "shipping_subdistrict": "Bangna",
        "shipping_district": "Bangna",
        "shipping_province": "Bangkok",
        "shipping_zipcode": "10260",
        "shopee_info": "user_abc"
    }

    record_b = {
        "firstname": "Nareeya",
        "lastname": "T.",
        "phone": "089-123-4567",
        "shipping_address_1": "123 Main Road",
        "shipping_subdistrict": "Bangna",
        "shipping_district": "Bangna",
        "shipping_province": "Bangkok",
        "shipping_zipcode": "10260",
        "shopee_info": "user_abc123"
    }

    score = fuzzy_match_customer(record_a, record_b)
    print(f"Match Score: {score}")
    if score > 0.85:
        print("✅ Potential Match")
    else:
        print("❌ No Match")


def find_wsis_id(input_path: string, output_path):

    with open(input_path, "r", encoding="utf-8") as f:
        orders = json.load(f)

    def random_str():
        random_word = ''.join(random.choices(string.ascii_lowercase, k=5))
        return random_word

    print(f"process file : {input_path}")
    print(f"total order to process : {len(orders)}")
    for order in orders:
        print(f"Process order {orders.index(order)}: {order.get('order_id')}")
        if order.get("member_id") is not None:
            member = find_one({"_id": order["member_id"]})

            time.sleep(2)

            tags = find_member_tag_list({"member_id": order["member_id"]})

            time.sleep(2)

            channels = find_list_member_channels(
                {"member_id": order["member_id"]})

            platform = None
            social_id = None

            if member:
                fb_id = member.get("facebook_profile", {}).get("facebook_id")
                line_id = member.get("line_profile", {}).get("line_id")
                ig_id = member.get("instagram_profile", {}).get("igsid")
                social_name = member.get("member_name", {})

                if fb_id:
                    platform = "FACEBOOK"
                    social_id = fb_id
                elif line_id:
                    platform = "LINE"
                    social_id = line_id
                elif ig_id:
                    platform = "INSTAGRAM"
                    social_id = ig_id
                if len(channels) <= 0:

                    order["social"] = [
                        {
                            "social_id": f"{social_id}",
                            "platform": platform,
                            "social_name": social_name,
                            "wsis_id": order["member_id"]
                        }
                    ]

                else:
                    socials = []

                    for channel in channels:
                        social = {
                            "social_id": f"{social_id}",
                            "platform": platform,
                            "social_name": social_name,
                            "wsis_id": order["member_id"],
                            "channel_name": channel.get("channel_name", None)
                        }
                        socials.append(social)
                    order["social"] = socials

                tag_names = [item["tag_name"] for item in tags]

                notes = member.get("notes", [])
                new_notes = []
                for note in notes:
                    new_note = {
                        "text": note.get("value", ""),
                        "modified_date": note.get("modified_date"),
                        "note_id" : note.get("note_id")
                    }
                    new_notes.append(new_note)

                order["tags"] = tag_names
                order["notes"] = new_notes
        time.sleep(3.5)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(orders, f, ensure_ascii=False, indent=4, default=str)

    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Find WSIS ID Successfully at: {now}")


if __name__ == "__main__":

    folder_path = "orders"
    input_path = f"{folder_path}/orders_2025-10-01_to_2025-10-31.json"

    output_path = "process/process_orders_2025-10-01_to_2025-10-31.json"

    find_wsis_id(input_path, output_path)

    # export_order()
