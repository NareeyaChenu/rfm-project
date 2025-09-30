from pymongo import MongoClient
mongo_host = "mongodb://root:f41a7bc0-f8da-478d-b7fb-672ff53c8d7a@localhost:27017/?authSource=admin&directConnection=true&readPreference=primaryPreferred"
client = MongoClient(mongo_host)

db = client['OmniService']
collection = db['btw_orders']




def query_orders(filter: dict, limit: int, skip: int):
    projection = {
        "provider_id": 1,
        "shop_id": 1,
        "channel_id": 1,
        "member_id": 1,
        "created_date": 1,
        "modified_date": 1,
        "shipping_zipcode": 1,
        "order_id": 1,
        "order_from": 1,
        "firstname": 1,
        "lastname": 1,
        "email": 1,
        "phone": 1,
        "shipping_firstname": 1,
        "shipping_lastname": 1,
        "shipping_email": 1,
        "shipping_phone": 1,
        "shipping_address_1": 1,
        "shipping_address_2": 1,
        "shipping_subdistrict": 1,
        "shipping_district": 1,
        "shipping_province": 1,
        "grand_total": 1,
        "extern_member_id": 1,
        "lazada_info": {
            "customer_first_name": {"$first": "$order_lazada.customer_first_name"},
            "customer_last_name": {"$first": "$order_lazada.customer_last_name"},
        },
        "shopee_info": {
            "shopee_user_id": {"$first": "$order_shopee.raw_body.buyer_user_id"},
            "shopee_user_name": {"$first": "$order_shopee.raw_body.buyer_username"},
        },
        "line_shopping_info": {
            "recipientName": "$order_line_shopping.shipping_address.recipientName",
            "address": "$order_line_shopping.shipping_address.address",
            "province": "$order_line_shopping.shipping_address.province",
            "postalCode": "$order_line_shopping.shipping_address.postalCode",
            "phoneNumber": "$order_line_shopping.shipping_address.phoneNumber",
            "email": "$order_line_shopping.shipping_address.email",
            "district": "$order_line_shopping.shipping_address.district",
            "subDistrict": "$order_line_shopping.shipping_address.subDistrict",
        },
        'products' : 1
    }

    pipeline = [
        {"$match": filter},
        {"$sort" : {'created_date' : 1}},
        {"$skip": skip},
        {"$limit": limit},
        {
            "$lookup": {
                "from": "btw_order_details",
                "localField": "order_id",
                "foreignField": "order_id",
                "pipeline" : [
                    {
                        "$project": {
                            "_id": 0,
                            "name": 1,
                            "sku": 1,
                            "product_id" : 1 ,
                            "order_item" : {
                                "$map": {
                                "input": "$order_item",
                                "as": "item",
                                "in": {
                                    "name": "$$item.name",
                                    "sku": "$$item.sku",
                                    "product_id" : "$$item.product_id"
                                }
                                }
                            }
                        }
                    }
                ],
                "as": "products"
            }
        },
        {"$project": projection},
    ]


    orders = list(collection.aggregate(pipeline))

    return orders




def count_document (filter : dict) -> int :
    return collection.count_documents(filter)