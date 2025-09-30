/* global use, db */
// MongoDB Playground
// Use Ctrl+Space inside a snippet or a string literal to trigger completions.

// The current database to use.
use('OmniService');

// Search for documents in the current collection.
db.getCollection('btw_orders')
    .find(
        {

            // order_from: 31,
            full_order_code : "WN0303255"
            // full_order_code : "WN0302230"

            /*
            * Filter
            * fieldA: value or expression
            */
        },
        {
            provider_id: 1,
            shop_id: 1,
            channel_id: 1,
            member_id: 1,
            created_date: 1,
            modified_date: 1,
            shipping_zipcode: 1,
            order_id: 1,
            order_from: 1,
            firstname: 1,
            lastname: 1,
            email: 1,
            phone: 1,
            shipping_firstname: 1,
            shipping_lastname: 1,
            shipping_email: 1,
            shipping_phone: 1,
            shipping_address_1: 1,
            shipping_address_2: 1,
            shipping_subdistrict: 1,
            shipping_district: 1,
            shipping_province: 1,
            grand_total: 1,
            extern_member_id : 1 , 
            lazada_info : {
                customer_first_name : {$first : "$order_lazada.customer_first_name"},
                customer_last_name : {$first : "$order_lazada.customer_last_name"},
            },
            shopee_info : {
                shopee_user_id : {$first : "$order_shopee.raw_body.buyer_user_id"} ,
                shopee_user_name : {$first : "$order_shopee.raw_body.buyer_username"}
            },
            line_shopping_info : {
                recipientName : '$order_line_shopping.shipping_address.recipientName',
                address : '$order_line_shopping.shipping_address.address',
                province : '$order_line_shopping.shipping_address.province',
                postalCode : '$order_line_shopping.shipping_address.postalCode',
                phoneNumber : '$order_line_shopping.shipping_address.phoneNumber',
                email : '$order_line_shopping.shipping_address.email',
                district : '$order_line_shopping.shipping_address.district',
                subDistrict : '$order_line_shopping.shipping_address.subDistrict',
            }
            /*
            * Projection
            * _id: 0, // exclude _id
            * fieldA: 1 // include field
            */
        }
    ).sort({ created_date: -1 }).limit(1)
