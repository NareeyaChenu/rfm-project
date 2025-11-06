

function remove() {
    
    use('OmniService');
    db.getCollection('btw_orders')
      .deleteOne(
        {

            order_id : 6208981112233112233
          /*
          * Filter
          * fieldA: value or expression
          */
        },
    
      )
    

      db.btw_order_details.deleteMany(
        {
            order_id : 6208981112233112233
        }
      )
}
// Search for documents in the current collection.


function find() {
    
    use('OmniService');
    return db.btw_orders.find(
        {
            order_id  :  10001515
        }
    )
}

// remove()


find()
// findCustomer()