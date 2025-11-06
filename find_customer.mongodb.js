/* global use, db */
// MongoDB Playground
// Use Ctrl+Space inside a snippet or a string literal to trigger completions.

// The current database to use.
use('report_db');

// Search for documents in the current collection.


function find() {
   return db.getCollection('crm_customer_profiles')
  .find(
    {

        // full_name : {$regex : "เสาวลัก"}

        "orders.order_id" : 6208981112233112233
      /*
      * Filter
      * fieldA: value or expression
      */
    },
    {
      /*
      * Projection
      * _id: 0, // exclude _id
      * fieldA: 1 // include field
      */
    }
  )
  .sort({
    /*
    * fieldA: 1 // ascending
    * fieldB: -1 // descending
    */
  });
}


function deleteOne() {
    return db.crm_customer_profiles.deleteOne({_id : "44f4b399-84ab-548f-b801-52ca177bf266"})
}


find()

// deleteOne()
