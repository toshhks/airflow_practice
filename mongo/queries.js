let query_1 = [
    { "$group": { "_id": "$content", "count": { "$sum": 1 } } },
    { "$sort": { "count": -1 } },
    { "$limit": 5 }
];

let query_2 = [
    {
      "$match": {
        "$expr": {
          "$lt": [ { "$strLenCP": { "$toString": "$content" } }, 5 ]
        }
      }
    }
];

let query_3 = [
    {
      "$group": {
        "_id": {
          "$toDate": {
            "$dateToString": {
              "format": "%Y-%m-%d",
              "date": { "$toDate": "$at" }
            }
          }
        },
        "avg_score": { "$avg": "$score" }
      }
    },
    { "$sort": { "_id": 1 } }
];