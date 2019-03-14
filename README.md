es commands to create and delete index

```$json
PUT /assets_policies
{
  "mappings": {
    "_doc": {
      "properties": {
        "applicable": {
          "type": "boolean"
        },
        "asset_id": {
          "type": "keyword"
        },
        "name": {
          "type": "keyword"
        },
        "org_id": {
          "type": "keyword"
        },
        "platform": {
          "type": "keyword"
        },
        "results": {
          "type": "nested",
          "properties": {
            "check_name": {
              "type": "keyword"
            },
            "proof": {
              "type": "keyword"
            },
            "result": {
              "type": "keyword"
            }
          }
        }
      }
    }
  },
  "settings" : {
    "number_of_shards" : 120,
    "number_of_replicas" : 1
  }
}

DELETE /assets_policies
```