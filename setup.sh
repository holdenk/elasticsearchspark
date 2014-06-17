#!/bin/bash
set -x
sudo /etc/init.d/elasticsearch start
curl -XPUT 'http://localhost:9200/twitter/tweet/_mapping' -d '
{
    "tweet" : {
        "properties" : {
            "message" : {"type" : "string", "store" : true },
            "hashTags" : {"type" : "string", "store" : true },
            "location" : {"type" : "geo_point", "store" : true }
        }
    }
}
'
