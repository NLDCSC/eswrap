import json

import requests
from elasticsearch import Elasticsearch

es = Elasticsearch([{"host": "localhost", "port": 9200}])
r = requests.get("http://localhost:9200")
i = 18
while r.status_code == 200:
    r = requests.get("https://swapi.dev/api/people/" + str(i))
    es.index(index="sw", doc_type="people", id=i, body=json.loads(r.content))
    i = i + 1
    print("inserting record: {}".format(i))
