import datetime
import json

import requests
from elasticsearch import Elasticsearch

es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])
r = requests.get("http://localhost:9200")
i = 18
while r.status_code == 200:
    r = requests.get("https://swapi.dev/api/people/" + str(i))

    data = json.loads(r.content)
    data["created"] = datetime.datetime.now()
    data["edited"] = datetime.datetime.now()
    a = es.index(index="sw", id=i, document=data)
    i = i + 1
    print(f"inserting record: {i} --> {a}")
