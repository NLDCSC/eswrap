import datetime
import json

import requests

from eswrap import EsWrap

es = EsWrap()
i = 1
while i < 30:
    r = requests.get("https://swapi.dev/api/people/" + str(i))

    data = json.loads(r.content)
    data["created"] = datetime.datetime.now()
    data["edited"] = datetime.datetime.now()
    data["enabled"] = True
    a = es.index(index_name="sw", doc_id=str(i), data=data)
    i += 1
    print(f"inserting record: {i} --> {a}")
