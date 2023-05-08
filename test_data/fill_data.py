import datetime
import json

import requests

from eswrap import EsWrap

es = EsWrap(scheme="https", verify_certs=False, http_auth=("elastic", "Knocks.01"))
i = 18
while i < 30:
    r = requests.get("https://swapi.dev/api/people/" + str(i))

    data = json.loads(r.content)
    data["created"] = datetime.datetime.now()
    data["edited"] = datetime.datetime.now()
    a = es.index(index_name="sw", doc_id=str(i), data=data)
    i += 1
    print(f"inserting record: {i} --> {a}")
