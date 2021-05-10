from elasticsearch import Elasticsearch

es = Elasticsearch([{"host": "localhost", "port": 9200}])

print(es.get(index="sw", id=5))

print(es.indices.get_alias("*").keys())
es.remote.transport.hosts