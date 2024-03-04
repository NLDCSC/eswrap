from elasticsearch import Elasticsearch

from eswrap.core.es_handler.es_handler import EsHandler


class EsIndex(object):
    def __init__(self, name: str, es_client: Elasticsearch):
        self.name = name
        self.handler = EsHandler(es_connection=es_client, index=name)

    def __call__(self, *args, **kwargs):
        return self.handler

    def __repr__(self):
        return "<EsIndex: {}>".format(self.name)
