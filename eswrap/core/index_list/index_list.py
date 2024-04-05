import logging
from typing import List

import elastic_transport
from elasticsearch import Elasticsearch

from eswrap.core.es_index.es_index import EsIndex


class IndexList(object):
    def __init__(self, es_client: Elasticsearch):
        self.logger = logging.getLogger(__name__)

        self.__indexes = []
        self.__es_client = es_client

    @property
    def es_client(self):
        return self.__es_client

    @property
    def index_list(self):
        return list(self.es_client.indices.get(index="*").keys())

    @property
    def indexes(self) -> List[EsIndex]:
        return self.__indexes

    @indexes.setter
    def indexes(self, val: EsIndex):
        self.__indexes.append(val)

    def get_index_list(self):
        return self.indexes

    def fill_index_list(self):
        try:
            for each in self.index_list:
                self.indexes = EsIndex(each, self.es_client)
        except elastic_transport.ConnectionError as err:
            self.logger.warning(
                f"Cannot connect to elasticsearch, error encountered: {err}"
            )
        except Exception as err:
            self.logger.error(f"Uncaught exception encountered: {err}")

    def __repr__(self):
        return "<IndexList>"
