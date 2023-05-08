import os
from typing import Optional

import urllib3
from elasticsearch import Elasticsearch
from urllib3.exceptions import InsecureRequestWarning

from eswrap.common.EsHandler import EsHandler

urllib3.disable_warnings(InsecureRequestWarning)

try:
    from version import VERSION
except ModuleNotFoundError:
    _PKG_DIR = os.path.dirname(__file__)
    version_file = os.path.join(_PKG_DIR, "VERSION")
    with open(version_file, "r") as fdsec:
        VERSION = fdsec.read()


class EsWrap(object):
    def __init__(
        self, host: str = "localhost", port: int = 9200, scheme: str = "http", **kwargs
    ):
        self.__version = VERSION

        self.connection_details = {"host": host, "port": port, "scheme": scheme}

        self.__es_client = Elasticsearch([self.connection_details], **kwargs)

        for each in self.__es_client.indices.get(index="*").keys():
            if not each.startswith("."):
                setattr(
                    self, each, EsHandler(es_connection=self.__es_client, index=each)
                )

    @property
    def es_client(self):
        return self.__es_client

    @property
    def version(self):
        """Property returning current version"""
        return self.__version

    @property
    def indices(self):
        return list(self.es_client.indices.get(index="*").keys())

    @property
    def info(self):
        return self.es_client.info()

    def index(self, index_name: str, data: dict, doc_id: Optional[str] = None):
        if not hasattr(self, index_name):
            setattr(
                self,
                index_name,
                EsHandler(es_connection=self.__es_client, index=index_name),
            )

        return self.es_client.index(index=index_name, document=data, id=doc_id)

    def __del__(self):
        self.__es_client.close()

    def __repr__(self):
        """String representation of object"""
        return "<< EsWrap:{} >>".format(self.version)
