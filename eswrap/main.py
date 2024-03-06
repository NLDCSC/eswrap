import collections
import logging
import os
from typing import Optional, List

import urllib3
from elasticsearch import Elasticsearch
from urllib3.exceptions import InsecureRequestWarning

from eswrap.core.es_handler.es_handler import EsHandler
from eswrap.core.es_index.es_index import EsIndex
from eswrap.core.index_list.index_list import IndexList
from eswrap.errors.indexes import IndexNotFoundError

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
        self,
        host: str = "localhost",
        port: int = 9200,
        scheme: str = "http",
        connection_details: list[str] | list[dict] = None,
        auto_init_index_handlers: bool = False,
        **kwargs,
    ):
        """
        connection_details should facilitate all connection options as stated in the API documentation of the
        Elasticsearch library.
        E.g.
        [
            {'host': 'localhost'},
            {'host': 'othernode', 'port': 443, 'url_prefix': 'es', 'use_ssl': True},
        ]

        or

        ['localhost:443', 'other_host:443'] with additional kwargs (if any)

        or

        [
            'http://user:secret@localhost:9200/',
            'https://user:secret@other_host:443/production'
        ]
        """
        self.__version = VERSION

        if connection_details is None:
            self.connection_details = [{"host": host, "port": port, "scheme": scheme}]
        else:
            self.connection_details = connection_details

        self.logger = logging.getLogger(__name__)

        self.__es_client = Elasticsearch(self.connection_details, **kwargs)

        self.__index_list = IndexList(es_client=self.es_client)
        self.__index_dict = {}
        self.__indexes = []

        if auto_init_index_handlers:
            self.setup_handlers_for_indexes()

    @property
    def es_client(self):
        return self.__es_client

    @property
    def version(self):
        """Property returning current version"""
        return self.__version

    @property
    def index_list(self) -> IndexList:
        return self.__index_list

    @property
    def index_dict(self) -> dict:
        return self.__index_dict

    @index_dict.setter
    def index_dict(self, val: dict) -> None:
        self.__index_dict = val

    @property
    def indexes(self) -> List[EsIndex]:
        return self.index_list.indexes

    @property
    def info(self):
        return self.es_client.info()

    def get_index_handler(self, index_name: str) -> EsHandler:
        if len(self.indexes) == 0:
            self.setup_handlers_for_indexes()

        try:
            index_handler = [
                x for x in self.index_list.indexes if x.name == index_name
            ][0]()
            return index_handler
        except IndexError:
            raise IndexNotFoundError

    def setup_handlers_for_indexes(self):
        self.index_list.fill_index_list()

        index_coldict = collections.defaultdict()

        for index in self.indexes:
            index_coldict[index.name] = index

        self.index_dict = dict(index_coldict)

    def index(self, index_name: str, data: dict, doc_id: Optional[str] = None):

        ret_data = self.es_client.index(index=index_name, document=data, id=doc_id)

        if index_name not in self.index_dict.keys():
            self.setup_handlers_for_indexes()

        return ret_data

    def search(self, index_name: str):

        return self.get_index_handler(index_name).search()

    def delete_index(self, index_name: str):

        ret_val = self.es_client.options(ignore_status=[400, 404]).indices.delete(
            index=index_name
        )

        try:
            if ret_val["acknowledged"]:
                self.setup_handlers_for_indexes()
                return True
        except KeyError:
            # failed somehow, assuming the given index does not exist
            self.logger.warning(
                f"The index {index_name} cannot not be deleted; reason -> {ret_val}"
            )
        except Exception as err:
            self.logger.error(f"Uncaught exception encountered: {err}")

        return False

    def create_index(self, index_name: str):

        ret_val = self.es_client.options(ignore_status=[400, 404]).indices.create(
            index=index_name
        )

        try:
            if ret_val["acknowledged"]:
                self.setup_handlers_for_indexes()
                return True
        except KeyError:
            # failed somehow, assuming the given index does not exist
            self.logger.warning(
                f"The index {index_name} cannot not be created; reason -> {ret_val}"
            )
        except Exception as err:
            self.logger.error(f"Uncaught exception encountered: {err}")

        return False

    def __del__(self):
        self.es_client.close()

    def __repr__(self):
        """String representation of object"""
        return "<< EsWrap:{} >>".format(self.version)
