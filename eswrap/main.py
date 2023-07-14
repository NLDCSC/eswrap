import logging
import os
from typing import Optional

import elastic_transport
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
        self,
        host: str = "localhost",
        port: int = 9200,
        scheme: str = "http",
        connection_details: list[str] | list[dict] = None,
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

        try:
            for each in self.__es_client.indices.get(index="*").keys():
                if not each.startswith("."):
                    setattr(
                        self,
                        each,
                        EsHandler(es_connection=self.__es_client, index=each),
                    )
                else:
                    # workaround for handling system indexes; needs further troubleshooting
                    setattr(
                        self,
                        each[1:].split("-")[0],
                        EsHandler(es_connection=self.__es_client, index=each),
                    )
        except elastic_transport.ConnectionError as err:
            self.logger.warning(
                f"Cannot connect to elasticsearch, error encountered: {err}"
            )
        except Exception as err:
            self.logger.error(f"Uncaught exception encountered: {err}")

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

    def delete_index(self, index_name: str):

        ret_val = self.es_client.options(ignore_status=[400, 404]).indices.delete(
            index=index_name
        )

        try:
            if ret_val["acknowledged"]:
                if hasattr(self, index_name):
                    delattr(self, index_name)
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
                if not hasattr(self, index_name):
                    setattr(
                        self,
                        index_name,
                        EsHandler(es_connection=self.__es_client, index=index_name),
                    )
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
        self.__es_client.close()

    def __repr__(self):
        """String representation of object"""
        return "<< EsWrap:{} >>".format(self.version)
