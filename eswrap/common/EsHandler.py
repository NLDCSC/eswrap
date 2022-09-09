from typing import Optional

from elasticsearch import Elasticsearch


class EsHandler(object):
    """
    The EsHandler
    """

    def __init__(self, es_connection: Elasticsearch, index: str):
        self.es_connection = es_connection
        self.index = index

    def find(self, filter: dict = None):
        """
        Query the api.

        :return: Reference to the EsCursor
        :rtype: EsCursor
        """

        return EsCursor(self, filter)

    def find_one(self, filter: dict = None):
        """
        Query the api.

        :return: Data or None
        :rtype: object
        """

        cursor = self.find(filter)

        for result in cursor.limit(10000):
            return result
        return None

    def count(self, filter: dict = None):
        """

        :return:
        :rtype:
        """
        if filter is None:
            data = {"query": {"match_all": {}}}
        else:
            data = {"query": {"match": filter}}

        return self.es_connection.count(index=self.index, body=data)["count"]

    def upsert(self, document: dict, doc_id: Optional[str] = None):
        """

        :return:
        :rtype:
        """
        if doc_id is None:
            return self.es_connection.index(index=self.index, document=document)
        else:
            return self.es_connection.index(index=self.index, id=doc_id, document=document)

    def __repr__(self):
        """return a string representation of the obj EsHandler"""
        return "<< EsHandler: {} >>".format(self.es_connection.remote.transport.hosts)


class EsCursor(object):
    """
    The EsCursor
    """

    def __init__(
        self,
        es_handler: EsHandler,
        filter: dict = None,
        limit: int = 10000,
        skip: int = None,
        sort: tuple = None,
    ):
        """
        Create a new CveSearchApi object.

        :param es_handler: EsHandler object
        :type es_handler: EsHandler
        """

        self.EsHandler = es_handler

        self.__data = {}

        data = filter
        if data is None:
            data = {"query": {"match_all": {}}}
        else:
            data = {"query": {"match": filter}}

        self.__data = data

        self.__empty = False

        self.__limit = limit
        self.__skip = skip
        self.__sort = sort

        self.data_queue = None

    def __repr__(self):
        """return a string representation of the obj GenericApi"""
        return "<<EsCursor: {}>>".format(self.EsHandler.index)

    def query(self):
        """
        Endpoint for free query
        """

        results = self.EsHandler.es_connection.search(
            index=self.EsHandler.index, body=self.__data
        )

        if isinstance(results, str):
            self.data_queue = None
            return

        try:
            if len(results["hits"]["hits"]) > 0:
                self.data_queue = [x["_source"] for x in results["hits"]["hits"]]
        except Exception:
            self.data_queue = results

    def limit(self, value: int):
        """
        Method to limit the amount of returned data

        :param value: Limit
        :type value: int
        :return: EsCursor object
        :rtype: EsCursor
        """

        if not isinstance(value, int):
            raise TypeError("limit must be an integer")

        self.__limit = value

        self.__data["size"] = self.__limit

        return self

    def skip(self, value: int):
        """
        Method to skip the given amount of records before returning the data

        :param value: Skip
        :type value: int
        :return: EsCursor object
        :rtype: EsCursor
        """

        if not isinstance(value, int):
            raise TypeError("skip must be an integer")

        self.__skip = value

        self.__data["from"] = self.__skip

        return self

    def sort(self, values: list):
        """
        A comma-separated list of <field>:<direction> pairs

        :param values: A comma-separated list of <field>:<direction> pairs
        :type values: list
        :return: EsCursor object
        :rtype: EsCursor
        """
        self.__sort = values

        self.__data["sort"] = values

        return self

    def __iter__(self):
        """ Make this class an iterator """
        self.query()
        return self

    def next(self):
        """ Iterate to the results and return database objects """
        if self.__empty:
            raise StopIteration
        if self.data_queue is None:
            raise StopIteration
        try:
            if len(self.data_queue):
                return self.data_queue.pop()
            else:
                raise StopIteration
        except TypeError:
            # We've received a Response object
            raise StopIteration

    __next__ = next
