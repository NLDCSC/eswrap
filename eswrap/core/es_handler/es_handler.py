from typing import Optional

from elasticsearch import Elasticsearch


class EsHandler(object):
    """
    The EsHandler
    """

    def __init__(self, es_connection: Elasticsearch, index: str):
        self.es_connection = es_connection
        self.index = index

    def search(self):
        """
        Search the index.
        """
        return EsCursor(self)

    def find(self, filter: dict = None, **kwargs):
        """
        Query the index.
        """
        return EsCursor(self, filter, **kwargs)

    def find_one(self, filter: dict = None):
        """
        Query the index and return the first result.
        """
        for result in self.find(filter).set_limit(1):
            return result
        return None

    def count(self, filter: dict = None, **kwargs):
        """ """
        if filter is None:
            data = {"query": {"match_all": {}}}
        else:
            if "regexp" in kwargs:
                if kwargs["regexp"]:
                    if "bool_query" in kwargs and kwargs["bool_query"]:
                        data = {"query": {"bool": filter}}
                        # popping this value; elasticsearch does not recognize this value
                        kwargs.pop("bool_query")
                    else:
                        data = {"query": {"regexp": filter}}
                    # popping this value; elasticsearch does not recognize this value
                    kwargs.pop("regexp")
                else:
                    data = {"query": {"match": filter}}
            else:
                data = {"query": {"match": filter}}

        return self.es_connection.count(index=self.index, body=data, **kwargs)["count"]

    def query_on_field(self, field_name: str, missing: bool = True, **kwargs):

        kwargs = kwargs
        kwargs["query_on_field"] = True

        if missing:
            kwargs["bool_query"] = True

            data = {"must_not": {"exists": {"field": f"{field_name}"}}}

        else:

            kwargs["exists_query"] = True

            data = {"field": f"{field_name}"}

        return EsCursor(self, data, **kwargs)

    def upsert(self, document: dict, doc_id: Optional[str] = None, **kwargs):
        """ """
        if doc_id is None:
            return self.es_connection.index(
                index=self.index, document=document, **kwargs
            )
        else:
            return self.es_connection.index(
                index=self.index, id=doc_id, document=document, **kwargs
            )

    def delete(self, doc_id: str, **kwargs):
        return self.es_connection.delete(index=self.index, id=doc_id, **kwargs)

    def delete_by_query(self, filter: dict, **kwargs):
        return self.es_connection.delete_by_query(
            index=self.index, body=filter, **kwargs
        )

    def __repr__(self):
        """return a string representation of the obj EsHandler"""
        return "<< EsHandler: {} >>".format(self.index)


class EsCursor(object):
    """
    The EsCursor
    """

    def __init__(
        self,
        es_handler: EsHandler,
        filter: dict = None,
        limit: int = 10,
        skip: int = None,
        sort: list = None,
        **kwargs,
    ):
        """
        Create a new EsCursor object.

        :param es_handler: EsHandler object
        :type es_handler: EsHandler
        """

        self.__es_handler = es_handler

        self.__filter_data = {}

        for k, v in kwargs.items():
            setattr(self, k, v)

        data = filter
        if data is None:
            data = {"query": {"match_all": {}}}
        else:
            if hasattr(self, "regexp"):
                if self.regexp:
                    if hasattr(self, "bool_query") and self.bool_query:
                        data = {"query": {"bool": filter}}
                    else:
                        data = {"query": {"regexp": filter}}
                else:
                    data = {"query": {"match": filter}}
            elif hasattr(self, "query_on_field"):
                if hasattr(self, "bool_query") and self.bool_query:
                    data = {"query": {"bool": filter}}
                if hasattr(self, "exists_query") and self.exists_query:
                    data = {"query": {"exists": filter}}
            else:
                data = {"query": {"match": filter}}

        self.__filter_data = data

        self.__empty = False

        self.__limit = limit
        self.__skip = skip
        self.__sort = sort

        self.data_queue = None

    @property
    def es_handler(self) -> EsHandler:
        return self.__es_handler

    @property
    def filter_data(self) -> dict:
        return self.__filter_data

    @filter_data.setter
    def filter_data(self, value: dict) -> None:
        self.__filter_data = value

    @property
    def empty(self) -> bool:
        return self.__empty

    @property
    def q_limit(self) -> int:
        return self.__limit

    @q_limit.setter
    def q_limit(self, limit: int) -> None:
        self.__limit = limit

    @property
    def q_skip(self) -> int:
        return self.__skip

    @q_skip.setter
    def q_skip(self, skip: int) -> None:
        self.__skip = skip

    @property
    def q_sort(self) -> list:
        return self.__sort

    @q_sort.setter
    def q_sort(self, sort: list) -> None:
        self.__sort = sort

    def __repr__(self):
        """return a string representation of the obj GenericApi"""
        return "<<EsCursor: {}>>".format(self.es_handler.index)

    def __fetch_results(self):
        return self.es_handler.es_connection.search(
            index=self.es_handler.index, body=self.filter_data
        )

    def filter(self, *args, **kwargs):
        pass

    def match_all(self):
        self.filter_data = {"query": {"match_all": {}}}

        return self

    def search(self):

        ret_dict = {"skip": self.q_skip, "limit": self.q_limit}

        self.filter_data["size"] = self.q_limit
        self.filter_data["from"] = self.q_skip

        results = self.__fetch_results()

        if isinstance(results, str):
            ret_dict["data"] = []
            ret_dict["total"] = 0
            return ret_dict

        try:
            if len(results["hits"]["hits"]) > 0:
                count = results["hits"]["total"]["value"]
                results = [x["_source"] for x in results["hits"]["hits"]]
            else:
                count = 0
                results = []

            ret_dict["data"] = results
            ret_dict["total"] = count

        except Exception:
            ret_dict["data"] = results
            ret_dict["total"] = len(results)

        return ret_dict

    def execute(self):
        """
        Fetch results from Elasticsearch
        """

        results = self.__fetch_results()

        if isinstance(results, str):
            self.data_queue = None
            return

        try:
            if len(results["hits"]["hits"]) > 0:
                ret_list = []
                for x in results["hits"]["hits"]:
                    val_dict = x["_source"]
                    val_dict.update({"_id": x["_id"]})
                    ret_list.append(val_dict)

                self.data_queue = ret_list
        except Exception:
            self.data_queue = results

    def set_limit(self, value: int):
        """
        Method to limit the amount of returned data; default is set to 10
        """

        if not isinstance(value, int):
            raise TypeError("limit must be an integer")

        self.q_limit = value

        self.filter_data["size"] = self.q_limit

        return self

    def set_skip(self, value: int):
        """
        Method to skip the given amount of records before returning the data
        """

        if not isinstance(value, int):
            raise TypeError("skip must be an integer")

        self.q_skip = value

        self.filter_data["from"] = self.q_skip

        return self

    def set_sort(self, values: list):
        """
        A comma-separated list of <field>:<direction> pairs
        """
        self.q_sort = values

        self.filter_data["sort"] = values

        return self

    def __iter__(self):
        """Make this class an iterator"""
        self.execute()
        return self

    def next(self):
        """Iterate to the results and return database objects"""
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
