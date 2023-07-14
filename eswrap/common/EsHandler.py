from typing import Optional

from elasticsearch import Elasticsearch


class EsHandler(object):
    """
    The EsHandler
    """

    def __init__(self, es_connection: Elasticsearch, index: str):
        self.es_connection = es_connection
        self.index = index

    def search(self, filter: dict = None, skip: int = 0, limit: int = 10):
        """
        Search the index.
        """
        return EsCursor(self, filter, limit=limit, skip=skip).search()

    def find(self, filter: dict = None,  **kwargs):
        """
        Query the index.
        """
        return EsCursor(self, filter, **kwargs)

    def find_one(self, filter: dict = None):
        """
        Query the index and return the first result.
        """
        for result in self.find(filter).limit(1):
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

        if missing:

            data = {
                "query": {
                    "bool": {
                        "must_not": {
                            "exists": {
                                "field": f"{field_name}"
                            }
                        }
                    }
                }
            }

        else:

            data = {
                "query": {
                    "exists": {
                        "field": f"{field_name}"
                    }
                }
            }

        return self.es_connection.search(index=self.index, body=data, **kwargs)

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
            sort: tuple = None,
            **kwargs
    ):
        """
        Create a new CveSearchApi object.

        :param es_handler: EsHandler object
        :type es_handler: EsHandler
        """

        self.EsHandler = es_handler

        self.__data = {}

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

    def __fetch_results(self):
        return self.EsHandler.es_connection.search(
            index=self.EsHandler.index, body=self.__data
        )

    def search(self):

        ret_dict = {"skip": self.__skip, "limit": self.__limit}

        self.__data["size"] = self.__limit
        self.__data["from"] = self.__skip

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

    def query(self):
        """
        Endpoint for free query
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

    def limit(self, value: int):
        """
        Method to limit the amount of returned data; default is set to 10

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
        """Make this class an iterator"""
        self.query()
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
