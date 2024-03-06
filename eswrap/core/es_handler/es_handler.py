import collections
from typing import Optional

from elasticsearch import Elasticsearch

from eswrap.errors.queries import QueryTypeNotSupportedError


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

    def count(self, filter_data: dict = None, **kwargs):
        """
        Count the number of records in the query
        """
        if filter_data is None:
            data = {"query": {"match_all": {}}}
        else:
            if "regexp" in kwargs:
                if kwargs["regexp"]:
                    if "bool_query" in kwargs and kwargs["bool_query"]:
                        data = {"query": {"bool": filter_data}}
                        # popping this value; elasticsearch does not recognize this value
                        kwargs.pop("bool_query")
                    else:
                        data = {"query": {"regexp": filter_data}}
                    # popping this value; elasticsearch does not recognize this value
                    kwargs.pop("regexp")
                else:
                    data = {"query": {"match": filter_data}}
            else:
                data = {"query": {"match": filter_data}}

        return self.es_connection.count(index=self.index, body=data, **kwargs)["count"]

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

    def delete_by_query(self, filter_data: dict, **kwargs):
        return self.es_connection.delete_by_query(
            index=self.index, body=filter_data, **kwargs
        )

    def __repr__(self):
        """return a string representation of the obj EsHandler"""
        return "<< EsHandler: {} >>".format(self.index)


"""
intervals query
    A full text query that allows fine-grained control of the ordering and proximity of matching terms.
match query
    The standard query for performing full text queries, including fuzzy matching and phrase or
    proximity queries.
match_bool_prefix query
    Creates a bool query that matches each term as a term query, except for the last term, which is
    matched as a prefix query
match_phrase query
    Like the match query but used for matching exact phrases or word proximity matches.
match_phrase_prefix query
    Like the match_phrase query, but does a wildcard search on the final word.
multi_match query
    The multi-field version of the match query.
combined_fields query
    Matches over multiple fields as if they had been indexed into one combined field.
query_string query
    Supports the compact Lucene query string syntax, allowing you to specify AND|OR|NOT conditions and
    multi-field search within a single query string. For expert users only.
simple_query_string query
    A simpler, more robust version of the query_string syntax suitable for exposing directly to users.

exists query
    Returns documents that contain any indexed value for a field.
fuzzy query
    Returns documents that contain terms similar to the search term. Elasticsearch measures similarity,
    or fuzziness, using a Levenshtein edit distance.
ids query
    Returns documents based on their document IDs.
prefix query
    Returns documents that contain a specific prefix in a provided field.
range query
    Returns documents that contain terms within a provided range.
regexp query
    Returns documents that contain terms matching a regular expression.
term query
    Returns documents that contain an exact term in a provided field.
terms query
    Returns documents that contain one or more exact terms in a provided field.
terms_set query
    Returns documents that contain a minimum number of exact terms in a provided field. You can define
    the minimum number of matching terms using a field or script.
wildcard query
    Returns documents that contain terms matching a wildcard pattern.
"""


class EsCursor(object):
    """
    The EsCursor
    """

    def __init__(
        self,
        es_handler: EsHandler,
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

        self.__filter_data = collections.defaultdict(
            lambda: collections.defaultdict(lambda: collections.defaultdict(dict))
        )

        self.tier1_query = False
        self.tierx_query = False

        self.supported_bool_filter_types = [
            "match",
            "match_phrase",
            "match_phrase_prefix",
            "exists",
            "fuzzy",
            "prefix",
            "term",
            "terms",
            "range",
            "regexp",
            "wildcard",
        ]

        self.supported_bool_query_types = [
            "match",
            "match_phrase",
            "match_phrase_prefix",
            "exists",
            "fuzzy",
            "ids",
            "prefix",
            "term",
            "terms",
            "range",
            "regexp",
            "wildcard",
        ]

        self.supported_bool_exclude_types = self.supported_bool_query_types

        for k, v in kwargs.items():
            setattr(self, k, v)

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

    def __set_query_tier_level(self):
        if not self.tier1_query:
            self.tier1_query = True
            return False

        if not self.tierx_query:
            self.tierx_query = True

        return True

    def filter(self, query_type: str = None, **kwargs):
        """
        In a filter context, a query clause answers the question “Does this document match this query clause?” The
        answer is a simple Yes or No, no scores are calculated. Filter context is mostly used for filtering
        structured data, e.g.

            Does this timestamp fall into the range 2015 to 2016?
            Is the status field set to "published"?

        Frequently used filters will be cached automatically by Elasticsearch, to speed up performance.

        Filter context is in effect whenever a query clause is passed to a filter parameter, such as the filter or
        must_not parameters in the bool query, the filter parameter in the constant_score query, or the filter
        aggregation.
        """
        if query_type not in self.supported_bool_filter_types:
            raise QueryTypeNotSupportedError

        query_list = []

        query_operand = "filter"

        for k, v in kwargs.items():
            query_data = collections.defaultdict(dict)
            if not isinstance(v, list):
                query_data[k] = v
                query_list.append({query_type: dict(query_data)})
            else:
                for each in v:
                    query_data[k] = each
                    query_list.append({query_type: dict(query_data)})

        self.filter_data["query"]["bool"][query_operand] = query_list

        return self

    def query(self, query_type: str = None, **kwargs):
        """
        In the query context, a query clause answers the question “How well does this document match this query clause?”
        Besides deciding whether or not the document matches, the query clause also calculates a relevance score in
        the _score metadata field.

        Query context is in effect whenever a query clause is passed to a query parameter, such as the query parameter
        in the search API.
        """

        if query_type not in self.supported_bool_query_types:
            raise QueryTypeNotSupportedError

        query_list = []

        query_operand = "must"

        for k, v in kwargs.items():
            query_data = collections.defaultdict(dict)
            if not isinstance(v, list):
                query_data[k] = v
                query_list.append({query_type: dict(query_data)})
            else:
                query_operand = "should"
                for each in v:
                    query_data[k] = each
                    query_list.append({query_type: dict(query_data)})

        self.filter_data["query"]["bool"][query_operand] = query_list

        return self

    def exclude(self, query_type: str = None, **kwargs):
        """
        In the query context, a query clause answers the question “How well does this document match this query clause?”
        Besides deciding whether or not the document matches, the query clause also calculates a relevance score in
        the _score metadata field.

        Query context is in effect whenever a query clause is passed to a query parameter, such as the query parameter
        in the search API.
        """

        if query_type not in self.supported_bool_exclude_types:
            raise QueryTypeNotSupportedError

        query_list = []

        query_operand = "must_not"

        for k, v in kwargs.items():
            query_data = collections.defaultdict(dict)
            if not isinstance(v, list):
                query_data[k] = v
                query_list.append({query_type: dict(query_data)})
            else:
                for each in v:
                    query_data[k] = each
                    query_list.append({query_type: dict(query_data)})

        self.filter_data["query"]["bool"][query_operand] = query_list

        return self

    def match_all(self):
        self.filter_data["query"]["bool"] = {"must": {"match_all": {}}}

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
