class EsCursor(object):
    """
    The EsCursor
    """

    def __init__(self, es_handler, filter=None, limit=None, skip=None, sort=None):
        """
        Create a new CveSearchApi object.

        :param es_handler: EsHandler object
        :type es_handler: EsHandler
        :param filter: Filter to be used when querying data
        :type filter: dict
        :param limit: Limit value
        :type limit: int
        :param skip: Skip value
        :type skip: int
        :param sort: Sort value
        :type sort: tuple
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
        Endpoint for free query to cve search data
        """

        results = self.EsHandler.es_connection.search(index=self.EsHandler.index, body=self.__data)

        if isinstance(results, str):
            self.data_queue = None
            return

        try:
            if len(results["hits"]["hits"]) > 0:
                self.data_queue = [x["_source"] for x in results["hits"]["hits"]]
        except Exception:
            self.data_queue = results

    def limit(self, value):
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

    def skip(self, value):
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

    def sort(self, values):
        """
        A comma-separated list of <field>:<direction> pairs

        :param values: A comma-separated list of <field>:<direction> pairs
        :type values: list
        :return: EsCursor object
        :rtype: EsCursor
        """
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
