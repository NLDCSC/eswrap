from common.EsCursor import EsCursor


class EsHandler(object):
    """
    The EsHandler
    """

    def __init__(self, es_connection, index):
        self.es_connection = es_connection
        self.index = index

    def find(self, filter=None):
        """
        Query the api.

        :return: Reference to the EsCursor
        :rtype: EsCursor
        """

        return EsCursor(self, filter)

    def find_one(self, filter=None):
        """
        Query the api.

        :return: Data or None
        :rtype: object
        """

        cursor = self.find(filter)

        for result in cursor.limit(-1):
            return result
        return None

    def count(self, filter=None):
        """

        :return:
        :rtype:
        """
        if filter is None:
            data = {"query": {"match_all": {}}}
        else:
            data = {"query": {"match": filter}}

        return self.es_connection.count(index=self.index, body=data)['count']

    def __repr__(self):
        """return a string representation of the obj EsHandler"""
        return "<< EsHandler: {} >>".format(self.es_connection.remote.transport.hosts)
