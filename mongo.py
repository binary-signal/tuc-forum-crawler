from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson.objectid import ObjectId
from pprint import pprint


class MDB(object):
    def __init__(self, port, host, dbname, dbcollection):
        self.__port = port
        self.__host = host
        self.__dbname = dbname
        self.__dbcollection = dbcollection

        self.__client = self.__connect()

    def __connect(self):
        conn = None
        try:
            conn = MongoClient(self.__host, self.__port)
            if conn is None:
                raise ConnectionFailure('Mongo DB failed to connect  %s:s', self.__host, self.__port)
        except ConnectionFailure:
            print('ups')

        return conn

    def num_of_docs(self):
        return self.__client[self.__dbname][self.__dbcollection].count()

    def search_by_id(self, id_):
        res = self.__client[self.__dbname][self.__dbcollection].find_one({'id': ObjectId(id_)})
        if res:
            return res

    def search_by_attr(self, attr, val):
        cursor = self.__client[self.__dbname][self.__dbcollection].find({attr: val})
        for document in cursor:
            pprint(document)

    def insert(self, page={}):
        return self.__client[self.__dbname][self.__dbcollection].insert_one(page)

    def destroy(self):
        self.__client.close()
