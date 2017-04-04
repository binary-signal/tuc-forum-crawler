from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson.objectid import ObjectId
import pprint

class MDB(object):
    __client = None
    __port = None
    __host = None
    __dbname =None
    __dbcollection =None

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
                raise ConnectionFailure('Failed to connect on MongoDB on host %s:s', self.__host, self.__port )
        except ConnectionFailure:
            print('fuck')

        return conn

    def num_of_docs(self):
        return self.__client[self.__dbname][self.__dbcollection].count()

    def search_by_id(self, id):
        res = self.__client[self.__dbname][self.__dbcollection].find_one({'id': ObjectId(id)})
        if res:
            return res

    def search_by_attr(self, attr, val):
        results = self.__client[self.__dbname][self.__dbcollection].find({attr: val})
        #res = self.__client[self.__dbname][self.__dbcollection].insert_one()
        for r in results:
            print(r)


    def insert(self, page={}):
        """
        page{link:
             title:
             body:
             lang:}
        """
        return self.__client[self.__dbname][self.__dbcollection].insert_one(page)

    def destroy(self):
        self.__client.close()




