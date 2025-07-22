# src/databases/mongodb/mongodb.py

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import certifi
import logging

from src.databases.configs.mongodb_config import MongoDBConfig
from src.databases.mongodb.crud.insert import insert_one

'''def convert_time(one_data_point):
    try:
        time = one_data_point['time']
        date_time = datetime.datetime.fromisoformat(time)
        one_data_point['time'] = date_time
        return one_data_point
    except Exception as e:
        logging.error("Convert time error: " + str(e))
        return None
'''

class MongoDBClient:
    def __init__(self, mongodb_config: MongoDBConfig, logging : logging.Logger):
        self.logging = logging

        self.cluster = mongodb_config.MONGO_DB_CLUSTER
        self.database_name = mongodb_config.MONGODB_DATABASE_NAME
        self.collection_name = mongodb_config.MONGODB_COLLECTION_NAME

        self.MONGO_DB_USER = mongodb_config.MONGO_DB_USER
        self.MONGO_DB_PASSWORD = mongodb_config.MONGO_DB_PASSWORD

        self.uri = f'mongodb+srv://{self.MONGO_DB_USER}:{self.MONGO_DB_PASSWORD}@{self.cluster}'
        
        self.client = None

    def connect(self):
        try:
            self.client = MongoClient(
                self.uri, server_api=ServerApi('1'), tlsCAFile=certifi.where())
            return True
        except Exception as e:
            self.logging.error("Connection error: " + str(e))
            return False
        
    def disconnect(self):
        self.client.close()
        self.logging.info('Disconnected')

    def ping(self):
        self.connect()
        try:
            self.client.admin.command('ping')
            self.logging.info(
                "Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            self.logging.error(str(e))
            return False
        self.disconnect()
        return True
    
    
    def insert_one(self, one_data_point):
        if not self.connect():
            self.logging.error("Failed to connect to MongoDB.")
            return False
        try:
            db = self.client[self.database_name]
        except Exception as e:
            self.logging.error("Database error: " + str(e))
            self.disconnect()
            return False

        success = insert_one(db, self.collection_name, one_data_point, self.logging)
        self.disconnect()
        return success

'''    def insert(self, one_data_point):
        if not self.connect():
            self.logging.error("Failed to connect to MongoDB.")
            return False
        
        try:
            db = self.client[self.database_name]
        except Exception as e:
            self.logging.error("Database error: " + str(e))
            self.disconnect()
            return False
        
        try:
            collection = db[self.collection_name]
        except Exception as e:
            self.logging.error("Collection error: " + str(e))
            self.disconnect()
            return False

        try:
            self.logging.info(f'Inserting data point: {one_data_point}')

            # one_data_point = convert_time(one_data_point)

            if not one_data_point:
                # self.logging.error("Failed to convert time.")
                self.disconnect()

                return False
            else:
                res = collection.insert_one(one_data_point)
                self.logging.info("Successfully inserted into " + self.collection_name + " collection.")

            self.logging.info(f'Successfully inserted data points')

            self.disconnect()

            return True

        except Exception as e:
            self.logging.error("Insert error: " + str(e))
            self.logging.error("Error occurred during insert: " + str(e))
            self.disconnect()
            return False
'''


if __name__ == '__main__':
    if True:
        print('^_^')