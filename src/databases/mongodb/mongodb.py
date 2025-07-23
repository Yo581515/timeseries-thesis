# src/databases/mongodb/mongodb.py

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import certifi
import logging

from src.databases.configs.mongodb_config import MongoDBConfig

class MongoDBClient:
    def __init__(self, mongodb_config: MongoDBConfig, logger: logging.Logger):

        self.logger = logger

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
            self.logger.error("Connection error: " + str(e))
            return False
        
    def disconnect(self):
        self.client.close()
        self.logger.info('Disconnected')
        

    def ping(self):
        self.connect()
        try:
            self.client.admin.command('ping')
            self.logger.info(
                "Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            self.logger.error(str(e))
            return False
        self.disconnect()
        return True

if __name__ == '__main__':
    if True:
        print('^_^')