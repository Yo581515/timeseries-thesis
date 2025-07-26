# src/databases/mongodb/mongodb.py

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import certifi
import logging

from src.databases.mongodb.config import MongoDBConfig

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

    def connect(self) -> bool:
        try:
            self.client = MongoClient(
                self.uri, server_api=ServerApi('1'), tlsCAFile=certifi.where())
            self.logger.info('Connected to MongoDB')
            return True
        except Exception as e:
            self.logger.error("Connection error: " + str(e))
            return False

    def disconnect(self) -> bool:
        if self.client:
            self.client.close()
            self.logger.info('Disconnected from MongoDB')
            return True
        else:
            self.logger.warning('No active MongoDB connection to disconnect')
        return False

if __name__ == '__main__':
    if True:
        print('^_^')