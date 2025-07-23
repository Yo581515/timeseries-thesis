# src/databases/mongodb/mongodb_repo.py

from src.databases.mongodb.mongodb import MongoDBClient
from src.databases.mongodb.functions.delete import drop_collection

class MongoDBRepository:
    def __init__(self, mongoDBClient: MongoDBClient):
        self.mongoDBClient = mongoDBClient
        
    def ping(self):
        return self.mongoDBClient.ping()
    
    def connect(self):
        return self.mongoDBClient.connect()
    
    def disconnect(self):
        self.mongoDBClient.disconnect()

    def drop_collection(self):
        if not self.connect():
            self.mongoDBClient.logger.error("Failed to connect to MongoDB.")
            return False
        try:
            return drop_collection(self.mongoDBClient)
        finally:
            self.disconnect()
