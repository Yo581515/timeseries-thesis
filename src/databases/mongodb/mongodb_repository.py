# src/databases/mongodb/mongodb_repo.py

from src.databases.mongodb.mongodb import MongoDBClient
from src.databases.mongodb.mongodb_functions.delete import delete_by_query
from src.databases.mongodb.mongodb_functions.insert import insert_one, insert_many
from src.databases.mongodb.mongodb_functions.fetch import find_by_query, aggregate

class MongoDBRepository:
    def __init__(self, mongodb_client: MongoDBClient):
        self.mongodb_client = mongodb_client
        
    def ping(self):
        return self.mongodb_client.ping()
    
    def connect(self):
        return self.mongodb_client.connect()
    
    def disconnect(self):
        self.mongodb_client.disconnect()
            
    def insert_one(self, data_point: dict) -> bool:
        if not self.connect():
            self.mongodb_client.logger.error("Failed to connect to MongoDB.")
            return False
        try:
            return insert_one(self.mongodb_client, data_point)
        finally:
            self.disconnect()

    def insert_many(self, data_points: list[dict]) -> bool:
        if not self.connect():
            self.mongodb_client.logger.error("Failed to connect to MongoDB.")
            return False
        try:
            return insert_many(self.mongodb_client, data_points)
        finally:
            self.disconnect()

    def delete_by_query(self, query: dict) -> bool:
        if not self.connect():
            self.mongodb_client.logger.error("Failed to connect to MongoDB.")
            return False
        try:
            return delete_by_query(self.mongodb_client, query)
        finally:
            self.disconnect()
            
    def find_by_query(self, query: dict) -> list[dict]:
        if not self.connect():
            self.mongodb_client.logger.error("Failed to connect to MongoDB.")
            return []
        try:
            return find_by_query(self.mongodb_client, query)
        finally:
            self.disconnect()

    def aggregate(self, pipeline: list[dict]) -> list[dict]:
        if not self.connect():
            self.mongodb_client.logger.error("Failed to connect to MongoDB.")
            return []
        try:
            return aggregate(self.mongodb_client, pipeline)
        finally:
            self.disconnect()
