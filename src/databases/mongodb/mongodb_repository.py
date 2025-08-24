# src/databases/mongodb/mongodb_repo.py

from src.databases.mongodb.client import MongoDBClient
from src.databases.mongodb.operations.delete import DeleteOperations
from src.databases.mongodb.operations.insert import InsertOperations
from src.databases.mongodb.operations.fetch import FetchOperations
from src.databases.mongodb.operations.update import UpdateOperations

class MongoDBRepository:
    def __init__(self, mongodb_client: MongoDBClient):
        self.mongodb_client = mongodb_client
        self.insert_operations = InsertOperations(mongodb_client)
        self.delete_operations = DeleteOperations(mongodb_client)
        self.fetch_operations = FetchOperations(mongodb_client)
        self.update_operations = UpdateOperations(mongodb_client)

    def ping(self):
        self.mongodb_client.connect()
        try:
            self.mongodb_client.client.admin.command('ping')
            self.mongodb_client.logger.info(
                "Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            self.mongodb_client.logger.error(str(e))
            return False
        finally:
            self.mongodb_client.disconnect()
        return True
            
    def insert_one(self, data_point: dict) -> bool:
        if not self.mongodb_client.connect():
            self.mongodb_client.logger.error("Failed to connect to MongoDB.")
            return False
        try:
            return self.insert_operations.insert_one(data_point)
        finally:
            self.mongodb_client.disconnect()

    def insert_many(self, data_points: list[dict]) -> bool:
        if not self.mongodb_client.connect():
            self.mongodb_client.logger.error("Failed to connect to MongoDB.")
            return False
        try:
            return self.insert_operations.insert_many(data_points)
        finally:
            self.mongodb_client.disconnect()

    def delete_by_query(self, query: dict) -> bool:
        if not self.mongodb_client.connect():
            self.mongodb_client.logger.error("Failed to connect to MongoDB.")
            return False
        try:
            return self.delete_operations.delete_by_query(query)
        finally:
            self.mongodb_client.disconnect()

    def find_by_query(self, query: dict) -> list[dict]:
        if not self.mongodb_client.connect():
            self.mongodb_client.logger.error("Failed to connect to MongoDB.")
            return []
        try:
            return self.fetch_operations.find_by_query(query)
        finally:
            self.mongodb_client.disconnect()

    def aggregate(self, pipeline: list[dict]) -> list[dict]:
        if not self.mongodb_client.connect():
            self.mongodb_client.logger.error("Failed to connect to MongoDB.")
            return []
        try:
            return self.fetch_operations.aggregate(pipeline)
        finally:
            self.mongodb_client.disconnect()

    def update_one(self, query: dict, update: dict) -> bool:
        if not self.mongodb_client.connect():
            self.mongodb_client.logger.error("Failed to connect to MongoDB.")
            return False
        try:
            return self.update_operations.update_one(query, update)
        finally:
            self.mongodb_client.disconnect()

    def update_many(self, query: dict, update: dict) -> bool:
        if not self.mongodb_client.connect():
            self.mongodb_client.logger.error("Failed to connect to MongoDB.")
            return False
        try:
            return self.update_operations.update_many(query, update)
        finally:
            self.mongodb_client.disconnect()
