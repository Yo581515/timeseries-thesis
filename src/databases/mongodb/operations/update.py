# src/databases/mongodb/operations/update.py

from src.databases.mongodb.client import MongoDBClient

class UpdateOperations:
    def __init__(self, mongodb_client: MongoDBClient):
        self.mongodb_client = mongodb_client

    def update_one(self, query: dict, update: dict) -> bool:
        self.mongodb_client.logger.info(f'Updating one document by query: {query} with update: {update}')

        if query is None or update is None:
            self.mongodb_client.logger.error("No query or update provided for update.")
            return False
        
        try:
            db = self.mongodb_client.client[self.mongodb_client.database_name]
        except Exception as e:
            self.mongodb_client.logger.error(f"Database error: {e}")
            return False

        try:
            collection = db[self.mongodb_client.collection_name]
        except Exception as e:
            self.mongodb_client.logger.error(f"Collection error: {e}")
            return False

        try:
            result = collection.update_one(query, {"$set": update})
            self.mongodb_client.logger.info(f"Updated {result.modified_count} documents in {self.mongodb_client.collection_name} collection.")
            return True
        except Exception as e:
            self.mongodb_client.logger.error(f"Update by query error: {e}")
            return False


    def update_many(self, query: dict, update: dict) -> bool:
        self.mongodb_client.logger.info(f'Updating many documents by query: {query} with update: {update}')

        if query is None or update is None:
            self.mongodb_client.logger.error("No query or update provided for update.")
            return False

        try:
            db = self.mongodb_client.client[self.mongodb_client.database_name]
        except Exception as e:
            self.mongodb_client.logger.error(f"Database error: {e}")
            return False

        try:
            collection = db[self.mongodb_client.collection_name]
        except Exception as e:
            self.mongodb_client.logger.error(f"Collection error: {e}")
            return False

        try:
            result = collection.update_many(query, {"$set": update})
            self.mongodb_client.logger.info(f"Updated {result.modified_count} documents in {self.mongodb_client.collection_name} collection.")
            return True
        except Exception as e:
            self.mongodb_client.logger.error(f"Update by query error: {e}")
            return False