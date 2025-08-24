# src/databases/mongodb/operations/delete.py

from src.databases.mongodb.client import MongoDBClient

class DeleteOperations:
    def __init__(self, mongodb_client: MongoDBClient):
        self.mongodb_client = mongodb_client

    def delete_by_query(self, query: dict) -> bool:
        self.mongodb_client.logger.info(f'Deleting documents by query: {query}')

        if query is None:
            self.mongodb_client.logger.error("No query provided for deletion.")
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
            result = collection.delete_many(query)
            self.mongodb_client.logger.info(f"Deleted {result.deleted_count} documents from {self.mongodb_client.collection_name} collection.")
            return True
        except Exception as e:
            self.mongodb_client.logger.error(f"Delete by query error: {e}")
            return False