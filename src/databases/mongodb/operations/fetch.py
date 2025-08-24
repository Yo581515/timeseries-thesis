# src/databases/mongodb/operations/fetch.py

from src.databases.mongodb.client import MongoDBClient

class FetchOperations:
    def __init__(self, mongodb_client: MongoDBClient):
        self.mongodb_client = mongodb_client

    def find_by_query(self, query: dict) -> list[dict]:
        self.mongodb_client.logger.info(f'Fetching documents by query: {query}')

        if query is None:
            self.mongodb_client.logger.error("No query provided for fetching.")
            return []

        try:
            db = self.mongodb_client.client[self.mongodb_client.database_name]
        except Exception as e:
            self.mongodb_client.logger.error(f"Database error: {e}")
            return []

        try:
            collection = db[self.mongodb_client.collection_name]
        except Exception as e:
            self.mongodb_client.logger.error(f"Collection error: {e}")
            return []

        try:
            results = collection.find(query)
            documents = list(results)
            self.mongodb_client.logger.info(f"Fetched {len(documents)} documents from {self.mongodb_client.collection_name} collection.")
            return documents
        except Exception as e:
            self.mongodb_client.logger.error(f"Fetch by query error: {e}")
            return []


    def aggregate(self, pipeline: list[dict]) -> list[dict]:
        self.mongodb_client.logger.info(f'Aggregating with pipeline: {pipeline}')

        if not pipeline:
            self.mongodb_client.logger.error("No aggregation pipeline provided.")
            return []

        try:
            db = self.mongodb_client.client[self.mongodb_client.database_name]
        except Exception as e:
            self.mongodb_client.logger.error(f"Database error: {e}")
            return []

        try:
            collection = db[self.mongodb_client.collection_name]
        except Exception as e:
            self.mongodb_client.logger.error(f"Collection error: {e}")
            return []

        try:
            results = collection.aggregate(pipeline)
            documents = list(results)
            self.mongodb_client.logger.info(f"Aggregated {len(documents)} documents from {self.mongodb_client.collection_name} collection.")
            return documents
        except Exception as e:
            self.mongodb_client.logger.error(f"Aggregation error: {e}")
            return []