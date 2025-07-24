# src/databases/mongodb/mongodb_functions/fetch.py

from src.databases.mongodb.client import MongoDBClient

def find_by_query(mongodb_client: MongoDBClient, query: dict) -> list[dict]:
    mongodb_client.logger.info(f'Fetching documents by query: {query}')
    
    if query is None:
        mongodb_client.logger.error("No query provided for fetching.")
        return []

    try:
        db = mongodb_client.client[mongodb_client.database_name]
    except Exception as e:
        mongodb_client.logger.error(f"Database error: {e}")
        return []

    try:
        collection = db[mongodb_client.collection_name]
    except Exception as e:
        mongodb_client.logger.error(f"Collection error: {e}")
        return []

    try:
        results = collection.find(query)
        documents = list(results)
        mongodb_client.logger.info(f"Fetched {len(documents)} documents from {mongodb_client.collection_name} collection.")
        return documents
    except Exception as e:
        mongodb_client.logger.error(f"Fetch by query error: {e}")
        return []

def aggregate(mongodb_client: MongoDBClient, pipeline: list[dict]) -> list[dict]:
    mongodb_client.logger.info(f'Aggregating with pipeline: {pipeline}')
    
    if not pipeline:
        mongodb_client.logger.error("No aggregation pipeline provided.")
        return []

    try:
        db = mongodb_client.client[mongodb_client.database_name]
    except Exception as e:
        mongodb_client.logger.error(f"Database error: {e}")
        return []

    try:
        collection = db[mongodb_client.collection_name]
    except Exception as e:
        mongodb_client.logger.error(f"Collection error: {e}")
        return []

    try:
        results = collection.aggregate(pipeline)
        documents = list(results)
        mongodb_client.logger.info(f"Aggregated {len(documents)} documents from {mongodb_client.collection_name} collection.")
        return documents
    except Exception as e:
        mongodb_client.logger.error(f"Aggregation error: {e}")
        return []