# src/databases/mongodb/mongodb_functions/delete.py

from src.databases.mongodb.client import MongoDBClient

def delete_by_query(mongodb_client : MongoDBClient, query: dict) -> bool:
    mongodb_client.logger.info(f'Deleting documents by query: {query}')
    
    if query is None:
        mongodb_client.logger.error("No query provided for deletion.")
        return False
    try:
        db = mongodb_client.client[mongodb_client.database_name]
    except Exception as e:
        mongodb_client.logger.error(f"Database error: {e}")
        return False

    try:
        collection = db[mongodb_client.collection_name]
    except Exception as e:
        mongodb_client.logger.error(f"Collection error: {e}")
        return False

    try:
        result = collection.delete_many(query)
        mongodb_client.logger.info(f"Deleted {result.deleted_count} documents from {mongodb_client.collection_name} collection.")
        return True
    except Exception as e:
        mongodb_client.logger.error(f"Delete by query error: {e}")
        return False