# src/db/crud/insert.py

from src.databases.mongodb.mongodb import MongoDBClient

def drop_collection(mongo_client : MongoDBClient) -> bool:
    mongo_client.logger.info(f'Dropping collection: {mongo_client.collection_name}')
    
    try:
        db = mongo_client.client[mongo_client.database_name]
    except Exception as e:
        mongo_client.logger.error(f"Database error: {e}")
        return False

    try:
        collection = db[mongo_client.collection_name]
    except Exception as e:
        mongo_client.logger.error(f"Collection error: {e}")
        return False

    try:
        collection.drop()
        mongo_client.logger.info(f"Collection {mongo_client.collection_name} dropped successfully.")
    except Exception as e:
        mongo_client.logger.error(f"Error dropping collection: {e}")
        return False

    return True