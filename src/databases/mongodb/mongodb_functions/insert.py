# src/databases/mongodb/mongodb_functions/insert.py

from src.databases.mongodb.mongodb import MongoDBClient


def insert_one(mongodb_client : MongoDBClient, data_point: dict)  -> bool:
    
    mongodb_client.logger.info(f'Inserting data point: {data_point}')
    
    if not data_point:
        mongodb_client.logger.error("No data to insert.")
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
        result = collection.insert_one(data_point)
        mongodb_client.logger.info("Successfully inserted into " + mongodb_client.collection_name + " collection.")
        return True

    except Exception as e:
        mongodb_client.logger.error(f"Insert one error: {e}")
        return False



def insert_many(mongodb_client : MongoDBClient, data_points : list[dict]) -> bool:

        mongodb_client.logger.info(f'Inserting multiple data points: {data_points[0]}, ... {data_points[-1]}')

        if not data_points or not isinstance(data_points, list) or len(data_points) == 0:
            mongodb_client.logger.error("No data to insert.")
            return False

        try:
            db = mongodb_client.client[mongodb_client.database_name]
        except Exception as e:
            mongodb_client.logger.error(f"Database error: {e}")
            return False
            
        try:
            collection = db[mongodb_client.collection_name]
        except Exception as e:
            mongodb_client.logger.error("Collection error: " + str(e))
            return False

        try:
            result = collection.insert_many(data_points)
            mongodb_client.logger.info(f"Successfully inserted {len(data_points)} documents into {mongodb_client.collection_name} collection.")
            return True
        except Exception as e:
            mongodb_client.logger.error(f"Insert many error: {e}")
            return False