# src/db/crud/insert.py

import logging


def insert_one(db, collection_name : str, data_point : dict, logger : logging.Logger) -> bool:
    try:
        logger.info(f'Inserting data point: {data_point}')

        if not data_point:
            logger.error("No data to insert.")
            return False
        
        try:
            collection = db[collection_name]
        except Exception as e:
            logger.error("Collection error: " + str(e))
            return False

        result = collection.insert_one(data_point)
        logger.info("Successfully inserted into " + collection_name + " collection.")
        return True

    except Exception as e:
        logger.error(f"Insert error: {e}")
        return False


def insert_many(db, collection_name : str, data_points : list, logger : logging.Logger) -> bool:
    try:
        logger.info(f'Inserting multiple data points: {data_points[0]}, ... {data_points[-1]}')

        if not data_points:
            logger.error("No data to insert.")
            return False
        
        try:
            collection = db[collection_name]
        except Exception as e:
            logger.error("Collection error: " + str(e))
            return False

        result = collection.insert_many(data_points)
        logger.info(f"Successfully inserted {len(data_points)} documents into {collection_name} collection.")
        return True

    except Exception as e:
        logger.error(f"Insert many error: {e}")
        return False