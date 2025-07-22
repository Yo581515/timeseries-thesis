# src/db/crud/insert.py

def insert_one(db, collection_name, data_point, logger):
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