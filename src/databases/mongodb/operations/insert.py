# src/databases/mongodb/mongodb_functions/insert.py

from src.databases.mongodb.client import MongoDBClient

class InsertOperations:
    def __init__(self, mongodb_client: MongoDBClient):
        self.mongodb_client = mongodb_client

    def insert_one(self, data_point: dict)  -> bool:
        
        self.mongodb_client.logger.info(f'Inserting data point: {data_point}')
        
        if not data_point:
            self.mongodb_client.logger.error("No data to insert.")
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
            result = collection.insert_one(data_point)
            self.mongodb_client.logger.info("Successfully inserted into " + self.mongodb_client.collection_name + " collection.")
            return True

        except Exception as e:
            self.mongodb_client.logger.error(f"Insert one error: {e}")
            return False



    def insert_many(self, data_points : list[dict]) -> bool:

            self.mongodb_client.logger.info(f'Inserting multiple data points: {data_points[0]}, ... {data_points[-1]}')

            if not data_points or not isinstance(data_points, list) or len(data_points) == 0:
                self.mongodb_client.logger.error("No data to insert.")
                return False

            try:
                db = self.mongodb_client.client[self.mongodb_client.database_name]
            except Exception as e:
                self.mongodb_client.logger.error(f"Database error: {e}")
                return False
                
            try:
                collection = db[self.mongodb_client.collection_name]
            except Exception as e:
                self.mongodb_client.logger.error("Collection error: " + str(e))
                return False

            try:
                result = collection.insert_many(data_points)
                self.mongodb_client.logger.info(f"Successfully inserted {len(data_points)} documents into {self.mongodb_client.collection_name} collection.")
                return True
            except Exception as e:
                self.mongodb_client.logger.error(f"Insert many error: {e}")
                return False