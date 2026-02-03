# src/databases/mongodb/config.py

class MongoDBConfigurationException(Exception):
    pass


class MongoDBConfig:
    def __init__(self, username, password, cluster, host, port, database, collection, mode):
        self.MONGO_DB_USER = username
        self.MONGO_DB_PASSWORD = password
        
        # for MongoDB Atlas connections
        self.MONGO_DB_CLUSTER = cluster
        
        # for local/container connections
        self.MONGO_DB_HOST = host
        self.MONGO_DB_PORT = port
        
        self.MONGODB_DATABASE_NAME = database
        self.MONGODB_COLLECTION_NAME = collection
        
        self.MONGO_MODE = mode  # "local" | "atlas"

    def __str__(self):
        return (
            "MongoDB Configuration:\n"
            f"Mode: {self.MONGO_MODE}\n"
            
            f"Cluster: {self.MONGO_DB_CLUSTER}\n"
            
            f"Host: {self.MONGO_DB_HOST}\n"
            f"Port: {self.MONGO_DB_PORT}\n"
            
            f"Database: {self.MONGODB_DATABASE_NAME}\n"
            f"Collection: {self.MONGODB_COLLECTION_NAME}\n"
        )


def get_mongodb_config(config_dict: dict) -> MongoDBConfig:
    try:
        return MongoDBConfig(
            username=config_dict["MONGO_DB_USER"],
            password=config_dict["MONGO_DB_PASSWORD"],
            
            cluster=config_dict["MONGO_DB_CLUSTER"],
            
            host=config_dict["MONGO_DB_HOST"],
            port=config_dict["MONGO_DB_PORT"],
            
            database=config_dict["MONGODB_DATABASE_NAME"],
            collection=config_dict["MONGODB_COLLECTION_NAME"],
            
            mode=config_dict["MONGO_MODE"],
        )
    except KeyError as e:
        raise MongoDBConfigurationException(f"Missing config key: {e}")