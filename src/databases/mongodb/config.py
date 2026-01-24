# src/databases/mongodb/config.py

class MongoDBConfigurationException(Exception):
    pass


class MongoDBConfig:
    def __init__(self, username, password, cluster, database, collection, mode="local"):
        self.MONGO_DB_USER = username
        self.MONGO_DB_PASSWORD = password
        self.MONGO_DB_CLUSTER = cluster
        self.MONGODB_DATABASE_NAME = database
        self.MONGODB_COLLECTION_NAME = collection
        self.MONGO_MODE = mode  # "local" | "atlas"

    def __str__(self):
        return (
            "MongoDB Configuration:\n"
            f"Mode: {self.MONGO_MODE}\n"
            f"Cluster: {self.MONGO_DB_CLUSTER}\n"
            f"Database: {self.MONGODB_DATABASE_NAME}\n"
            f"Collection: {self.MONGODB_COLLECTION_NAME}\n"
        )


def get_mongodb_config(config_dict: dict) -> MongoDBConfig:
    try:
        return MongoDBConfig(
            username=config_dict["MONGO_DB_USER"],
            password=config_dict["MONGO_DB_PASSWORD"],
            cluster=config_dict["MONGO_DB_CLUSTER"],
            database=config_dict["MONGODB_DATABASE_NAME"],
            collection=config_dict["MONGODB_COLLECTION_NAME"],
            mode=config_dict["MONGO_MODE"] if "MONGO_MODE" in config_dict else "local",
        )
    except KeyError as e:
        raise MongoDBConfigurationException(f"Missing config key: {e}")