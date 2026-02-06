# src/databases/mongodb/client.py

import logging
from pymongo import MongoClient
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import certifi



from src.databases.mongodb.config import MongoDBConfig

class MongoDBClient:
    def __init__(self, mongodb_config : MongoDBConfig, logger: logging.Logger):
        self.logger = logger

        self.username = mongodb_config.MONGODB_USER
        self.password = mongodb_config.MONGODB_PASSWORD
        
        self.cluster = mongodb_config.MONGODB_CLUSTER
        
        self.host = mongodb_config.MONGODB_HOST
        self.port = mongodb_config.MONGODB_PORT

        self.database_name = mongodb_config.MONGODB_DATABASE_NAME
        self.collection_name = mongodb_config.MONGODB_COLLECTION_NAME

        self.mongo_mode = mongodb_config.MONGODB_MODE  # localhost/mongodb | atlas

        self.client = None
        
        

    def _build_uri(self) -> str:
        if self.mongo_mode == "localhost":
            return f"mongodb://{self.host}:{self.port}/"
        elif self.mongo_mode == "container":
            return f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/?authSource=admin"
        elif self.mongo_mode == "atlas":
            return f"mongodb+srv://{self.username}:{self.password}@{self.cluster}/?retryWrites=true&w=majority"
         


        raise ValueError(f"Invalid mongo_mode: {self.mongo_mode!r}")


    def connect(self) -> bool:
        try:
            if self.client is not None:
                return True

            uri = self._build_uri()
            
            if self.mongo_mode == "atlas":
                self.client = MongoClient(
                    uri,
                    server_api=ServerApi("1"),
                    tlsCAFile=certifi.where(),
                    serverSelectionTimeoutMS=5000,
                )
            elif self.mongo_mode in ["localhost", "container"]:
                self.client = MongoClient(uri, serverSelectionTimeoutMS=3000)
            else:
                raise ValueError(f"Invalid mongo_mode: {self.mongo_mode!r}")

            # Force real connection check
            self.client.admin.command("ping")
            self.logger.info(
                "Connected to MongoDB (mode=%s, db=%s, collection=%s)",
                self.mongo_mode,
                self.database_name,
                self.collection_name,
            )
            return True

        except Exception as e:
            self.logger.exception("MongoDB connect failed: %s", e)
            self.client = None
            return False

    def disconnect(self) -> bool:
        try:
            if self.client:
                self.client.close()
            self.client = None
            self.logger.info("Disconnected from MongoDB")
            return True
        except Exception as e:
            self.logger.exception("MongoDB disconnect error: %s", e)
            return False