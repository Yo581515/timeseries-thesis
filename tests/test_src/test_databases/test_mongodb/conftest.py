# test/test_src/test_databases/test_mongodb/conftest.py

import os
import logging
from pymongo import MongoClient
import pytest

from src.common.data_loader import load_json_data
from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.mongodb.config import MongoDBConfig, get_mongodb_config
from src.databases.mongodb.client import MongoDBClient
from src.databases.mongodb.mongodb_repository import MongoDBRepository


@pytest.fixture(scope="session")
def config() -> dict:
    config = {
        'general': {
            'mongodb_data_file': 'data/sample_data.json',
            'mongodb_log_file': 'logs/mongodb_forwarder.log'
        },
        'mongodb': {
            'MONGO_DB_USER': os.getenv('MONGO_DB_USER', ''),
            'MONGO_DB_PASSWORD': os.getenv('MONGO_DB_PASSWORD', ''),
            'MONGO_DB_CLUSTER': os.getenv('MONGO_DB_CLUSTER', ''),
            'MONGODB_DATABASE_NAME': 'timeseries_test_db',
            'MONGODB_COLLECTION_NAME': 'sensor_data_test_collection'
        }
    }
    return config

@pytest.fixture(scope="session")
def load_data(config) -> list[dict]:
    return load_json_data(config["general"]["mongodb_data_file"])

@pytest.fixture(scope="session")
def logger(config) -> logging.Logger:
    return get_logger("mongodb_test_logger", config["general"]["mongodb_log_file"])

@pytest.fixture(scope="session")
def mongodb_config(config) -> MongoDBConfig:
    return get_mongodb_config(config["mongodb"])

@pytest.fixture(scope="session")
def mongodb_client(mongodb_config, logger) -> MongoDBClient:
    
    client = MongoDBClient(mongodb_config, logger)
    
    # client.uri = "localhost:27017"

    # def custom_connect():
    #     try:
    #         client.client = MongoClient(client.uri) 
    #         client.logger.info("Connected to local MongoDB.")
    #         return True
    #     except Exception as e:
    #         client.logger.error(f"Connection error (custom): {e}")
    #         return False
    
    # def custom_disconnect():
    #     if client.client:
    #         client.client.close()
    #         client.logger.info("Disconnected from MongoDB.")
    #     else:
    #         client.logger.warning("No active MongoDB connection to close.")
    
    
    # client.connect = custom_connect
    # client.disconnect = custom_disconnect
    return client

@pytest.fixture(scope="session")
def mongodb_repo(mongodb_client) -> MongoDBRepository:
    return MongoDBRepository(mongodb_client)