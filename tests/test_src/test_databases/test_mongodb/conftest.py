# test/test_src/test_databases/test_mongodb/conftest.py

import logging
from pprint import pprint
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
    return load_config("tests/configs/config-test-mgdb-fwd.yml")

@pytest.fixture(scope="session")
def load_data(config) -> list[dict]:
    pprint(config["general"]["data_file"])
    return load_json_data(config["general"]["data_file"])

@pytest.fixture(scope="session")
def logger(config) -> logging.Logger:
    return get_logger("mongodb_test_logger", config["general"]["log_file"])

@pytest.fixture(scope="session")
def mongodb_config(config) -> MongoDBConfig:
    return get_mongodb_config(config["mongodb"])

@pytest.fixture(scope="session")
def mongodb_client(mongodb_config, logger) -> MongoDBClient:
    
    client = MongoDBClient(mongodb_config, logger)
    
    client.uri = "localhost:27017"

    def custom_connect():
        try:
            client.client = MongoClient(client.uri) 
            client.logger.info("Connected to local MongoDB.")
            return True
        except Exception as e:
            client.logger.error(f"Connection error (custom): {e}")
            return False
    
    def custom_disconnect():
        if client.client:
            client.client.close()
            client.logger.info("Disconnected from MongoDB.")
        else:
            client.logger.warning("No active MongoDB connection to close.")
    
    
    client.connect = custom_connect
    client.disconnect = custom_disconnect
    return client

@pytest.fixture(scope="session")
def mongodb_repo(mongodb_client):
    return MongoDBRepository(mongodb_client)