# test/test_src/test_databases/test_mongodb/conftest.py

import logging
import pytest

from src.common.data_loader import load_json_data
from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.mongodb.config import MongoDBConfig, get_mongodb_config
from src.databases.mongodb.client import MongoDBClient
from src.databases.mongodb.mongodb_repository import MongoDBRepository


@pytest.fixture(scope="session")
def config() -> dict:
    config_path = './tests/configs/config-test-mgdb-fwd.yml'
    config = load_config(config_path)
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
    return client

@pytest.fixture(scope="session")
def mongodb_repo(mongodb_client) -> MongoDBRepository:
    return MongoDBRepository(mongodb_client)