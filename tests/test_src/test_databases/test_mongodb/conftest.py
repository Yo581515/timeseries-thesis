# test/test_src/test_databases/test_mongodb/conftest.py

import logging
import pytest

from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.mongodb.config import MongoDBConfig, get_mongodb_config
from src.databases.mongodb.client import MongoDBClient
from src.databases.mongodb.mongodb_repository import MongoDBRepository


@pytest.fixture(scope="session")
def config() -> dict:
    return load_config("configs/config-test-mgdb-fwd.yml")

@pytest.fixture(scope="session")
def logger(config) -> logging.Logger:
    return get_logger("mongodb_test_logger", config["general"]["log_file"])

@pytest.fixture(scope="session")
def mongodb_config(config) -> MongoDBConfig:
    return get_mongodb_config(config["mongodb"])

@pytest.fixture(scope="session")
def mongodb_client(mongodb_config, logger) -> MongoDBClient:
    return MongoDBClient(mongodb_config, logger)

@pytest.fixture(scope="session")
def mongodb_repo(mongodb_client):
    return MongoDBRepository(mongodb_client)