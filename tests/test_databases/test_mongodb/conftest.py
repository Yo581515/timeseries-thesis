# src/tests/test_databases/test_mongodb/conftest.py

import pytest

from src.common.config import load_config
from src.common.logger import get_logger
from src.common.data_loader import load_json_data
from src.databases.mongodb.config import get_mongodb_config
from src.databases.mongodb.client import MongoDBClient
from src.databases.mongodb.mongodb_repository import MongoDBRepository
from src.databases.mongodb.utils.data_utils import resolve_data


@pytest.fixture(scope="session")
def config():
    return load_config("configs/config-test-mgdb-fwd.yml")

@pytest.fixture(scope="session")
def logger(config):
    return get_logger("mongodb_test_logger", config["general"]["log_file"])

@pytest.fixture(scope="session")
def data(config, logger):
    raw_data = load_json_data(config["general"]["data_file"])
    return [resolve_data(doc, logger) for doc in raw_data]

@pytest.fixture(scope="session")
def mongodb_client(config, logger):
    mongodb_config = get_mongodb_config(config["mongodb"])
    return MongoDBClient(mongodb_config, logger)

@pytest.fixture(scope="session")
def mongodb_repo(mongodb_client):
    return MongoDBRepository(mongodb_client)