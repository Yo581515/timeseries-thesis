# tests/test_src/test_databases/test_mongodb/conftest.py

import logging
import os

import pytest

from src.common.config import load_config
from src.common.data_loader import load_json_data
from src.common.logger import get_logger
from src.databases.mongodb.config import MongoDBConfig, get_mongodb_config
from src.databases.mongodb.mongodb_repository import MongoDBRepository


@pytest.fixture(scope="session")
def config() -> dict:
    config_path = "./tests/configs/config-test-mgdb-fwd.yml"
    return load_config(config_path)


@pytest.fixture(scope="session")
def logger(config) -> logging.Logger:
    return get_logger("mongodb_test_logger", config["general"]["mongodb_log_file"])


@pytest.fixture(scope="session")
def mongodb_config(config) -> dict:
    return get_mongodb_config(config["mongodb"])


@pytest.fixture(scope="session")
def raw_data(config) -> list[dict]:
    return load_json_data(config["general"]["mongodb_data_file"])


@pytest.fixture(scope="function")
def mongodb_repo(mongodb_config : MongoDBConfig , logger: logging.Logger) -> MongoDBRepository:
    """
    Unit-test friendly repo fixture (does NOT connect).
    Use this in tests that mock repo.client / repo.collection.
    """
    return MongoDBRepository(mongodb_config, logger)


@pytest.fixture(scope="function")
def mongodb_repo_connected(mongodb_repo: MongoDBRepository) -> MongoDBRepository:
    """
    Integration-test fixture (connects to a real MongoDB).
    Skips automatically unless you set RUN_MONGO_INTEGRATION_TESTS=1.
    """
    if os.getenv("RUN_MONGO_INTEGRATION_TESTS", "0") != "1":
        pytest.skip("Mongo integration tests disabled. Set RUN_MONGO_INTEGRATION_TESTS=1 to enable.")

    if not mongodb_repo.connect_and_cache():
        pytest.skip("Could not connect to MongoDB (connect_and_cache failed).")

    # Clean slate per test
    mongodb_repo.delete_by_query({})

    try:
        yield mongodb_repo
    finally:
        # mongodb_repo.delete_by_query({})
        mongodb_repo.disconnect()