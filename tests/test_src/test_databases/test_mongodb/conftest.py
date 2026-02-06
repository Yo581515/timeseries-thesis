# tests/test_src/test_databases/test_mongodb/conftest.py

import logging
import os
from pathlib import Path

import pytest

from src.common.config import load_config
from src.common.data_loader import load_json_data
from src.common.logger import get_logger
from src.databases.mongodb.config import MongoDBConfig, get_mongodb_config
from src.databases.mongodb.mongodb_repository import MongoDBRepository

TEST_CONFIG_PATH = Path("tests/configs/config-test-mgdb.yml")


@pytest.fixture(scope="session")
def config() -> dict:
    return load_config(str(TEST_CONFIG_PATH))


@pytest.fixture(scope="session")
def logger(config: dict) -> logging.Logger:
    return get_logger("mongodb_test_logger", config["general"]["mongodb_log_file"])


@pytest.fixture(scope="session")
def mongodb_config(config: dict) -> MongoDBConfig:
    return get_mongodb_config(config["database"])


@pytest.fixture(scope="session")
def raw_data(config: dict) -> list[dict]:
    return load_json_data(config["general"]["mongodb_data_file"])


@pytest.fixture
def mongodb_repo(mongodb_config: MongoDBConfig, logger: logging.Logger) -> MongoDBRepository:
    """Unit-test friendly repo fixture (does NOT connect)."""
    return MongoDBRepository(mongodb_config, logger)


@pytest.fixture
def mongodb_repo_connected(mongodb_repo: MongoDBRepository) -> MongoDBRepository:
    """
    Integration-test fixture (connects to a real MongoDB).
    Enabled only if RUN_MONGODB_INTEGRATION_TESTS=1.
    """
    if os.getenv("RUN_MONGODB_INTEGRATION_TESTS") != "1":
        pytest.skip("Mongo integration tests disabled. Set RUN_MONGODB_INTEGRATION_TESTS=1 to enable.")

    if not mongodb_repo.connect_and_cache():
        pytest.skip("Could not connect to MongoDB (connect_and_cache failed).")

    mongodb_repo.delete_by_query({})  # clean slate

    try:
        yield mongodb_repo
    finally:
        mongodb_repo.disconnect()