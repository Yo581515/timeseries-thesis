# test/test_databases/test_mongodb/conftest.py

from src.common.config import load_config
from src.common.logger import get_logger
from src.common.data_loader import load_json_data

from src.databases.mongodb.config import get_mongodb_config
from src.databases.mongodb.client import MongoDBClient
from src.databases.mongodb.mongodb_repository import MongoDBRepository

from src.databases.mongodb.utils.data_utils import resolve_data


def setup_mongodb_env(config_path: str = 'configs/config-test-mgdb-fwd.yml'):
    config = load_config(config_path)

    # Setup logger
    logger = get_logger("mongodb_test_logger", config['general']['log_file'])

    # Load and resolve data
    raw_data = load_json_data(config['general']['data_file'])
    resolved_data = [resolve_data(doc, logger) for doc in raw_data]

    # Setup MongoDB client and repository
    mongodb_config = get_mongodb_config(config['mongodb'])
    mongo_client = MongoDBClient(mongodb_config, logger)
    mongo_repo = MongoDBRepository(mongo_client)

    return {
        "config": config,
        "logger": logger,
        "data": resolved_data,
        "client": mongo_client,
        "repository": mongo_repo
    }