# tests/test_src/test_databases/test_mongodb/test_config_unit.py

import pytest

from src.databases.mongodb.config import get_mongodb_config, MongoDBConfigurationException


def test_get_mongodb_config_reads_mode(mongodb_config):
    # mongodb_config fixture comes from conftest.py
    assert hasattr(mongodb_config, "MONGO_MODE")
    assert mongodb_config.MONGO_MODE in ("local", "atlas")


def test_get_mongodb_config_default_mode_local():
    cfg = {
        "MONGO_DB_USER": "u",
        "MONGO_DB_PASSWORD": "p",
        "MONGO_DB_CLUSTER": "cluster.example.mongodb.net",
        "MONGODB_DATABASE_NAME": "db",
        "MONGODB_COLLECTION_NAME": "col",
        # MONGO_MODE missing on purpose
    }
    mongodb_config = get_mongodb_config(cfg)
    assert mongodb_config.MONGO_MODE == "local"


def test_get_mongodb_config_mode_atlas():
    cfg = {
        "MONGO_DB_USER": "u",
        "MONGO_DB_PASSWORD": "p",
        "MONGO_DB_CLUSTER": "cluster.example.mongodb.net",
        "MONGODB_DATABASE_NAME": "db",
        "MONGODB_COLLECTION_NAME": "col",
        "MONGO_MODE": "atlas",
    }
    mongodb_config = get_mongodb_config(cfg)
    assert mongodb_config.MONGO_MODE == "atlas"


@pytest.mark.parametrize("missing_key", [
    "MONGO_DB_USER",
    "MONGO_DB_PASSWORD",
    "MONGO_DB_CLUSTER",
    "MONGODB_DATABASE_NAME",
    "MONGODB_COLLECTION_NAME",
])
def test_get_mongodb_config_missing_keys_raise(missing_key):
    cfg = {
        "MONGO_DB_USER": "u",
        "MONGO_DB_PASSWORD": "p",
        "MONGO_DB_CLUSTER": "cluster.example.mongodb.net",
        "MONGODB_DATABASE_NAME": "db",
        "MONGODB_COLLECTION_NAME": "col",
        "MONGO_MODE": "local",
    }
    cfg.pop(missing_key)

    with pytest.raises(MongoDBConfigurationException):
        get_mongodb_config(cfg)