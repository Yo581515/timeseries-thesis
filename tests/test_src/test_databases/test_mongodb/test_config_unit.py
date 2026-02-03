# tests/test_src/test_databases/test_mongodb/test_config_unit.py

import pytest

from src.databases.mongodb.config import MongoDBConfig, get_mongodb_config, MongoDBConfigurationException
    

REQUIRED_KEYS = (
    "MONGO_DB_USER",
    "MONGO_DB_PASSWORD",
    "MONGO_DB_CLUSTER",
    "MONGO_DB_HOST",
    "MONGO_DB_PORT",
    "MONGODB_DATABASE_NAME",
    "MONGODB_COLLECTION_NAME",
    "MONGO_MODE",
)


def make_cfg(**overrides) -> dict:
    """
    Base valid config dict. Override anything as needed in tests.
    """
    cfg = {
        "MONGO_DB_USER": "u",
        "MONGO_DB_PASSWORD": "p",
        "MONGO_DB_CLUSTER": "cluster.example.mongodb.net",
        "MONGO_DB_HOST": "localhost",
        "MONGO_DB_PORT": 27017,
        "MONGODB_DATABASE_NAME": "db",
        "MONGODB_COLLECTION_NAME": "col",
        "MONGO_MODE": "atlas",
    }
    cfg.update(overrides)
    return cfg


def test_fixture_mongodb_config_has_valid_mode(mongodb_config: MongoDBConfig):
    assert mongodb_config.MONGO_MODE in ("localhost", "atlas", "container")


@pytest.mark.parametrize("mode", ["atlas", "localhost", "container"])
def test_get_mongodb_config_reads_mode(mode: str):
    mongodb_config = get_mongodb_config(make_cfg(MONGO_MODE=mode))
    assert mongodb_config.MONGO_MODE == mode


@pytest.mark.parametrize("missing_key", REQUIRED_KEYS)
def test_get_mongodb_config_missing_keys_raise(missing_key: str):   
    cfg = make_cfg()
    cfg.pop(missing_key)

    with pytest.raises(MongoDBConfigurationException) as exc:
        get_mongodb_config(cfg)

    # Optional: ensure error message points to the missing key
    assert missing_key in str(exc.value)