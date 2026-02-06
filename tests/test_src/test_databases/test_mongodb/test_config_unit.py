# tests/test_src/test_databases/test_mongodb/test_config_unit.py

import pytest

from src.databases.mongodb.config import MongoDBConfig, get_mongodb_config, MongoDBConfigurationException
    

REQUIRED_KEYS = (
    "MONGODB_USER",
    "MONGODB_PASSWORD",
    "MONGODB_CLUSTER",
    "MONGODB_HOST",
    "MONGODB_PORT",
    "MONGODB_DATABASE_NAME",
    "MONGODB_COLLECTION_NAME",
    "MONGODB_MODE",
)


def make_cfg(**overrides) -> dict:
    """
    Base valid config dict. Override anything as needed in tests.
    """
    cfg = {
        "MONGODB_USER": "u",
        "MONGODB_PASSWORD": "p",
        "MONGODB_CLUSTER": "cluster.example.mongodb.net",
        "MONGODB_HOST": "localhost",
        "MONGODB_PORT": 27017,
        "MONGODB_DATABASE_NAME": "db",
        "MONGODB_COLLECTION_NAME": "col",
        "MONGODB_MODE": "atlas",
    }
    cfg.update(overrides)
    return cfg


def test_fixture_mongodb_config_has_valid_mode(mongodb_config: MongoDBConfig):
    assert mongodb_config.MONGODB_MODE in ("localhost", "atlas", "container")


@pytest.mark.parametrize("mode", ["atlas", "localhost", "container"])
def test_get_mongodb_config_reads_mode(mode: str):
    mongodb_config = get_mongodb_config(make_cfg(MONGODB_MODE=mode))
    assert mongodb_config.MONGODB_MODE == mode


@pytest.mark.parametrize("missing_key", REQUIRED_KEYS)
def test_get_mongodb_config_missing_keys_raise(missing_key: str):   
    cfg = make_cfg()
    cfg.pop(missing_key)

    with pytest.raises(MongoDBConfigurationException) as exc:
        get_mongodb_config(cfg)

    # Optional: ensure error message points to the missing key
    assert missing_key in str(exc.value)