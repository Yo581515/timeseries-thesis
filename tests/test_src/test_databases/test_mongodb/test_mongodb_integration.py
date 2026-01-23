# tests/test_src/test_databases/test_mongodb/test_mongodb_integration.py

import logging
import pytest

from src.databases.mongodb.utils.data_utils import resolve_data


@pytest.fixture
def prepared_data(raw_data: list[dict], logger: logging.Logger) -> list[dict]:
    """
    Copy + resolve sample data so each test can insert its own docs.
    resolve_data() modifies docs in-place (time -> datetime, location -> GeoJSON, meta -> dict).
    """
    data = [dict(d) for d in raw_data]
    data = [doc for doc in data if resolve_data(doc, logger)]
    return data


@pytest.fixture(autouse=True)
def clean_collection(mongodb_repo_connected):
    """
    Ensure collection is empty before and after each test.
    mongodb_repo_connected is your integration fixture that connects/disconnects.
    """
    mongodb_repo_connected.delete_by_query({})
    yield
    mongodb_repo_connected.delete_by_query({})


def test_ping(mongodb_repo_connected):
    assert mongodb_repo_connected.ping() is True


def test_insert_one_and_find_by_id(mongodb_repo_connected, prepared_data):
    doc = prepared_data[0]

    ok = mongodb_repo_connected.insert_one(doc)
    assert ok is True

    # PyMongo typically injects _id into the dict if missing
    assert "_id" in doc

    found = mongodb_repo_connected.find_by_query({"_id": doc["_id"]})
    assert isinstance(found, list)
    assert len(found) == 1
    assert found[0]["_id"] == doc["_id"]


def test_insert_many_and_find_by_in(mongodb_repo_connected, prepared_data):
    batch = prepared_data[0:3]

    ok = mongodb_repo_connected.insert_many(batch, ordered=False)
    assert ok is True

    ids = [d["_id"] for d in batch]
    found = mongodb_repo_connected.find_by_query({"_id": {"$in": ids}})
    assert isinstance(found, list)
    assert len(found) == len(batch)


def test_delete_by_query_clears_collection(mongodb_repo_connected, prepared_data):
    assert mongodb_repo_connected.insert_many(prepared_data[:2], ordered=False) is True
    assert mongodb_repo_connected.delete_by_query({}) is True

    found = mongodb_repo_connected.find_by_query({})
    assert isinstance(found, list)
    assert len(found) == 0


def test_find_by_query_meta_source(mongodb_repo_connected, prepared_data):
    assert mongodb_repo_connected.insert_many(prepared_data[:5], ordered=False) is True

    found = mongodb_repo_connected.find_by_query({"meta.source": "Node 1"})
    assert isinstance(found, list)
    assert len(found) >= 1


def test_aggregate_returns_time_and_id(mongodb_repo_connected, prepared_data):
    assert mongodb_repo_connected.insert_many(prepared_data[:6], ordered=False) is True

    pipeline = [
        {"$match": {"meta.source": "Node 1"}},
        {"$project": {"time": 1, "_id": 1}},
    ]
    result = mongodb_repo_connected.aggregate(pipeline)

    assert isinstance(result, list)
    assert all("_id" in d for d in result)
    assert all("time" in d for d in result)


def test_update_many_by_meta_device_id(mongodb_repo_connected, prepared_data):
    """
    Update using a meta-based filter to avoid updating measurement fields.
    Your meta has: source, device_id, sensor_id.
    """
    doc = prepared_data[0]
    assert mongodb_repo_connected.insert_one(doc) is True

    # Use meta.device_id (you set it from top-level source_id)
    device_id = doc["meta"]["device_id"]
    assert device_id is not None

    flt = {"meta.device_id": device_id}
    upd = {"meta.source": "Node X"}

    assert mongodb_repo_connected.update_many(flt, upd) is True

    after = mongodb_repo_connected.find_by_query({"meta.device_id": device_id})
    assert isinstance(after, list)
    assert len(after) >= 1
    assert all(d["meta"]["source"] == "Node X" for d in after)