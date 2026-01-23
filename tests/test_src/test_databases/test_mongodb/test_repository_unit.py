# tests/test_src/test_databases/test_mongodb/test_repository_unit.py

from unittest.mock import MagicMock

import pytest


def _make_repo_with_mocked_collection(mongodb_repo):
    """
    mongodb_repo fixture returns a disconnected repo object.
    We inject a fake cached collection to unit test CRUD without a real DB.
    """
    repo = mongodb_repo
    repo.collection = MagicMock()
    return repo


def test_insert_one_calls_collection_insert_one(mongodb_repo):
    repo = _make_repo_with_mocked_collection(mongodb_repo)
    doc = {"a": 1}

    ok = repo.insert_one(doc)

    assert ok is True
    repo.collection.insert_one.assert_called_once_with(doc)


def test_insert_one_empty_doc_returns_false(mongodb_repo):
    repo = _make_repo_with_mocked_collection(mongodb_repo)

    ok = repo.insert_one({})

    assert ok is False
    repo.collection.insert_one.assert_not_called()


def test_insert_one_exception_returns_false(mongodb_repo):
    repo = _make_repo_with_mocked_collection(mongodb_repo)
    repo.collection.insert_one.side_effect = Exception("boom")

    ok = repo.insert_one({"a": 1})

    assert ok is False
    repo.collection.insert_one.assert_called_once()


def test_insert_many_calls_collection_insert_many(mongodb_repo):
    repo = _make_repo_with_mocked_collection(mongodb_repo)
    docs = [{"a": 1}, {"a": 2}]

    ok = repo.insert_many(docs, ordered=False)

    assert ok is True
    repo.collection.insert_many.assert_called_once_with(docs, ordered=False)


def test_insert_many_empty_docs_returns_false(mongodb_repo):
    repo = _make_repo_with_mocked_collection(mongodb_repo)

    ok = repo.insert_many([], ordered=False)

    assert ok is False
    repo.collection.insert_many.assert_not_called()


def test_find_by_query_calls_collection_find(mongodb_repo):
    repo = _make_repo_with_mocked_collection(mongodb_repo)
    repo.collection.find.return_value = [{"x": 1}, {"x": 2}]

    docs = repo.find_by_query({"x": 1})

    assert docs == [{"x": 1}, {"x": 2}]
    repo.collection.find.assert_called_once_with({"x": 1})


def test_find_by_query_query_none_returns_empty_list(mongodb_repo):
    repo = _make_repo_with_mocked_collection(mongodb_repo)

    docs = repo.find_by_query(None)

    assert docs == []
    repo.collection.find.assert_not_called()


def test_aggregate_calls_collection_aggregate(mongodb_repo):
    repo = _make_repo_with_mocked_collection(mongodb_repo)
    repo.collection.aggregate.return_value = [{"k": "v"}]
    pipeline = [{"$match": {"a": 1}}]

    docs = repo.aggregate(pipeline)

    assert docs == [{"k": "v"}]
    repo.collection.aggregate.assert_called_once_with(pipeline)


def test_aggregate_empty_pipeline_returns_empty_list(mongodb_repo):
    repo = _make_repo_with_mocked_collection(mongodb_repo)

    docs = repo.aggregate([])

    assert docs == []
    repo.collection.aggregate.assert_not_called()


def test_update_one_calls_collection_update_one(mongodb_repo):
    repo = _make_repo_with_mocked_collection(mongodb_repo)

    # mock return object with modified_count attribute
    repo.collection.update_one.return_value = MagicMock(modified_count=1)

    ok = repo.update_one({"a": 1}, {"b": 2})

    assert ok is True
    repo.collection.update_one.assert_called_once_with({"a": 1}, {"$set": {"b": 2}})


def test_update_many_calls_collection_update_many(mongodb_repo):
    repo = _make_repo_with_mocked_collection(mongodb_repo)
    repo.collection.update_many.return_value = MagicMock(modified_count=2)

    ok = repo.update_many({"a": 1}, {"b": 2})

    assert ok is True
    repo.collection.update_many.assert_called_once_with({"a": 1}, {"$set": {"b": 2}})


def test_delete_by_query_calls_collection_delete_many(mongodb_repo):
    repo = _make_repo_with_mocked_collection(mongodb_repo)
    repo.collection.delete_many.return_value = MagicMock(deleted_count=3)

    ok = repo.delete_by_query({"a": 1})

    assert ok is True
    repo.collection.delete_many.assert_called_once_with({"a": 1})


def test_drop_collection_calls_collection_drop(mongodb_repo):
    repo = _make_repo_with_mocked_collection(mongodb_repo)

    ok = repo.drop_collection()

    assert ok is True
    repo.collection.drop.assert_called_once()


def test_methods_raise_if_collection_not_cached(mongodb_repo):
    """
    This ensures you don't silently connect inside operations (important for benchmarks).
    If repo.collection isn't set, your code should raise to force connect_and_cache().
    """
    repo = mongodb_repo
    repo.collection = None

    with pytest.raises(RuntimeError):
        repo.insert_one({"a": 1})