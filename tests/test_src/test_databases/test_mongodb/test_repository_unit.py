# tests/test_src/test_databases/test_mongodb/test_repository_unit.py

from unittest.mock import MagicMock
import datetime

from src.databases.mongodb.mongodb_repository import MongoDBRepository


# -------------------------
# insert_one
# -------------------------

def test_insert_one_calls_collection_insert_one():
    # Arrange
    repo = MagicMock(spec=MongoDBRepository)
    repo.collection = MagicMock()
    repo.get_collection = MagicMock(return_value=repo.collection)
    repo.logger = MagicMock()

    doc = {
        "time": datetime.datetime.now(datetime.timezone.utc)
    }

    # Act
    ok = MongoDBRepository.insert_one(repo, doc)

    # Assert
    assert ok is True
    repo.collection.insert_one.assert_called_once_with(doc)
    repo.logger.error.assert_not_called()
    repo.logger.exception.assert_not_called()


def test_insert_one_empty_doc_returns_false_and_logs_error():
    # Arrange
    repo = MagicMock(spec=MongoDBRepository)
    repo.collection = MagicMock()
    repo.logger = MagicMock()

    # Act
    ok = MongoDBRepository.insert_one(repo, {})

    # Assert
    assert ok is False
    repo.collection.insert_one.assert_not_called()
    repo.logger.error.assert_called_once()
    repo.logger.exception.assert_not_called()


# def test_insert_one_exception_returns_false_and_logs_exception():
#     # Arrange
#     repo = MagicMock(spec=MongoDBRepository)
#     repo.collection = MagicMock()
#     repo.get_collection = MagicMock(return_value=repo.collection)
#     repo.logger = MagicMock()
#     repo.collection.insert_one.side_effect = Exception("boom")

#     doc = {"a": 1}

#     # Act
#     ok = MongoDBRepository.insert_one(repo, doc)

#     # Assert
#     assert ok is False
#     repo.collection.insert_one.assert_called_once_with(doc)
#     repo.logger.exception.assert_called_once()