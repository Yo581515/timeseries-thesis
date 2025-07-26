# tests/test_src/test_databases/test_mongodb/test_repository.py
from unittest.mock import MagicMock, patch

from src.databases.mongodb.client import MongoDBClient
from src.databases.mongodb.mongodb_repository import MongoDBRepository

class TestMongoDBRepository:
    
    @patch.object(MongoDBRepository, 'ping')
    def test_ping(self, ping_mock : MagicMock):
        # Arrange
        ping_mock.return_value = True
        repository = MongoDBRepository(mongodb_client=MagicMock())

        # Act
        result = repository.ping()

        # Assert
        assert result is True
        ping_mock.assert_called_once()
    
    @patch.object(MongoDBRepository, 'insert_one')
    def test_insert_one(self, insert_one_mock : MagicMock):
        # Arrange
        insert_one_mock.return_value = True
        repository = MongoDBRepository(mongodb_client=MagicMock())
        data_point = {"key": "value"}

        # Act
        result = repository.insert_one(data_point)

        # Assert
        assert result is True
        insert_one_mock.assert_called_once_with(data_point)
        
    @patch.object(MongoDBRepository, 'insert_many')
    def test_insert_many(self, insert_many_mock : MagicMock):
        # Arrange
        insert_many_mock.return_value = True
        repository = MongoDBRepository(mongodb_client=MagicMock())
        data_points = [{"key1": "value1"}, {"key2": "value2"}]

        # Act
        result = repository.insert_many(data_points)

        # Assert
        assert result is True
        insert_many_mock.assert_called_once_with(data_points)
        
        
    @patch.object(MongoDBRepository, 'delete_by_query')
    def test_delete_by_query(self, delete_by_query_mock : MagicMock):
        # Arrange
        delete_by_query_mock.return_value = True
        repository = MongoDBRepository(mongodb_client=MagicMock())
        query = {"key": "value"}

        # Act
        result = repository.delete_by_query(query)

        # Assert
        assert result is True
        delete_by_query_mock.assert_called_once_with(query)
        
    @patch.object(MongoDBRepository, 'find_by_query')
    def test_find_by_query(self, find_by_query_mock : MagicMock):
        # Arrange
        find_by_query_mock.return_value = [{"key": "value"}]
        repository = MongoDBRepository(mongodb_client=MagicMock())
        query = {"key": "value"}

        # Act
        result = repository.find_by_query(query)

        # Assert
        assert result == [{"key": "value"}]
        find_by_query_mock.assert_called_once_with(query)
        
    @patch.object(MongoDBRepository, 'aggregate')
    def test_aggregate(self, aggregate_mock : MagicMock):
        # Arrange
        aggregate_mock.return_value = [{"key": "value"}]
        repository = MongoDBRepository(mongodb_client=MagicMock())
        pipeline = [{"$match": {"key": "value"}}]

        # Act
        result = repository.aggregate(pipeline)

        # Assert
        assert result == [{"key": "value"}]
        aggregate_mock.assert_called_once_with(pipeline)