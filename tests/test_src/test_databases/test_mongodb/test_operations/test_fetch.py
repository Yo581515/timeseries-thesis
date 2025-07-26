# tests/test_src/test_databases/test_mongodb/test_operations/test_fetch.py

from unittest.mock import MagicMock, patch
from src.databases.mongodb.operations.fetch import FetchOperations

class TestMongoDBOperationsFetch:
    @patch.object(FetchOperations, 'find_by_query')
    def test_find_by_query(self, find_by_query_mock: MagicMock):
        # Arrange
        fetch_operations = FetchOperations(mongodb_client=MagicMock())
        query = {"name": "test"}
        find_by_query_mock.return_value = [{"name": "test", "value": 42}]

        # Act
        result = fetch_operations.find_by_query(query)

        # Assert
        assert result == [{"name": "test", "value": 42}]
        find_by_query_mock.assert_called_once_with(query)

    @patch.object(FetchOperations, 'aggregate')
    def test_aggregate(self, aggregate_mock: MagicMock):
        # Arrange
        fetch_operations = FetchOperations(mongodb_client=MagicMock())
        pipeline = [{"$match": {"name": "test"}}]
        aggregate_mock.return_value = [{"name": "test", "value": 42}]

        # Act
        result = fetch_operations.aggregate(pipeline)

        # Assert
        assert result == [{"name": "test", "value": 42}]
        aggregate_mock.assert_called_once_with(pipeline)