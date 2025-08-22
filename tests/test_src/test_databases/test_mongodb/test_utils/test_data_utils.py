# tests/test_src/test_databases/test_mongodb/test_utils/test_data_utils.py

import datetime
import logging
from unittest.mock import MagicMock

from src.databases.mongodb.utils.data_utils import convert_time, convert_location, extract_meta_fields, resolve_data

class TestMDDataUtils:

    def test_convert_time(self):
        logger = MagicMock()
        data_point = {"time": "2024-09-18T20:06:45.179+00:00"}
        assert convert_time(data_point, logger) is True
        assert isinstance(data_point['time'], datetime.datetime)
        assert data_point['time'].tzinfo is not None  # Check if timezone is set
        

    def test_convert_location(self):
        logger = MagicMock()
        data_point = {"location": {"latitude": 40.7128, "longitude": -74.0060}}
        assert convert_location(data_point, logger) is True
        assert data_point['location']['type'] == "Point"
        assert data_point['location']['coordinates'] == [-74.0060, 40.7128]

    def test_extract_meta_fields(self):
        logger = MagicMock()
        data_point = {
            "source": "Node 1",
            "source_id": "12345",
            "location": {
                "coordinates": [-74.0060, 40.7128],
                "type": "Point"
            }
        }
        assert extract_meta_fields(data_point, logger) is True
        assert 'meta' in data_point
        assert data_point['meta']['source'] == "Node 1"
        assert data_point['meta']['source_id'] == "12345"
        assert data_point['meta']['location']['type'] == "Point"
        assert data_point['meta']['location']['coordinates'] == [-74.0060, 40.7128]

    def test_resolve_data(self):
        logger = MagicMock()
        data_point = {
            "time": "2024-09-18T20:06:45.179+00:00",
            "location": {"latitude": 40.7128, "longitude": -74.0060},
            "source": "Node 1",
            "source_id": "12345"
        }
        resolved_data = resolve_data(data_point, logger)
        assert resolved_data is True
        assert isinstance(data_point['time'], datetime.datetime)
        assert data_point['meta']['location']['type'] == "Point"
        assert data_point['meta']['location']['coordinates'] == [-74.0060, 40.7128]
        assert 'meta' in data_point
        assert data_point['meta']['source'] == "Node 1"
        assert data_point['meta']['source_id'] == "12345"

    def test_convert_time_failure(self):
        logger = MagicMock(spec=logging.Logger)
        data_point = {"time": "invalid-time-format"}
        assert convert_time(data_point, logger) is False
        logger.error.assert_called_once()

    def test_convert_location_failure(self):
        logger = MagicMock(spec=logging.Logger)
        data_point = {"location": {"lat": 40.7128, "lng": -74.0060}}  # wrong keys
        assert convert_location(data_point, logger) is False
        logger.error.assert_called_once()

    def test_extract_meta_fields_missing_keys(self):
        logger = MagicMock()
        data_point = {
            "extra": "value"
        }
        assert extract_meta_fields(data_point, logger) is True
        assert "meta" in data_point
        assert data_point["meta"] == {}  # No keys matched