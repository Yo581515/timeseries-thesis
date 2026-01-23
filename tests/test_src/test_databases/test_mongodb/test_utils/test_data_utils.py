# tests/test_src/test_databases/test_mongodb/test_utils/test_data_utils.py

from datetime import datetime

import pytest

from src.databases.mongodb.utils.data_utils import (
    convert_location,
    convert_time,
    extract_meta_fields,
    resolve_data,
)


def test_convert_time_success(logger):
    doc = {"time": "2024-09-18T20:06:45.179000"}
    ok = convert_time(doc, logger)

    assert ok is True
    assert isinstance(doc["time"], datetime)


def test_convert_time_missing_time(logger):
    doc = {}
    ok = convert_time(doc, logger)

    assert ok is False


def test_convert_location_success(logger):
    doc = {"location": {"latitude": 60.090717, "longitude": 5.263733}}
    ok = convert_location(doc, logger)

    assert ok is True
    assert doc["location"]["type"] == "Point"
    assert doc["location"]["coordinates"] == [5.263733, 60.090717]


def test_convert_location_already_geojson(logger):
    doc = {"location": {"type": "Point", "coordinates": [5.0, 60.0]}}
    ok = convert_location(doc, logger)

    assert ok is True
    assert doc["location"]["coordinates"] == [5.0, 60.0]


def test_convert_location_missing_location(logger):
    doc = {}
    ok = convert_location(doc, logger)

    assert ok is False


def test_extract_meta_fields_from_top_level_and_observation(logger):
    doc = {
        "source": "Node 1",
        "source_id": "sfi_smart_ocean;demo;d1;1",
        "observations": [{"parameter": "battery_voltage_mv", "value": 3624.0}],
    }

    ok = extract_meta_fields(doc, logger)

    assert ok is True
    assert doc["meta"]["source"] == "Node 1"
    assert doc["meta"]["device_id"] == "sfi_smart_ocean;demo;d1;1"
    assert doc["meta"]["sensor_id"] == "battery_voltage_mv"


def test_extract_meta_fields_handles_missing_observations(logger):
    doc = {"source": "Node 1", "source_id": "id-1"}
    ok = extract_meta_fields(doc, logger)

    assert ok is True
    assert doc["meta"]["source"] == "Node 1"
    assert doc["meta"]["device_id"] == "id-1"
    assert doc["meta"]["sensor_id"] is None


def test_resolve_data_success(logger):
    doc = {
        "source": "Node 1",
        "source_id": "sfi_smart_ocean;demo;d1;1",
        "time": "2024-09-18T20:06:45.179000",
        "location": {"latitude": 60.090717, "longitude": 5.263733},
        "observations": [{"parameter": "battery_voltage_mv", "value": 3624.0}],
    }

    ok = resolve_data(doc, logger)

    assert ok is True
    assert isinstance(doc["time"], datetime)
    assert doc["location"]["type"] == "Point"
    assert "meta" in doc


@pytest.mark.parametrize("bad_doc", [
    # missing time
    {
        "source": "Node 1",
        "source_id": "id",
        "location": {"latitude": 1.0, "longitude": 2.0},
        "observations": [{"parameter": "p", "value": 1.0}],
    },
    # missing location
    {
        "source": "Node 1",
        "source_id": "id",
        "time": "2024-09-18T20:06:45.179000",
        "observations": [{"parameter": "p", "value": 1.0}],
    },
])
def test_resolve_data_failure_cases(logger, bad_doc):
    ok = resolve_data(bad_doc, logger)
    assert ok is False
