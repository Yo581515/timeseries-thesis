# src/databases/mongodb/utils/data_utils.py

import logging
from datetime import datetime, timezone


def convert_time(data_point: dict, logger: logging.Logger) -> bool:
    """Convert 'time' field to datetime."""
    try:
        if "time" in data_point and data_point["time"]:
            data_point["time"] = datetime.fromisoformat(data_point["time"]).replace(tzinfo=timezone.utc)
        else:
            logger.error("[convert_time] Missing 'time'")
            return False
        return True
    except Exception as e:
        logger.error(f"[convert_time] Time conversion error: {e}")
        return False


def convert_location(data_point: dict, logger: logging.Logger) -> bool:
    """Convert location to GeoJSON Point {type:'Point', coordinates:[lon,lat]}."""
    try:
        loc = data_point.get("location") or {}
        if not isinstance(loc, dict):
            logger.error("[convert_location] 'location' is not a dict")
            return False

        # already GeoJSON
        if loc.get("type") == "Point" and isinstance(loc.get("coordinates"), list) and len(loc["coordinates"]) == 2:
            return True

        lat = loc.get("latitude", loc.get("lat"))
        lon = loc.get("longitude", loc.get("lon", loc.get("lng")))

        if lat is None or lon is None:
            logger.error("[convert_location] Missing latitude/longitude")
            return False

        data_point["location"] = {"type": "Point", "coordinates": [float(lon), float(lat)]}
        return True

    except Exception as e:
        logger.error(f"[convert_location] Location conversion error: {e}")
        return False


def extract_meta_fields(data_point: dict, logger: logging.Logger) -> bool:
    """
    Build meta from your dataset structure:
      - meta.source    <- top-level 'source'
      - meta.device_id <- top-level 'source_id'
      - meta.sensor_id <- first observation 'parameter' (fallback to observation 'source_id')
    """
    try:
        source = data_point.get("source")
        source_id = data_point.get("source_id")

        sensor_id = None
        observations = data_point.get("observations")
        if isinstance(observations, list) and observations:
            obs0 = observations[0] if isinstance(observations[0], dict) else None
            if obs0:
                sensor_id = obs0.get("parameter") or obs0.get("source_id")

        data_point["meta"] = {
            "source": source,
            "device_id": source_id,
            "sensor_id": sensor_id,
        }
        return True

    except Exception as e:
        logger.error(f"[extract_meta_fields] Meta extraction error: {e}")
        return False


def resolve_data(data_point: dict, logger: logging.Logger) -> bool:
    """
    Normalize a doc in-place.
    Returns True if valid, False if it should be dropped.
    """
    if not convert_time(data_point, logger):
        return False
    if not convert_location(data_point, logger):
        return False
    if not extract_meta_fields(data_point, logger):
        return False
    return True




def make_strftime_from_utc(d: dict) -> dict:
    if "time" not in d or d["time"] is None:
        breakpoint()

    dt = d["time"]

    # Enforce UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    d["time"] = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return d




