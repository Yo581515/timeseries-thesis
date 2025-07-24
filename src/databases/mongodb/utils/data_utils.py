# src/databases/mongodb/utils/data_utils.py

import logging
import random
from datetime import datetime

def rand_float(a, b, precision):
    return round(random.uniform(a, b), precision)

def resolve_data(data_point: dict, logger: logging.Logger) -> dict:
    """Apply resolution steps to a data point. Always returns a dict."""
    convert_time(data_point, logger)
    convert_location(data_point, logger)
    extract_meta_fields(data_point, logger)
    return data_point


def convert_time(data_point: dict, logger: logging.Logger) -> bool:
    """Convert 'time' field to datetime."""
    try:  
        if 'time' in data_point:
            data_point['time'] = datetime.fromisoformat(data_point['time'])
        return True
    except Exception as e:
        logger.error(f"[convert_time] Time conversion error: {e}")
        return False


def convert_location(data_point: dict, logger: logging.Logger) -> bool:
    """Convert 'location' to GeoJSON Point."""
    try:
        loc = data_point.get('location')
        if loc and 'latitude' in loc and 'longitude' in loc:
            data_point['location'] = {
                "type": "Point",
                "coordinates": [loc['longitude'], loc['latitude']]
            }
        return True
    except Exception as e:
        logger.error(f"[convert_location] Location formatting error: {e}")
        return False


def extract_meta_fields(data_point: dict, logger: logging.Logger) -> bool:
    """Move static metadata fields to 'meta'."""
    try:
        meta_fields = ['source', 'source_id', 'location']
        data_point['meta'] = {k: data_point.pop(k) for k in meta_fields if k in data_point}
        return True
    except Exception as e:
        logger.error(f"[extract_meta_fields] Meta construction error: {e}")
        return False