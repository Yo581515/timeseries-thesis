import logging
import random
from datetime import datetime, UTC

def rand_float(a, b, precision):
    return round(random.uniform(a, b), precision)

def resolve_data(one_data_point : dict, logger : logging.Logger) -> dict:
    try:
        if 'time' in one_data_point:
            time = one_data_point['time']
            date_time = datetime.fromisoformat(time)
            one_data_point['time'] = date_time
    except Exception as e:
        logger.error("Convert time error: " + str(e))
        return None
    
    try:
        if 'location' in one_data_point:
            loc = one_data_point['location']
            if 'latitude' in loc and 'longitude' in loc:
                one_data_point['location'] = {
                    "type": "Point",
                    "coordinates": [loc['longitude'], loc['latitude']]
                }
    except Exception as e:
        logger.error("Convert location error: " + str(e))
        return None
    
    
    try:
        meta = {}
        for key in ['source', 'source_id', 'location']:
            if key in one_data_point:
                meta[key] = one_data_point[key]
                del one_data_point[key]
        one_data_point['meta'] = meta
    except Exception as e:
        logger.error("Meta field construction error: " + str(e))
        return None
        
    return one_data_point

