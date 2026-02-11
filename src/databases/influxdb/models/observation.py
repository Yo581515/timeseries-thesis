from dataclasses import dataclass
from datetime import datetime
from typing import List


class Observation:
    def __init__(
        self,
        time: datetime,

        # node / source metadata (tags)
        node_source: str,
        node_source_id: str,
        
        # sensor / source metadata (tags)
        sensor_source: str,    
        sensor_source_id: str,

        # location (fields)
        latitude: float,
        longitude: float,

        # measurements (fields)
        temperature: float,
        humidity: float,
        salinity: float,

        # units (fields)
        temperature_unit: str,
        humidity_unit: str,
        salinity_unit: str,

        # quality flags (fields)
        quality_codes: List[int]
        
    ):
        self.time = time

        self.node_source = node_source
        self.node_source_id = node_source_id
        
        self.sensor_source = sensor_source
        self.sensor_source_id = sensor_source_id

        self.latitude = latitude
        self.longitude = longitude

        self.temperature = temperature
        self.humidity = humidity
        self.salinity = salinity

        self.temperature_unit = temperature_unit
        self.humidity_unit = humidity_unit
        self.salinity_unit = salinity_unit
        
        self.quality_codes = quality_codes
        
    def __str__(self):
        return (
            f"(Observation(time={self.time},\n"
            f"  node_source={self.node_source},\n"
            f"  node_source_id={self.node_source_id},\n"
            f"  sensor_source={self.sensor_source},\n"
            f"  sensor_source_id={self.sensor_source_id},\n"
            f"  latitude={self.latitude}, longitude={self.longitude},\n"
            f"  temperature={self.temperature} {self.temperature_unit},\n"
            f"  humidity={self.humidity} {self.humidity_unit},\n"
            f"  salinity={self.salinity} {self.salinity_unit},\n"
            f"  quality_codes={self.quality_codes}))"
            )
        