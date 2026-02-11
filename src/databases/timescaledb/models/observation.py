from dataclasses import dataclass
from datetime import datetime
from typing import List


class Observation:
    
    def __init__(
        self,
        time: datetime,
        node_source: str,
        node_source_id: str,
        latitude: float,
        longitude: float,
        sensor_source: str,
        sensor_source_id: str,
        parameter: str,
        value: float,
        unit: str,
        quality_codes: List[int],
    ):
        self.time = time
        self.node_source = node_source
        self.node_source_id = node_source_id
        self.latitude = latitude
        self.longitude = longitude
        self.sensor_source = sensor_source
        self.sensor_source_id = sensor_source_id
        self.parameter = parameter
        self.value = value
        self.unit = unit
        self.quality_codes = quality_codes

    def to_tuple(self) -> tuple:
        return (
            self.time,
            self.node_source,
            self.node_source_id,
            self.latitude,
            self.longitude,
            self.sensor_source,
            self.sensor_source_id,
            self.parameter,
            self.value,
            self.unit,
            self.quality_codes,
        )
        
        
    def __str__(self):
        return (
            f"Observation(time={self.time}\n"
            f"node_source={self.node_source}\n"
            f"node_source_id={self.node_source_id}\n"
            f"latitude={self.latitude}\n"
            f"longitude={self.longitude}\n"
            f"sensor_source={self.sensor_source}\n"
            f"sensor_source_id={self.sensor_source_id}\n"
            f"parameter={self.parameter}\n"
            f"value={self.value}\n"
            f"unit={self.unit}\n"
            f"quality_codes={self.quality_codes})"
        )