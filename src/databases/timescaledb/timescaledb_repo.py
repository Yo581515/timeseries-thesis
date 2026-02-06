# src/databases/timescaledb/timescaledb_repo.py

import datetime
import time
import logging

from src.common.config import load_config
from src.common.logger import get_logger
from src.databases.timescaledb.client import TimeScaleDBClient
from src.databases.timescaledb.config import get_timescaledb_config
from src.databases.timescaledb.config import TimeScaleDBConfig
from src.databases.timescaledb.models.measurement import Measurement

class TimescaleDBRepo(TimeScaleDBClient):
    def __init__(self, timescaledb_config: TimeScaleDBConfig, logger: logging.Logger): 
        super().__init__(timescaledb_config, logger)
        
    
    def ping(self):
        with self.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            if result and result[0] == 1:
                self.logger.info("TimescaleDB ping successful")
                return True
            else:
                self.logger.error("TimescaleDB ping failed")
                return False
    
    def insert_one_measurement(self, measurement: Measurement):
        with self.cursor() as cur:
            insert_query = """
                INSERT INTO measurements (
                    time, node_source, node_source_id, latitude, longitude,
                    sensor_source, sensor_source_id, parameter, value, unit, quality_codes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            start_time = time.perf_counter()
            cur.execute(insert_query, measurement.to_tuple())
            end_time = time.perf_counter()
            self.logger.info(f"Inserted one measurement into TimescaleDB in {end_time - start_time:.6f} seconds")
            
    def insert_many_measurements(self, measurements: list[Measurement]):
        with self.cursor() as cur:
            insert_query = """
                INSERT INTO measurements (
                    time, node_source, node_source_id, latitude, longitude,
                    sensor_source, sensor_source_id, parameter, value, unit, quality_codes
                ) VALUES %s
            """
            measurement_tuples = [m.to_tuple() for m in measurements]
            start_time = time.perf_counter()
            cur.executemany(insert_query, measurement_tuples)
            end_time = time.perf_counter()
            self.logger.info(f"Inserted {len(measurements)} measurements into TimescaleDB in {end_time - start_time:.6f} seconds")
            
    def query_measurements(self, parameter: str, start_time: datetime.datetime, end_time: datetime.datetime) -> list[Measurement]:
        with self.cursor() as cur:
            query = """
                SELECT time, node_source, node_source_id, latitude, longitude,
                       sensor_source, sensor_source_id, parameter, value, unit, quality_codes
                FROM measurements
                WHERE parameter = %s AND time >= %s AND time <= %s
                ORDER BY time DESC
            """
            start_query_time = time.perf_counter()
            cur.execute(query, (parameter, start_time, end_time))
            rows = cur.fetchall()
            end_query_time = time.perf_counter()
            self.logger.info(f"Queried measurements from TimescaleDB in {end_query_time - start_query_time:.6f} seconds")
            
            return [Measurement(*row) for row in rows]
if __name__ == "__main__":
    timescale_config_path = "./configs/config-timescaledb.yml" 
    timescale_config = load_config(timescale_config_path) #dict
    logger = get_logger("timescaledb_repo.py", timescale_config["general"]["log_file"])
    timescale_db_config = get_timescaledb_config(timescale_config["database"])
    timescaledb = TimescaleDBRepo(timescale_db_config, logger)
    
    
    def utc_now() -> datetime:
        """UTC timestamp for created_at."""
        return datetime.datetime.now(datetime.timezone.utc)



    timescaledb.ping() 
    
    try:
        measurement = Measurement(
            time=utc_now(),
            node_source="test_node_source",
            node_source_id="test_node_source_id",
            latitude=0.0,
            longitude=0.0,
            sensor_source="test_sensor_source",
            sensor_source_id="test_sensor_source_id",
            parameter="temperature",
            value=25.5,
            unit="C",
            quality_codes=[1, 2, 3]
        )
        timescaledb.insert_one_measurement(measurement)
    except Exception as e:
        logger.error(f"Error inserting measurement: {e}")   
        
        
    
    # query measurements for the last hour
    try:
        end_time = utc_now()
        start_time = end_time - datetime.timedelta(hours=1)
        results = timescaledb.query_measurements("temperature", start_time, end_time)
        logger.info(f"Queried {len(results)} measurements for parameter 'temperature' in the last hour")
        print()
        print(results[0])  # print the first result for verification
    except Exception as e:
        logger.error(f"Error querying measurements: {e}")