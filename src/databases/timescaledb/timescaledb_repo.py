# src/databases/timescaledb/timescaledb_repo.py

import time
import logging
from datetime import datetime, timedelta, timezone


from src.databases.timescaledb.client import TimeScaleDBClient
from src.databases.timescaledb.config import TimeScaleDBConfig
from src.databases.timescaledb.models.observation import Observation

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
    
    def insert_one_observation(self, observation: Observation):
        print("Inserting one observation into TimescaleDB...")
        with self.cursor() as cur:
            insert_query = """
                INSERT INTO observations (
                    time, node_source, node_source_id, latitude, longitude,
                    sensor_source, sensor_source_id, parameter, value, unit, quality_codes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            start_time = time.perf_counter()
            cur.execute(insert_query, observation.to_tuple())
            end_time = time.perf_counter()
            self.logger.info(f"Inserted one observation into TimescaleDB in {end_time - start_time:.6f} seconds")
            
    def insert_many_observations(self, observations: list[Observation]):
        print("about to insert many observations into TimescaleDB...")
        with self.cursor() as cur:
            insert_query = """
                INSERT INTO observations (
                    time, node_source, node_source_id, latitude, longitude,
                    sensor_source, sensor_source_id, parameter, value, unit, quality_codes
                ) VALUES %s
            """
            observation_tuples = [o.to_tuple() for o in observations]
            start_time = time.perf_counter()
            cur.executemany(insert_query, observation_tuples)
            end_time = time.perf_counter()
            self.logger.info(f"Inserted {len(observations)} observations into TimescaleDB in {end_time - start_time:.6f} seconds")
            
    def query_observations(self, parameter: str, start_time: datetime, end_time: datetime) -> list[Observation]:
        with self.cursor() as cur:
            query = """
                SELECT time, node_source, node_source_id, latitude, longitude,
                       sensor_source, sensor_source_id, parameter, value, unit, quality_codes
                FROM observations
                WHERE parameter = %s AND time >= %s AND time <= %s
                ORDER BY time DESC
            """
            start_query_time = time.perf_counter()
            cur.execute(query, (parameter, start_time, end_time))
            rows = cur.fetchall()
            end_query_time = time.perf_counter()
            self.logger.info(f"Queried observations from TimescaleDB in {end_query_time - start_query_time:.6f} seconds")
            
            return [Observation(*row) for row in rows]
if __name__ == "__main__":
    from src.common.config import load_config
    from src.common.logger import get_logger
    from src.databases.timescaledb.config import get_timescaledb_config
    
    from pprint import pprint
    print("Testing TimescaleDBRepo...")
    timescale_config_path = "./configs/config-timescaledb.yml" 
    timescale_config = load_config(timescale_config_path) #dict
    logger = get_logger("timescaledb_repo.py", timescale_config["general"]["log_file"])
    timescale_db_config = get_timescaledb_config(timescale_config["database"])
    timescaledb = TimescaleDBRepo(timescale_db_config, logger)
    
    # pprint(timescale_config)
    
    
    def utc_now() -> datetime:
        """UTC timestamp for created_at."""
        return datetime.now(timezone.utc)



    print(timescaledb.ping() and "TimescaleDB connection successful" or "TimescaleDB connection failed")
    
    try:
        observation  = Observation(
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
        print("Created test observation:")
        timescaledb.insert_one_observation(observation)
    except Exception as e:
        logger.error(f"Error inserting observation: {e}")   
        
        
    
    # query observations for the last hour
    try:
        end_time = utc_now()
        start_time = end_time - timedelta(hours=1)
        results = timescaledb.query_observations("temperature", start_time, end_time)
        logger.info(f"Queried {len(results)} observations for parameter 'temperature' in the last hour")            
        print()
        print(results[0])  # print the first result for verification
    except Exception as e:
        logger.error(f"Error querying observations: {e}")
    
    print("Done testing TimescaleDBRepo")