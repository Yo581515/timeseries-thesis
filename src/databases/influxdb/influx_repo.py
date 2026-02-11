# src/databases/influxdb/influx_repo.py


from datetime import datetime, time, timezone, timedelta
import logging
import time

from influxdb_client import Point, WritePrecision


from src.databases.influxdb.client import InfluxDB_Client
from src.databases.influxdb.config import InfluxDBConfig
from src.databases.influxdb.models.observation import Observation


class InfluxRepo(InfluxDB_Client):
    def __init__(self, influxdb_config: InfluxDBConfig, logger: logging.Logger): 
        super().__init__(influxdb_config, logger)
        
    
    def ping(self):
        try:
            self.logger.info("Pinging InfluxDB...")
            ping = self.write_client.ping()
            
            if ping:
                self.logger.info(f"Successfully connected to InfluxDB, pinged : {ping}")
            else:
                self.logger.error(f"Failed to connect to InfluxDB, pinged: {ping}")
        except Exception as e:
            self.logger.error(f"Error connecting to InfluxDB: {e}")
            
            
    
    def insert_one_observation(self, obs: Observation):
        try:
            self.logger.info("Inserting one observation into InfluxDB...")
            with self.write_api() as write_api:
                point = (
                    Point("observations")
                .tag("node_source", obs.node_source)
                .tag("node_source_id", obs.node_source_id)
                .tag("sensor_source", obs.sensor_source)
                .tag("sensor_source_id", obs.sensor_source_id)

                .field("temperature", float(obs.temperature))
                .field("humidity", float(obs.humidity))
                .field("salinity", float(obs.salinity))
                
                .tag("temperature_unit", obs.temperature_unit)
                .tag("humidity_unit", obs.humidity_unit)
                .tag("salinity_unit", obs.salinity_unit)
                
                .field("latitude", float(obs.latitude))
                .field("longitude", float(obs.longitude))
                .field(
                    "quality_codes",
                    "[" + ",".join(str(q) for q in obs.quality_codes) + "]"
                )
                .time(obs.time, WritePrecision.NS)
                )
                write_api.write(bucket=self.bucket, record=point)
            
        except Exception as e:
            self.logger.error(f"Error inserting one observation into InfluxDB: {e}")
            
            
    def insert_observations(self, obs_list: list[Observation]):
        try:
            self.logger.info(f"Inserting {len(obs_list)} observations into InfluxDB...")
            with self.write_api() as write_api:
                points = []
                for obs in obs_list:
                    point = (
                        Point("observations")
                    .tag("node_source", obs.node_source)
                    .tag("node_source_id", obs.node_source_id)
                    .tag("sensor_source", obs.sensor_source)
                    .tag("sensor_source_id", obs.sensor_source_id)

                    .field("temperature", float(obs.temperature))
                    .field("humidity", float(obs.humidity))
                    .field("salinity", float(obs.salinity))
                    
                    .tag("temperature_unit", obs.temperature_unit)
                    .tag("humidity_unit", obs.humidity_unit)
                    .tag("salinity_unit", obs.salinity_unit)
                    
                    .field("latitude", float(obs.latitude))
                    .field("longitude", float(obs.longitude))
                    .field(
                        "quality_codes",
                        "[" + ",".join(str(q) for q in obs.quality_codes) + "]"
                    )
                    .time(obs.time, WritePrecision.NS)
                    )
                    points.append(point)
                measure_per_time = time.perf_counter()
                write_api.write(bucket=self.bucket, record=points)
                measure_per_time = time.perf_counter() - measure_per_time
                self.logger.info(f"Inserted {len(obs_list)} observations into InfluxDB in {measure_per_time:.2f} seconds")
            
        except Exception as e:
            self.logger.error(f"Error inserting observations into InfluxDB: {e}")
            
     


    def query_observations_range_date(self, start_utc : datetime, end_utc:datetime)-> list[Observation]:
        print(f"Querying observations from {start_utc} to {end_utc}...")
        
        try:
            start = start_utc.astimezone(timezone.utc).isoformat()
            stop = end_utc.astimezone(timezone.utc).isoformat()
            print(f"Converted start: {start}, stop: {stop}")

            query = f"""
                        from(bucket: "{self.bucket}")
                        |> range(start: time(v: "{start}"), stop: time(v: "{stop}"))
                        |> filter(fn: (r) => r._measurement == "observations")
                        |> pivot(
                            rowKey: ["_time", "node_source", "node_source_id", "sensor_source", "sensor_source_id"],
                            columnKey: ["_field"],
                            valueColumn: "_value"
                        )
                    """
            print(query)
            self.logger.info("Query: %s", query)
            
            measure_per_time = time.perf_counter()
            tables = self.query_api().query(query, org=self.org)
            measure_per_time = time.perf_counter() - measure_per_time
            self.logger.info("Query execution time: %f seconds", measure_per_time)

            observations = []
            for table in tables:
                for record in table.records:
                    v = record.values
                    qc_raw = v.get("quality_codes", "[]")
                    qcs = [int(q) for q in qc_raw.strip("[]").split(",") if q]

                    observations.append(
                        Observation(
                            time=v.get("_time"),
                            node_source=v.get("node_source"),
                            node_source_id=v.get("node_source_id"),
                            sensor_source=v.get("sensor_source"),
                            sensor_source_id=v.get("sensor_source_id"),
                            latitude=float(v.get("latitude", 0)),
                            longitude=float(v.get("longitude", 0)),
                            temperature=float(v.get("temperature", 0)),
                            humidity=float(v.get("humidity", 0)),
                            salinity=float(v.get("salinity", 0)),
                            temperature_unit=v.get("temperature_unit", ""),
                            humidity_unit=v.get("humidity_unit", ""),
                            salinity_unit=v.get("salinity_unit", ""),
                            quality_codes=qcs,
                        )
                    )

            return observations

        except Exception as e:
            self.logger.error(f"Error querying observations from InfluxDB: {e}")
            return []



    def clear_observations(self):
        try:
            self.logger.info("Clearing all observations from InfluxDB...")
            delete_api = self.write_client.delete_api()
            start = "2025-01-01T00:00:00Z"
            stop = datetime.now(timezone.utc).isoformat()
            delete_api.delete(start, stop, '_measurement="observations"', bucket=self.bucket, org=self.org)
            self.logger.info("All observations cleared from InfluxDB.")
        except Exception as e:
            self.logger.error(f"Error clearing observations from InfluxDB: {e}")
        

if __name__ == "__main__":
    import random
    
    from src.common.config import load_config
    from src.common.logger import get_logger
    from src.databases.influxdb.config import get_influxdb_config
    
    

    influxdb_config_path = "./configs/config-influxdb.yml" 
    influxdb_config_dict = load_config(influxdb_config_path) #dict
    logger = get_logger("influx_repo.py", influxdb_config_dict["general"]["log_file"])
    influxdb_config = get_influxdb_config(influxdb_config_dict["database"])
    print(influxdb_config)
    client_repo = InfluxRepo(influxdb_config, logger)
    
    # clear existing observations
    try:
        client_repo.clear_observations()
    except Exception as e:
        print(f"Error clearing observations: {e}")  

    
    def utc_now(delta):
        return datetime.now(timezone.utc) - timedelta(hours=delta)
    
    observations = []
    
    # insert 1000 different observations
    i = 0
    range_end = 100
    for i in range(range_end):
        
        obs = Observation(
            time=utc_now(range_end - i   ),
            node_source="test_node",
            node_source_id="node_123",
            latitude=40.7128 + (i * 0.001),
            longitude=-74.0060 + (i * 0.001),
            sensor_source="test_sensor",
            sensor_source_id="sensor_456",
            temperature=25.5 + (i % 10) - 5,
            humidity=60.0 + (i % 20) - 10,
            salinity=35.0 + (i % 5) - 2.5,
            temperature_unit="C",
            humidity_unit="%",
            salinity_unit="ppt",
            quality_codes= random.sample(range(6), random.randint(1, 6))
        )
        observations.append(obs)
        print(f"Created observation: {i}")
        i += 1
        import time
        time.sleep(0.01)  # slight delay to ensure different timestamps
    
    
    # insert observations
    try:
        print(f"Inserting {len(observations)} observations into InfluxDB...")
        client_repo.insert_observations(observations)
        print(f"Inserted {len(observations)} observations into InfluxDB.")
    except Exception as e:
        print(f"Error writing observation: {e}")
    
    tables = None
    start_time = utc_now(200)
    stop_time = utc_now(0)
        
        
    # query_observations
    try:
        queried_observations = client_repo.query_observations_range_date(start_time, stop_time)
        print(f"Queried {len(queried_observations)} observations from InfluxDB.")
        for obs in queried_observations[:5]:  # print first 5 observations
            print(obs)
    except Exception as e:
        print(f"Error querying observations: {e}")
    finally:
        client_repo.close()
        