# src/databases/timescaledb/config.py

class TimeScaleDBConfigurationException(Exception):
    pass


class TimeScaleDBConfig:
    def __init__(self, username, password, host, port, database, schema):
        self.TIMESCALEDB_USER = username
        self.TIMESCALEDB_PASSWORD = password
        self.TIMESCALEDB_HOST = host
        self.TIMESCALEDB_PORT = port
        self.TIMESCALEDB_DATABASE = database
        self.TIMESCALEDB_SCHEMA = schema

    def __str__(self):
        return (
            "TimeScaleDB Configuration:\n"
            f"Host: {self.TIMESCALEDB_HOST}\n"
            f"Port: {self.TIMESCALEDB_PORT}\n"
            f"Database: {self.TIMESCALEDB_DATABASE}\n"
            f"Schema: {self.TIMESCALEDB_SCHEMA}\n"
        )


def get_timescaledb_config(config_dict: dict) -> TimeScaleDBConfig:
    try:
        return TimeScaleDBConfig(
            username=config_dict["TIMESCALEDB_USER"],
            password=config_dict["TIMESCALEDB_PASSWORD"],
            host=config_dict["TIMESCALEDB_HOST"],
            port=config_dict["TIMESCALEDB_PORT"],
            database=config_dict["TIMESCALEDB_DATABASE"],
            schema=config_dict["TIMESCALEDB_SCHEMA"],
        )
    except KeyError as e:
        raise TimeScaleDBConfigurationException(f"Missing config key: {e}")
