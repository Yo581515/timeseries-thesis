# src/databases/benchmark_db/config.py

class PostgresBConfigurationException(Exception):
    pass


class PostgresBConfig:
    def __init__(self, username, password, host, port, database, schema):
        self.POSTGRES_USER = username
        self.POSTGRES_PASSWORD = password
        self.POSTGRES_HOST = host
        self.POSTGRES_PORT = port
        self.POSTGRES_DATABASE = database
        self.POSTGRES_SCHEMA = schema

    def __str__(self):
        return (
            "PostgresB Configuration:\n"
            f"Host: {self.POSTGRES_HOST}\n"
            f"Port: {self.POSTGRES_PORT}\n"
            f"Database: {self.POSTGRES_DATABASE}\n"
            f"Schema: {self.POSTGRES_SCHEMA}\n"
        )


def get_postgres_config(config_dict: dict) -> PostgresBConfig:
    try:
        return PostgresBConfig(
            username=config_dict["POSTGRES_USER"],
            password=config_dict["POSTGRES_PASSWORD"],
            host=config_dict["POSTGRES_HOST"],
            port=config_dict["POSTGRES_PORT"],
            database=config_dict["POSTGRES_DATABASE"],
            schema=config_dict["POSTGRES_SCHEMA"],
        )
    except KeyError as e:
        raise PostgresBConfigurationException(f"Missing config key: {e}")
