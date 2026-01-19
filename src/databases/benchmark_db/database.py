

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base
from pprint import pprint

from src.common.config import load_config
from src.common.logger import get_logger

config_file_path = './configs/config-benchmark_db.yml'
config = load_config(config_file_path)
logger = get_logger('benchmark_db', config['general']['log_file'])

print()
pprint(config)
print()

POSTGRES_USER =  config['postgres']['POSTGRES_USER']
POSTGRES_PASSWORD = config['postgres']['POSTGRES_PASSWORD']
POSTGRES_DB = config['postgres']['POSTGRES_DB']
POSTGRES_HOST = config['postgres']['POSTGRES_HOST']  # localhost or IP address
POSTGRES_PORT = config['postgres']['POSTGRES_PORT']  # default port
POSTGRES_SCHEMA = config['postgres']['POSTGRES_SCHEMA']

# POSTGRES_SCHEMA = "test_schema"

SQLALCHEMY_DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Create schema-aware metadata
metadata = MetaData(schema=POSTGRES_SCHEMA)

# Set schema-aware Base
Base = declarative_base(metadata=metadata)

engine = create_engine(SQLALCHEMY_DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
