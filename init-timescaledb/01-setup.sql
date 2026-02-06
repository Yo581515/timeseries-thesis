
-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- measurements data table
CREATE TABLE IF NOT EXISTS measurements (
  time            timestamptz NOT NULL,

  node_source    TEXT,
  node_source_id  TEXT NOT NULL,
  
  latitude  DOUBLE PRECISION CHECK (latitude BETWEEN -90 AND 90),
  longitude DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),  

  sensor_source     TEXT,
  sensor_source_id  TEXT NOT NULL,

  parameter       TEXT NOT NULL,
  value           DOUBLE PRECISION,
  unit            TEXT,
  quality_codes   INT[],

  PRIMARY KEY (time, node_source_id, sensor_source_id, parameter)
);


SELECT create_hypertable('measurements', 'time', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);

CREATE INDEX ON measurements (node_source_id, time DESC);
CREATE INDEX ON measurements (parameter, time DESC);
CREATE INDEX ON measurements (sensor_source_id, time DESC);

