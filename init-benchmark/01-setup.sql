CREATE TABLE IF NOT EXISTS benchmark_one (
    id BIGSERIAL PRIMARY KEY,

    benchmark_name TEXT NOT NULL,
    database_name TEXT NOT NULL,
    dataset_name TEXT NOT NULL,

    total_points INTEGER NOT NULL,

    total_seconds DOUBLE PRECISION NOT NULL,
    throughput_points_per_sec DOUBLE PRECISION NOT NULL,

    success BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_benchmark_one_benchmark_name
    ON benchmark_one (benchmark_name);

CREATE INDEX IF NOT EXISTS idx_benchmark_one_database_name
    ON benchmark_one (database_name);

CREATE INDEX IF NOT EXISTS idx_benchmark_one_dataset_name
    ON benchmark_one (dataset_name);

CREATE INDEX IF NOT EXISTS idx_benchmark_one_created_at
    ON benchmark_one (created_at DESC);