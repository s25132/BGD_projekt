CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS raw.ingestion_log (
    file_name TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (file_name, file_hash)
);

CREATE TABLE IF NOT EXISTS raw.transactions_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    batch_no INT NOT NULL,
    source_file TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    id TEXT,
    vendor_id TEXT,
    pickup_datetime TEXT,
    dropoff_datetime TEXT,
    passenger_count TEXT,
    pickup_longitude TEXT,
    pickup_latitude TEXT,
    dropoff_longitude TEXT,
    dropoff_latitude TEXT,
    store_and_fwd_flag TEXT,
    trip_duration TEXT
);

CREATE TABLE IF NOT EXISTS silver.batch_log (
    batch_no INT PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.transactions_clean (
    id TEXT PRIMARY KEY,
    batch_no INT NOT NULL,
    source_file TEXT NOT NULL,
    file_hash TEXT NOT NULL,

    vendor_id INT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    pickup_longitude DOUBLE PRECISION,
    pickup_latitude DOUBLE PRECISION,
    dropoff_longitude DOUBLE PRECISION,
    dropoff_latitude DOUBLE PRECISION,
    store_and_fwd_flag TEXT,
    trip_duration INT,

    is_valid BOOLEAN NOT NULL,
    validation_error TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.dim_vendor (
    vendor_id INT PRIMARY KEY,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT
);

CREATE TABLE IF NOT EXISTS gold.fact_trips (
    id TEXT PRIMARY KEY,
    vendor_id INT,
    date_id DATE,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    pickup_longitude DOUBLE PRECISION,
    pickup_latitude DOUBLE PRECISION,
    dropoff_longitude DOUBLE PRECISION,
    dropoff_latitude DOUBLE PRECISION,
    store_and_fwd_flag TEXT,
    trip_duration INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE VIEW gold.v_trip_report AS
SELECT
    f.id,
    d.year,
    d.month,
    d.day,
    f.vendor_id,
    f.pickup_datetime,
    f.dropoff_datetime,
    f.passenger_count,
    f.pickup_longitude,
    f.pickup_latitude,
    f.dropoff_longitude,
    f.dropoff_latitude,
    f.store_and_fwd_flag,
    f.trip_duration
FROM gold.fact_trips f
JOIN gold.dim_date d
    ON f.date_id = d.date_id;