CREATE TABLE IF NOT EXISTS raw_sleep(
    patient_id TEXT,
    sleep_hours NUMERIC,
    sleep_quality TEXT,
    measurement_date DATE
);

CREATE TABLE IF NOT EXISTS dw_sleep(
    patient_id TEXT,
    sleep_hours NUMERIC,
    sleep_quality TEXT,
    measurement_date DATE,
    sleep_quality_score INTEGER
);

CREATE TABLE IF NOT EXISTS etl_log(
    id SERIAL PRIMARY KEY,
    process TEXT,
    rows_loaded INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
