# ETL SleepHealth - Airflow Scaffold

## Mục tiêu ✅
- Load CSV vào `raw` table
- Clean / validate
- Load vào `data warehouse` (Postgres)
- Logging & data quality checks
- DAG chạy hoàn chỉnh trong Docker

## Files tạo
- `docker-compose.yml` - Postgres + Airflow
- `Dockerfile` + `requirements.txt` - build airflow image
- `dags/sleep_etl_dag.py` - DAG orchestration
- `scripts/` - only `extract_load_raw.py` and `transform_load.py` are active; other utility scripts were moved to `scripts/deprecated/`
- `postgres/init.sql` - tạo bảng
- `data/sample_sleep.csv` - sample dữ liệu

## Chạy local (Windows)
1. Cài Docker Desktop và bật Docker Engine.
2. Ở thư mục project, build và chạy:

   docker compose up --build

3. Mở Airflow UI: http://localhost:8080 
   - User: `admin` / `admin`
   - Trigger DAG `sleep_etl` bằng UI.


