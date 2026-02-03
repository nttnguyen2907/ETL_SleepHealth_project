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

## Notes
- Sample CSV có 1 hàng có `sleep_hours` null để demo validation fail. Bạn có thể sửa `data/sample_sleep.csv` để thử pass.
- Pipeline sẽ ưu tiên `data/sample_sleep.xlsx` nếu có.
- Nếu bạn có file Excel gốc `Sleep_health_and_lifestyle_dataset.xlsx`, `extract_load_raw.py` đã hỗ trợ đọc trực tiếp file đó, tự động lấy **20 dòng đầu** (mặc định) và chèn vào `raw_sleep`:
  ```bash
  python scripts/extract_load_raw.py path/to/Sleep_health_and_lifestyle_dataset.xlsx 20
  ```
  Hoặc đặt file vào `data/Sleep_health_and_lifestyle_dataset.xlsx` và chạy extract không tham số.

_Note: `scripts/generate_excel_sample.py` đã bị gỡ và được lưu trữ trong `scripts/deprecated/` — không cần để file này trong workflow._
- Để kiểm tra nhanh, sau khi tạo `data/sample_sleep.xlsx` có thể chạy:
  ```bash
  docker-compose run --rm airflow bash -lc "python /opt/airflow/scripts/extract_load_raw.py"
  ```
- Logs: xem `docker compose logs -f` hoặc trong Airflow UI task logs.

## Thay đổi tiếp theo đề xuất 🔧
- Thêm Airflow Connections và Variables thay vì dùng env vars
- Thêm unit tests cho các script
- Thêm retry/backoff logic cho các bước IO nặng

