import os
from sqlalchemy import create_engine

# Archived copy of db_utils.py

def get_engine(db=None):
    host = os.getenv("PG_HOST", "postgres")
    port = os.getenv("PG_PORT", "5432")
    user = os.getenv("PG_USER", "airflow")
    pwd = os.getenv("PG_PASSWORD", "airflow")
    db_name = db or os.getenv("PG_DB", "sleep_dw")
    conn = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db_name}"
    return create_engine(conn)
