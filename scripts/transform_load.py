import os
import logging
from logging.handlers import RotatingFileHandler
from sqlalchemy import create_engine, text
import pandas as pd

# Inline DB and logger to make transform self-contained
def get_engine(db=None):
    host = os.getenv("PG_HOST", "postgres")
    port = os.getenv("PG_PORT", "5432")
    user = os.getenv("PG_USER", "airflow")
    pwd = os.getenv("PG_PASSWORD", "airflow")
    db_name = db or os.getenv("PG_DB", "sleep_dw")
    conn = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db_name}"
    return create_engine(conn)

# Setup logger
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
LOG_DIR = os.path.join(PROJECT_ROOT, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, 'etl.log')
logger = logging.getLogger('transform')
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3)
    fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)


def score(q):
    if not isinstance(q, str):
        return 0
    m = {'good': 3, 'fair': 2, 'poor': 1}
    return m.get(q.lower(), 0)


def main():
    engine = get_engine()
    with engine.begin() as conn:
        df = pd.read_sql("select * from raw_sleep where measurement_date is not null", conn)
        if df.empty:
            logger.warning('No rows to transform')
            return
        df['measurement_date'] = pd.to_datetime(df['measurement_date'])
        df['sleep_quality_score'] = df['sleep_quality'].apply(score)
        df.to_sql('dw_sleep', conn, if_exists='append', index=False)
        conn.execute(text("INSERT INTO etl_log(process,rows_loaded) VALUES (:p,:r)"), [{"p": "transform_load", "r": len(df)}])
        logger.info(f"Loaded {len(df)} rows into dw_sleep")


if __name__ == '__main__':
    main()
