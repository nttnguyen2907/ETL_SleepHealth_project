import os
import sys
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from sqlalchemy import create_engine

# Inline minimal DB and logger instead of external modules to keep extract self-contained
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
logger = logging.getLogger('extract')
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3)
    fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

# Heuristics to detect relevant columns in large Excel files
PATIENT_KEYS = ['patient_id', 'patient', 'id', 'name', 'person id']
SLEEP_HOURS_KEYS = ['sleep_hours', 'sleep_duration', 'sleep duration', 'hours', 'sleep_time']
SLEEP_QUALITY_KEYS = ['sleep_quality', 'quality', 'quality of sleep', 'sleep_rating']
DATE_KEYS = ['measurement_date', 'date', 'measurement_date_utc', 'date of measurement']


def find_column(cols, candidates):
    cols_lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    # try fuzzy contains
    for col in cols:
        for cand in candidates:
            if cand.lower() in col.lower():
                return col
    return None


def standardize_df(df, top_n=None):
    cols = list(df.columns)
    patient_col = find_column(cols, PATIENT_KEYS)
    hours_col = find_column(cols, SLEEP_HOURS_KEYS)
    quality_col = find_column(cols, SLEEP_QUALITY_KEYS)
    date_col = find_column(cols, DATE_KEYS)

    logger.info('Detected columns: patient=%s, hours=%s, quality=%s, date=%s',
                patient_col, hours_col, quality_col, date_col)

    out = pd.DataFrame()

    if patient_col:
        out['patient_id'] = df[patient_col].astype(str)
    else:
        out['patient_id'] = df.index.astype(str)
        logger.warning('No patient column found; using index as patient_id')

    if hours_col:
        # Try to parse common formats like '7:30', '7h30m', '450m'
        out['sleep_hours'] = pd.to_numeric(df[hours_col], errors='coerce')
        # additional parsing for string formats
        def parse_hours(v):
            if pd.isna(v):
                return pd.NA
            if isinstance(v, (int, float)):
                return v
            s = str(v).strip()
            # HH:MM
            if ':' in s:
                parts = s.split(':')
                try:
                    return float(parts[0]) + float(parts[1]) / 60.0
                except Exception:
                    return pd.NA
            # 7h30m or 450m
            if 'h' in s or 'm' in s:
                hours = 0.0
                if 'h' in s:
                    try:
                        h = s.split('h')[0]
                        hours += float(h)
                    except Exception:
                        pass
                if 'm' in s:
                    try:
                        m = ''.join(c for c in s if c.isdigit())
                        hours += float(m) / 60.0
                    except Exception:
                        pass
                return hours if hours != 0.0 else pd.NA
            return pd.to_numeric(s, errors='coerce')
        out['sleep_hours'] = out['sleep_hours'].apply(parse_hours)
    else:
        out['sleep_hours'] = pd.NA
        logger.warning('No sleep hours column found; filling with NA')

    if quality_col:
        out['sleep_quality'] = df[quality_col].astype(str)
    else:
        out['sleep_quality'] = pd.NA
        logger.warning('No sleep quality column found; filling with NA')

    if date_col:
        out['measurement_date'] = pd.to_datetime(df[date_col], errors='coerce')
    else:
        out['measurement_date'] = pd.NaT
        logger.warning('No date column found; filling with NA')

    if top_n:
        out = out.head(top_n)

    return out


def main():
    # Allow optional CLI args: path and top_n
    path = sys.argv[1] if len(sys.argv) > 1 else None
    top_n = int(sys.argv[2]) if len(sys.argv) > 2 else None

    project_data = '/opt/airflow/data'
    candidates = []
    if path:
        candidates.append(path)
    candidates += [
        os.path.join(project_data, 'Sleep_health_and_lifestyle_dataset.xlsx'),
        os.path.join(project_data, 'sample_sleep.xlsx'),
        os.path.join(project_data, 'sample_sleep.csv')
    ]

    selected = None
    for c in candidates:
        if c and os.path.exists(c):
            selected = c
            break

    if not selected:
        logger.error('No input file found. Place Excel or CSV in %s or pass path as argument', project_data)
        return

    logger.info('Reading input file: %s', selected)

    try:
        if selected.lower().endswith(('.xls', '.xlsx')):
            raw = pd.read_excel(selected)
        else:
            raw = pd.read_csv(selected)
    except Exception as e:
        logger.exception('Failed to read input file: %s', e)
        return

    # If the source is the big Sleep_health... file and top_n not set, default to 20
    if os.path.basename(selected).lower().startswith('sleep_health') and top_n is None:
        top_n = 20
        logger.info('Detected large dataset; defaulting to top_n=20')

    df = standardize_df(raw, top_n=top_n)

    # Normalize dates
    if 'measurement_date' in df.columns:
        df['measurement_date'] = pd.to_datetime(df['measurement_date']).dt.date

    engine = get_engine()
    df.to_sql('raw_sleep', engine, if_exists='append', index=False)
    logger.info('Inserted %d rows into raw_sleep', len(df))


if __name__ == '__main__':
    main()
