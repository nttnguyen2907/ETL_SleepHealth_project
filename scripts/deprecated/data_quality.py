from scripts.deprecated.db_utils import get_engine
from sqlalchemy import text
import sys
from scripts.deprecated.log_utils import logger

# Archived copy of data_quality.py

def main():
    engine = get_engine()
    with engine.connect() as conn:
        r = conn.execute(text("select count(*) from dw_sleep")).scalar()
        if not r or r == 0:
            logger.error("Data quality check failed: zero rows in dw_sleep")
            sys.exit(1)
        logger.info(f"Data quality check passed: {r} rows in dw_sleep")


if __name__ == '__main__':
    main()