import sys
from scripts.deprecated.db_utils import get_engine
import pandas as pd
from scripts.deprecated.log_utils import logger

# Archived copy of validate.py

def main():
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql("select * from raw_sleep", conn)

    if df.empty:
        logger.warning("No rows to validate")
        sys.exit(1)

    # Basic checks
    if df['sleep_hours'].isnull().any():
        logger.error("Validation failed: Null sleep_hours found")
        sys.exit(1)

    if (df['sleep_hours'] < 0).any():
        logger.error("Validation failed: Negative sleep_hours found")
        sys.exit(1)

    logger.info(f"Validation passed for {len(df)} rows")


if __name__ == '__main__':
    main()