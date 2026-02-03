import logging
from logging.handlers import RotatingFileHandler
import os

# Archived copy of log_utils.py
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
try:
    os.makedirs(LOG_DIR, exist_ok=True)
except Exception:
    pass
LOG_FILE = os.path.join(LOG_DIR, "etl.log")

handler = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
handler.setFormatter(formatter)

logger = logging.getLogger("etl")
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(handler)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
