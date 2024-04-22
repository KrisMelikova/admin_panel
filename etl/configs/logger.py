import logging

from configs.etl_config import settings

if settings.debug.upper() == "TRUE":
    logs_level = logging.DEBUG
else:
    logs_level = logging.INFO

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    level=logs_level,
)

etl_logger = logging.getLogger("ETL service")
