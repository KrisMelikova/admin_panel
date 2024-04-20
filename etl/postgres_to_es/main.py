import time

from etl.configs.etl_config import settings
from etl.configs.logger import etl_logger
from etl.storage.state_storage import JsonFileStorage, State


def run_data_transfer_from_pg_to_es() -> None:
    """ ETL Service for Online Cinema. """

    etl_logger.info("ETL service started")

    storage = JsonFileStorage(settings.storage_file_path)
    state = State(storage)
    updated_at = state.get_state("updated_at")

    if not updated_at:
        updated_at = settings.default_updated_at


if __name__ == "__main__":
    etl_logger.info("ETL service initialization")

    while True:
        run_data_transfer_from_pg_to_es()

        time.sleep(settings.delay)
