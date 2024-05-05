import time

from elasticsearch import Elasticsearch

from configs.etl_config import settings
from configs.logger import etl_logger
from db.postgres_db import PostgresConnection
from postgres_to_es.extractor import Extractor
from postgres_to_es.loader import Loader
from postgres_to_es.transformer import Transformer
from storage.state_storage import JsonFileStorage, State

PG_CONNECTION = PostgresConnection(settings.postgres.dict())
ES_CONNECTION = Elasticsearch(settings.es.connection.host, max_retries=15)


def run_data_transfer_from_pg_to_es() -> None:
    """ ETL Service for Online Cinema. """

    etl_logger.info("ETL service started")

    storage: JsonFileStorage = JsonFileStorage(settings.storage_file_path)
    state: State = State(storage)
    updated_at: str = state.get_state("updated_at")

    if not updated_at:
        updated_at: str = settings.default_updated_at

    etl_logger.info(f"ETL service last 'updated_at' is {updated_at}")

    loader = Loader(es=ES_CONNECTION)
    transformer = Transformer(result_handler=loader.process)
    extractor = Extractor(postgres=PG_CONNECTION, result_handler=transformer.transform)

    for entity in settings.entities:
        extractor.proccess(
            table=entity,
            schema=settings.db_schema,
            page_size=settings.page_size,
            updated_at=updated_at,
        )


if __name__ == "__main__":
    etl_logger.info("ETL service initialization")

    while True:
        run_data_transfer_from_pg_to_es()

        time.sleep(settings.delay)
