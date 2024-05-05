from dotenv import load_dotenv
from pydantic import BaseSettings, Field

load_dotenv()


class ElasticsearchConnection(BaseSettings):
    """ Elasticsearch connection settings. """

    host: str = Field('http://localhost:9200', env="ES_HOST")


class ElasticsearchSettings(BaseSettings):
    """ Elasticsearch settings. """

    connection: ElasticsearchConnection = ElasticsearchConnection()


class PostgresSettings(BaseSettings):
    """ Postgres settings. """

    host: str = Field("127.0.0.1", env="DB_HOST")
    port: str = Field(5432, env="DB_PORT")
    dbname: str = Field("movies_database", env="DB_NAME")
    user: str = Field("app", env="DB_USER")
    password: str = Field(env="DB_PASSWORD")
    connect_timeout: int = 1


class Settings(BaseSettings):
    """ ETL service settings. """

    debug: str = Field("False", env="DEBUG")
    default_updated_at: str = Field(env="ETL_DEFAULT_UPDATED_AT")
    delay: int = Field(30, env="ETL_DELAY")
    page_size: int = Field(500, env="ETL_PAGE_SIZE")
    storage_file_path: str = Field("../storage/storage.json", env="ETL_STORAGE_FILE_PATH")
    entities: list = ["genre", "person", "film_work"]
    db_schema: str = Field("content", env="DB_SCHEME")
    postgres: PostgresSettings = PostgresSettings()
    es: ElasticsearchSettings = ElasticsearchSettings()


settings = Settings()
