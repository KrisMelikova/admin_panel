from dotenv import load_dotenv
from pydantic import BaseSettings, Field

load_dotenv()


class Settings(BaseSettings):
    """ ETL service settings. """

    debug: str = Field("False", env="DEBUG")
    default_updated_at: str = Field(env="ETL_DEFAULT_UPDATED_AT")
    delay: int = Field(30, env="ETL_DELAY")
    page_size: int = 500
    storage_file_path: str = Field("../storage/storage.json", env="ETL_STORAGE_FILE_PATH")


settings = Settings()
