import abc
import json
import os
from typing import Any, Dict

from etl.configs.etl_config import settings


class BaseStorage(abc.ABC):
    """ Абстрактное хранилище состояния. """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """ Сохранить состояние в хранилище. """

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """ Получить состояние из хранилища. """


class JsonFileStorage(BaseStorage):
    """
    Реализация хранилища, использующего локальный файл.
    Формат хранения: JSON.
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        """ Сохранить состояние в хранилище."""

        with open(self.file_path, 'w') as f:
            json.dump(state, f)

    def retrieve_state(self) -> Dict[str, Any]:
        """ Получить состояние из хранилища. """

        if not os.path.exists(self.file_path):
            os.mknod(settings.storage_file_path)

        with open(self.file_path, 'r') as f:
            try:
                state_properties = json.load(f)
                return state_properties
            except json.decoder.JSONDecodeError:
                return {}


class State:
    """ Класс для работы с состояниями. """

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """ Установить состояние для определённого ключа. """

        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """ Получить состояние по определённому ключу. """
        state = self.storage.retrieve_state()

        return state[key] if key in state else None
