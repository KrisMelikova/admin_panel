import abc
import json
import os
from typing import Any, Dict

from configs.etl_config import settings


class BaseStorage(abc.ABC):
    """ Abstract state store. """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """ Save state to storage. """

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """ Get state from storage. """


class JsonFileStorage(BaseStorage):
    """
    Implementation of a repository that uses a local file.
    Storage format: JSON.
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        """ Save state to storage. """

        with open(self.file_path, 'w') as f:
            json.dump(state, f)

    def retrieve_state(self) -> Dict[str, Any]:
        """ Get state from storage. """

        if not os.path.exists(self.file_path):
            os.mknod(settings.storage_file_path)

        with open(self.file_path, 'r') as f:
            try:
                state_properties = json.load(f)
                return state_properties
            except json.decoder.JSONDecodeError:
                return {}


class State:
    """ Class for working with states. """

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """ Set the state for a specific key. """

        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """ Get the state for a specific key.  """
        state = self.storage.retrieve_state()

        return state[key] if key in state else None
