import abc
from datetime import datetime
from json import JSONDecoder, JSONEncoder
from typing import Any

decoder = JSONDecoder()
encoder = JSONEncoder()


class BaseStorage(abc.ABC):
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        pass


class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter):
        self.redis = redis_adapter

    def save_state(self, state: dict) -> None:
        self.redis.set('data', encoder.encode(state))

    def retrieve_state(self) -> dict | None:
        res = self.redis.get('data')
        if res == 'null' or res is None:
            # self.redis.set('data', encoder.encode(None))
            return None
        return decoder.decode(res)


class State:
    def __init__(self, storage: RedisStorage):
        self.storage = storage
        self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        state = self.storage.retrieve_state()
        if state is not None:
            state[key] = value if type(value) is not datetime else str(value)
            self.storage.save_state(state)
        else:
            value = value if type(value) is not datetime else str(value)
            self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        try:
            state = self.storage.retrieve_state()
            return state.get(key)
        except AttributeError:
            return None


