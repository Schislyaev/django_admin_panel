import abc
from datetime import datetime

from redis import Redis
from json import JSONDecoder, JSONEncoder

from typing import Any, Optional

de = JSONDecoder()
en = JSONEncoder()

adapter = Redis(
                host='127.0.0.1',
                port=6379,
                db=0,
                password=None,
                socket_timeout=None,
                decode_responses=True
)


class BaseStorage:
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
        self.redis.set('data', en.encode(state))

    def retrieve_state(self) -> dict | None:
        res = self.redis.get('data')
        if res == 'null' or res is None:
            self.redis.set('data', en.encode(None))
            return None
        return de.decode(res)


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: RedisStorage):
        self.storage = storage
        self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        state = self.storage.retrieve_state()
        try:
            state[key] = value if type(value) is not datetime else str(value)
            self.storage.save_state(state)
        except TypeError:
            value = value if type(value) is not datetime else str(value)
            self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        try:
            state = self.storage.retrieve_state()
            return state.get(key)
        except AttributeError:
            return None


def main():
    pass


if __name__ == '__main__':
    main()

