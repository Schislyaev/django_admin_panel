import abc
import json

from redis import Redis

from typing import Any, Optional

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
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter: Redis):
        self.redis = redis_adapter
        self.redis.set('data', json.dumps({}))

    def save_state(self, state: dict) -> None:
        self.redis.set('data', json.dumps(state))

    def retrieve_state(self) -> dict:
        return json.loads(self.redis.get('data'))


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: RedisStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        state = self.storage.retrieve_state()
        state[key] = value
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        state = self.storage.retrieve_state()
        return state.get(key)


def main():
    storage = RedisStorage(adapter)
    s = State(storage)

    s.set_state('key', 123)
    print(s.get_state('key'))


if __name__ == '__main__':
    main()
