import abc
from typing import Any, Optional
import json


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = 'data.json'):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        with open(self.file_path, 'a', encoding='utf-8') as file:
            json.dump(state, file)

    def retriev_state(self) -> dict:
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                try:
                    data = json.load(file)
                except Exception as ex:
                    if ex.msg == 'Expecting value':
                        return {}
        except Exception as ex:
            if ex.errno == 2:
                f = open('data.json', 'a', encoding='utf-8')
                f.close
                return {}

        return data


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: JsonFileStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        state = self.storage.retriev_state()
        state[key] = value
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        state = self.storage.retriev_state()
        if key not in state.keys():
            return None
        return state.get(key)

storage = JsonFileStorage()
s = State(storage)
d = s.get_state(key='name')
print(d)
