from datetime import datetime, timedelta
from typing import Optional


class Storage:
    def __init__(self):
        self.__storage = {}

    def get(self, key: str) -> Optional[any]:
        if key in self.__storage:
            value, since, duration_ms = self.__storage[key]
            elapsed = datetime.now() - since
            if elapsed / timedelta(milliseconds=1) >= duration_ms:
                del self.__storage[key]
                return None
            else:
                return value
        return None

    def set(self, key: str, value: any, duration_ms: int = 10**10) -> None:
        self.__storage[key] = (value, datetime.now(), duration_ms)
