from datetime import datetime, timedelta
import sys
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

    def get_list(self, key: str) -> list[any]:
        return self.get(key) or []

    def get_list_range(self, key: str, start: int, end: int) -> list[any]:
        values = self.get_list(key)
        n = len(values)
        start = start if start >= 0 else max(0, n + start)
        end = (end if end >= 0 else max(0, n + end)) + 1
        return values[start:end]

    def clean(self):
        self.__storage = {}


storage = Storage()
