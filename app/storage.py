from dataclasses import dataclass
from datetime import datetime, timedelta
import sys
from typing import Optional


@dataclass
class StreamEntry:
    idx: str
    field_values: tuple[tuple[str, str]]


@dataclass
class Stream:
    entries: list[StreamEntry]

    def __init__(self, *entries: list[StreamEntry]):
        self.entries = list(entries)

    def append(self, entry: StreamEntry) -> None:
        if entry.idx <= "0-0":
            raise ValueError("The ID specified in XADD must be greater than 0-0")
        if self.entries and self.entries[-1].idx >= entry.idx:
            raise ValueError(
                "The ID specified in XADD is equal or smaller than the target stream top item"
            )

        self.entries.append(entry)

    def __len__(self):
        return len(self.entries)


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

    def get_stream(self, key: str) -> Stream:
        return self.get(key) or Stream()


storage = Storage()
