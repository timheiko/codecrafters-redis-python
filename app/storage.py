from dataclasses import dataclass
from datetime import datetime, timedelta
from io import BufferedRandom
from os import path
import time
from typing import Optional, Self

from app.log import log


@dataclass
class StreamEntry:
    idx: str
    field_values: tuple[str]
    ts: datetime

    def __init__(self, idx: str, field_values: tuple[str], ts: datetime | None = None):
        if idx == "*":
            self.idx = f"{int(time.time() * 1_000)}-*"
        else:
            self.idx = idx
        self.field_values = field_values
        self.ts = ts if ts is not None else datetime.now()

    def increment_idx_seq_num_and_get(self, other: Self | None = None) -> Self:
        match self.idx.split("-"):
            case [idx_ms, "*"]:
                if other is None or other.idx < self.idx:
                    seq_num = 1 if idx_ms == "0" else 0
                else:
                    other_idx_ms, other_idx_seq_num = other.idx.split("-")
                    if idx_ms == other_idx_ms:
                        seq_num = int(other_idx_seq_num) + 1
                    else:
                        seq_num = 0

                return StreamEntry(
                    idx=f"{idx_ms}-{seq_num}",
                    field_values=self.field_values,
                    ts=self.ts,
                )
            case ["*"]:
                idx_ms = int(time.time() * 1_000)
                return StreamEntry(
                    idx=f"{idx_ms}-0", field_values=self.field_values, ts=self.ts
                )
            case _:
                return self


@dataclass
class Stream:
    entries: list[StreamEntry]

    def __init__(self, *entries: list[StreamEntry]):
        self.entries = list(entries)

    def append(self, entry: StreamEntry) -> StreamEntry:
        if entry.increment_idx_seq_num_and_get().idx <= "0-0":
            raise ValueError("The ID specified in XADD must be greater than 0-0")

        if self.entries:
            entry = entry.increment_idx_seq_num_and_get(self.entries[-1])
            if self.entries[-1].idx >= entry.idx:
                raise ValueError(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                )
        else:
            entry = entry.increment_idx_seq_num_and_get(None)

        self.entries.append(entry)
        return entry

    def __len__(self):
        return len(self.entries)


class Storage:
    def __init__(self):
        self.__storage = {}

    def load_from_rdb_dump(
        self, dirname: str | None = None, dbfilename: str | None = None
    ) -> None:
        if dirname is not None and dbfilename is not None:
            self.__storage |= load_from_rdb_dump(dirname, dbfilename)

    def get(self, key: str) -> Optional[any]:
        if key in self.__storage:
            value, expiration = self.__storage[key]
            if int(datetime.now().timestamp() * 1_000) >= expiration:
                del self.__storage[key]
                return None
            else:
                return value
        return None

    def set(self, key: str, value: any, duration_ms: int = 10**10) -> None:
        self.__storage[key] = (
            value,
            int(datetime.now().timestamp() * 1_000) + duration_ms,
        )

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

    def get_keys(self) -> list[str]:
        return list(self.__storage.keys())

    def get_sorted_set(self, set_name: str) -> dict[str, float]:
        return self.get(set_name) or {}

    def add_to_sorted_set(self, set_name: str, priority: float, value: str) -> bool:
        sorted_set = self.get_sorted_set(set_name)
        is_new = value not in sorted_set
        sorted_set[value] = priority
        self.set(set_name, sorted_set)
        return is_new


storage = Storage()


def load_from_rdb_dump(dirname: str, dbfilename: str) -> dict[str, tuple[any, int]]:
    def read_int(file: BufferedRandom, count: int) -> int:
        return int.from_bytes(file.read(count), byteorder="big")

    def read_size(file: BufferedRandom) -> int:
        first_byte = read_int(file, 1)
        match first_byte >> 6:
            case 0:
                return first_byte
            case 1:
                return ((first_byte & 0x5F) << 8) | read_int(file, 1)
            case 2:
                return read_int(file, 4)
            case 3:
                last_six_bits = first_byte & 0x3F
                match last_six_bits:
                    case 0:
                        return -1
                    case 1:
                        return -2
                    case 2:
                        return -4
                    case _:
                        raise ValueError(f"Unknown special format {last_six_bits}")

    def read_var_length_value(file: BufferedRandom) -> any:
        size = read_size(file)
        match size:
            case -1:
                return read_int(file, 1)
            case -2:
                return read_int(file, 2)
            case -4:
                return read_int(file, 4)
            case _:
                value = file.read(size).decode()
                return value

    def read_key_value(file: BufferedRandom) -> tuple[str, any]:
        value_type = file.read(1)
        key = read_var_length_value(file)
        match value_type:
            case b"\x00":  # string
                return (key, read_var_length_value(file))
            case _:
                raise ValueError(f"Unknown value type: {value_type}")

    rdbpath = path.join(dirname, dbfilename)
    if not path.exists(rdbpath):
        log("RDB dump path does not exist:", rdbpath)
        return {}

    contents = {}
    with open(rdbpath, "b+r") as file:
        file.seek(9)  # 5 bytes magic string "REDIS" + 4 bytes version string

        while len(opcode := file.read(1)) > 0:
            match opcode:
                case b"\xff":
                    break
                case b"\xfe":
                    _db_num = read_int(file, 1)
                case b"\xfd":
                    expiration_s = int.from_bytes(file.read(4), byteorder="little")
                    key, value = read_key_value(file)
                    contents[key] = (value, expiration_s * 1_000)
                    log("Loaded RDB EX entry", key, value, expiration_s)
                case b"\xfc":
                    expiration_ms = int.from_bytes(file.read(8), byteorder="little")
                    key, value = read_key_value(file)
                    contents[key] = (value, expiration_ms)
                    log("Loaded RDB PX entry", key, value, expiration_ms)
                case b"\xfb":
                    _hash_table_size = read_size(file)
                    _hash_table_size_exp = read_size(file)
                case b"\xfa":
                    key = read_var_length_value(file)
                    value = read_var_length_value(file)
                    log("Loaded RDB AUX entry", key, value)
                case b"\x00":
                    key = read_var_length_value(file)
                    value = read_var_length_value(file)
                    contents[key] = (value, 10**20)
                    log("Loaded RDB non-exp entry", key, value)
                case _:
                    raise ValueError(opcode)

    return contents
