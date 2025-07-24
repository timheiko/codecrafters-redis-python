from abc import ABC, abstractmethod
import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
import itertools

from app.resp import encode, encode_simple

from app.storage import Stream, StreamEntry, storage


class CommandRegistry:
    def __init__(self):
        self.__registry = {}

    def register(self, cls):
        if not issubclass(cls, RedisCommand):
            raise ValueError(f"{cls} does not subclass {RedisCommand}")

        cls_name = cls.__name__
        if cls_name in self:
            raise KeyError(f"Already registered <{cls_name}>")

        self.__registry[cls_name] = cls
        return cls

    async def execute(self, command: str, *args: list[str]) -> bytes:
        cmd = command.upper()
        if cmd in self:
            return await self[cmd](*args).execute()
        raise Exception(f"Unknown command: {command}")

    def __getitem__(self, key):
        return self.__registry[key]

    def __contains__(self, key):
        return key in self.__registry


registry = CommandRegistry()

waiting_queue = defaultdict(deque)


class RedisCommand(ABC):
    @abstractmethod
    def __init__(self, args: list[str]):
        """
        Instantiates a given command from arguments
        """

    @abstractmethod
    async def execute(self) -> bytes:
        """
        Executes the command and returns bytes to be sent to client
        """


@registry.register
class PING(RedisCommand):
    def __init__(self, *_args: list[str]):
        pass

    async def execute(self):
        return encode_simple("PONG")


@registry.register
@dataclass
class ECHO(RedisCommand):
    args = list[str]

    def __init__(self, *args: list[str]):
        self.args = list(args)

    async def execute(self):
        return encode_simple(" ".join(self.args))


@registry.register
@dataclass
class SET(RedisCommand):
    key: str
    value: any
    ttlms: float | None

    def __init__(self, *args: list[str]):
        match args:
            case [key, value, *rest]:
                self.key = key
                self.value = value
                self.ttlms = None
                match rest:
                    case [unit, ttl]:
                        match unit.upper():
                            case "PX":
                                self.ttlms = float(ttl)
                            case "EX":
                                self.ttlms = float(ttl) * 1_000
                            case _:
                                raise ValueError(f"Unknown unit: {unit}")
                    case []:
                        pass
                    case _:
                        raise ValueError(f"Expected [unit, ttl], got {rest}")
            case _:
                raise ValueError

    async def execute(self):
        if self.ttlms is not None:
            storage.set(self.key, self.value, self.ttlms)
        else:
            storage.set(self.key, self.value)
        return encode_simple("OK")


@registry.register
@dataclass
class GET(RedisCommand):
    key: str

    def __init__(self, *args: list[str]):
        match args:
            case [key, *_]:
                self.key = key
            case _:
                raise ValueError

    async def execute(self):
        return encode(storage.get(self.key))


@registry.register
@dataclass
class LLEN(RedisCommand):
    key: str

    def __init__(self, *args: list[str]):
        match args:
            case [key, *_]:
                self.key = key
            case _:
                raise ValueError

    async def execute(self):
        return encode(len(storage.get_list(self.key)))


@registry.register
@dataclass
class LPOP(RedisCommand):
    key: str
    count: int

    def __init__(self, *args: list[str]):
        match args:
            case [key]:
                self.key = key
                self.count = 1
            case [key, count, *_]:
                self.key = key
                self.count = int(count)
            case _:
                raise ValueError

    async def execute(self):
        values = storage.get_list(self.key)
        if not values:
            return encode(None)
        elif self.count == 1:
            return encode(values.pop(0))
        popped = values[: self.count]
        values[: self.count] = []
        return encode(popped)


@registry.register
@dataclass
class LRANGE(RedisCommand):
    key: str
    start: int
    end: int

    def __init__(self, *args: list[str]):
        match args:
            case [key, start, end]:
                self.key = key
                self.start = int(start)
                self.end = int(end)
            case _:
                raise ValueError

    async def execute(self):
        return encode(storage.get_list_range(self.key, self.start, self.end))


@registry.register
@dataclass
class RPUSH(RedisCommand):
    key: str
    items: list[str]

    def __init__(self, *args: list[str]):
        match args:
            case [key, *items]:
                self.key = key
                self.items = items
            case _:
                raise ValueError

    async def execute(self):
        values = storage.get_list(self.key)
        values.extend(self.items)
        storage.set(self.key, values)
        for _ in self.items:
            if len(waiting_queue[self.key]) > 0:
                waiting_queue[self.key].popleft().set_result(True)
        return encode(len(values))


@registry.register
@dataclass
class LPUSH(RedisCommand):
    key: str
    items: list[str]

    def __init__(self, *args: list[str]):
        match args:
            case [key, *items]:
                self.key = key
                self.items = items
            case _:
                raise ValueError

    async def execute(self):
        values = storage.get_list(self.key)
        values = self.items[::-1] + values
        storage.set(self.key, values)
        for _ in self.items:
            if len(waiting_queue[self.key]) > 0:
                waiting_queue[self.key].popleft().set_result(True)
        return encode(len(values))


@registry.register
@dataclass
class BLPOP(RedisCommand):
    key: str
    timeout: float | None

    def __init__(self, *args: list[str]):
        match args:
            case [key, timeout]:
                self.key = key
                self.timeout = float(timeout) if float(timeout) > 0 else None
            case _:
                raise ValueError

    async def execute(self):
        values = storage.get_list(self.key)
        if values:
            return encode([self.key, values.pop(0)])
        else:
            loop = asyncio.get_event_loop()
            future = loop.create_future()
            waiting_queue[self.key].append(future)
            try:
                _ = await asyncio.wait_for(future, self.timeout)
                return encode([self.key, storage.get_list(self.key).pop(0)])
            except TimeoutError:
                return encode(None)


@registry.register
@dataclass
class TYPE(RedisCommand):
    key: str

    def __init__(self, *args: list[str]):
        match args:
            case [key]:
                self.key = key
            case _:
                raise ValueError

    async def execute(self):
        match storage.get(self.key):
            case None:
                return encode_simple("none")
            case str(_):
                return encode_simple("string")
            case [*_]:
                return encode_simple("list")
            case Stream():
                return encode_simple("stream")
            case _:
                raise ValueError


@registry.register
@dataclass
class XADD(RedisCommand):
    key: str
    idx: str
    field_values: tuple[tuple[str, str]]

    def __init__(self, *args: list[str]):
        match args:
            case [key, idx, *field_vals]:
                self.key = key
                self.idx = idx
                self.field_values = tuple(
                    (field, value)
                    for field, value in itertools.batched(field_vals, 2, strict=True)
                )
            case _:
                raise ValueError

    async def execute(self):
        stream = storage.get_stream(self.key)
        try:
            stream.append(StreamEntry(idx=self.idx, field_values=self.field_values))
            storage.set(self.key, stream)
            return encode(self.idx)
        except ValueError as error:
            return encode(error)
