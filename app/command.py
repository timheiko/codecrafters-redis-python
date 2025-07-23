from abc import ABC, abstractmethod
from dataclasses import dataclass

from app.resp import encode, encode_simple

from app.storage import storage


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

    def __getitem__(self, key):
        return self.__registry[key]

    def __contains__(self, key):
        return key in self.__registry


registry = CommandRegistry()


class RedisCommand(ABC):
    @abstractmethod
    def __init__(self, args: list[str]):
        """
        Instantiates a given command from arguments
        """

    @abstractmethod
    def execute(self) -> bytes:
        """
        Executes the command and returns bytes to be sent to client
        """


@registry.register
class PING(RedisCommand):
    def __init__(self, *_args: list[str]):
        pass

    def execute(self):
        return encode_simple("PONG")


@registry.register
@dataclass
class ECHO(RedisCommand):
    args = list[str]

    def __init__(self, *args: list[str]):
        self.args = list(args)

    def execute(self):
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

    def execute(self):
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

    def execute(self):
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

    def execute(self):
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

    def execute(self):
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

    def execute(self):
        return encode(storage.get_list_range(self.key, self.start, self.end))
