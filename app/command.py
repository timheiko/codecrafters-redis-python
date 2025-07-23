from abc import ABC, abstractmethod
from dataclasses import dataclass

from app.resp import encode, encode_simple

from app.storage import storage


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


class PING(RedisCommand):
    def __init__(self, *_args: list[str]):
        pass

    def execute(self):
        return encode_simple("PONG")


@dataclass
class ECHO(RedisCommand):
    args = list[str]

    def __init__(self, *args: list[str]):
        self.args = list(args)

    def execute(self):
        return encode_simple(" ".join(self.args))


@dataclass
class SET(RedisCommand):
    key: str
    value: any
    ttlms: float | None

    def __init__(self, *args: list[str]):
        match args:
            case [key, value]:
                self.key = key
                self.value = value
                self.ttlms = None
            case [key, value, ("PX" | "px" | "Px" | "pX"), ttlms]:
                self.key = key
                self.value = value
                self.ttlms = float(ttlms)
            case _:
                raise ValueError

    def execute(self):
        if self.ttlms is not None:
            storage.set(self.key, self.value, self.ttlms)
        else:
            storage.set(self.key, self.value)
        return encode_simple("OK")


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
