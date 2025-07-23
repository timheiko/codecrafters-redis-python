from abc import ABC, abstractmethod
from dataclasses import dataclass

from app.resp import encode_simple


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
