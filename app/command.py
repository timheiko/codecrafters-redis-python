from abc import ABC, abstractmethod
import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, ClassVar

from app.args import parse_args
from app.log import log
from app.resp import decode, encode, encode_simple

from app.storage import Stream, StreamEntry, storage


class CommandRegistry:
    def __init__(self):
        self.__registry = {}
        self.__transactions = {}

    def register(self, cls):
        if not issubclass(cls, RedisCommand):
            raise ValueError(f"{cls} does not subclass {RedisCommand}")

        cls_name = cls.__name__
        if cls_name in self:
            raise KeyError(f"Already registered <{cls_name}>")

        self.__registry[cls_name] = cls
        return cls

    async def execute(
        self, transaction_id: int, command: str, *args: list[str]
    ) -> list[bytes]:
        log("execute", command, args)
        response = await self.__execute(transaction_id, command, *args)
        match response:
            case [*payloads]:
                return payloads
            case bytes(payload):
                return [payload]

    async def __execute(
        self, transaction_id: int, command: str, *args: list[str]
    ) -> bytes:
        cmd = command.upper()
        if cmd in self:
            cmd_class = self[cmd]
            if cmd_class == MULTI:
                self.__transactions[transaction_id] = []

                return await cmd_class(*args).execute()
            elif cmd_class == EXEC:
                if transaction_id not in self.__transactions:
                    return encode(ValueError("EXEC without MULTI"))

                responses = [
                    decode(await cmd.execute())
                    for cmd in self.__transactions[transaction_id]
                ]
                del self.__transactions[transaction_id]

                return encode(responses)
            elif cmd_class == DISCARD:
                if transaction_id not in self.__transactions:
                    return encode(ValueError("DISCARD without MULTI"))

                del self.__transactions[transaction_id]

                return await cmd_class(*args).execute()
            elif transaction_id in self.__transactions:
                self.__transactions[transaction_id].append(cmd_class(*args))
                return encode_simple("QUEUED")
            else:
                return await cmd_class(*args).execute()
        raise Exception(f"Unknown command: {command}")

    def __getitem__(self, key):
        return self.__registry[key.upper()]

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
    async def execute(self) -> bytes | list[bytes]:
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
        await notify_waiting_list(self.key, len(values))
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
        await notify_waiting_list(self.key, len(values))
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
            callback = lambda: [self.key, storage.get_list(self.key).pop(0)]
            value = await join_waiting_list(self.key, self.timeout, callback)
            return encode(value)


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
    field_values: tuple[str]

    def __init__(self, *args: list[str]):
        match args:
            case [key, idx, *field_vals]:
                if len(field_vals) % 2 == 0:
                    self.key = key
                    self.idx = idx
                    self.field_values = tuple(field_vals)
                else:
                    raise ValueError
            case _:
                raise ValueError

    async def execute(self):
        stream = storage.get_stream(self.key)
        try:
            entry = stream.append(
                StreamEntry(idx=self.idx, field_values=self.field_values)
            )
            storage.set(self.key, stream)
            await notify_waiting_list(self.key, 1)
            return encode(entry.idx)
        except ValueError as error:
            log("encode(error)", encode(error))
            return encode(error)


@registry.register
@dataclass
class XRANGE(RedisCommand):
    """
    https://redis.io/docs/latest/commands/xrange/
    """

    key: str
    start: str
    end: str

    def __init__(self, *args):
        match args:
            case [key, start, end]:
                self.key = key
                self.start = start if start != "-" else "0-0"
                self.end = end if end != "+" else "9" * 20
            case _:
                raise ValueError

    async def execute(self):
        stream: Stream = storage.get_stream(self.key)
        entries = [
            [entry.idx, list(entry.field_values)]
            for entry in stream.entries
            if self.start <= entry.idx <= self.end
        ]
        return encode(entries)


@registry.register
@dataclass
class XREAD(RedisCommand):
    """
    https://redis.io/docs/latest/commands/xread/
    """

    STREAMS: ClassVar[str] = "STREAMS"
    BLOCK: ClassVar[str] = "BLOCK"

    kind: str
    queries: tuple[tuple[str, str]]
    timeout: float | None
    new_only: bool

    def __init__(self, *args):
        self.new_only = False

        match args:
            case [kind, *rest]:
                self.kind = kind.upper()
                match self.kind:
                    case self.BLOCK:
                        timeout, _streams, *rest = rest
                        self.timeout = (
                            float(timeout) / 1_000 if float(timeout) > 0 else None
                        )
                        self.new_only = rest[-1] == "$"

                match self.kind:
                    case self.BLOCK | self.STREAMS:
                        n = len(rest)
                        self.queries = tuple(
                            (rest[i], rest[i + n // 2]) for i in range(n // 2)
                        )
                    case _:
                        raise ValueError
            case _:
                raise ValueError

    async def execute(self):
        response = self.__query()
        match self.kind:
            case self.STREAMS:
                return encode(response)
            case self.BLOCK:
                if not self.new_only:
                    if list(key for key, values in response if len(values)):
                        return encode(response)

                ts = datetime.now() if self.new_only else datetime.fromtimestamp(0)
                callback = lambda: self.__query(ts)
                loop = asyncio.get_event_loop()
                tasks = set(
                    loop.create_task(join_waiting_list(key, self.timeout, callback))
                    for key, _ in self.queries
                )
                finished_tasks, _pending = await asyncio.wait(
                    tasks, timeout=self.timeout, return_when=asyncio.FIRST_COMPLETED
                )

                if len(finished_tasks) > 0:
                    task = list(finished_tasks)[0]
                    return encode(task.result())
                return encode(None)
            case _:
                raise ValueError

    def __query(self, ts: datetime = datetime.fromtimestamp(0)):
        return [
            [
                key,
                [
                    [entry.idx, list(entry.field_values)]
                    for entry in storage.get_stream(key).entries
                    if ts < entry.ts and start < entry.idx
                    # if start < entry.idx
                ],
            ]
            for key, start in self.queries
        ]


async def join_waiting_list(
    key: str, timeout: float | None, callback: Callable[[], any]
) -> any:
    loop = asyncio.get_event_loop()
    future = loop.create_future()
    waiting_queue[key].append(future)
    try:
        _ = await asyncio.wait_for(future, timeout)
        return callback()
    except TimeoutError:
        return None


async def notify_waiting_list(key: str, times: int) -> None:
    for _ in range(times):
        if len(waiting_queue[key]) > 0:
            waiting_queue[key].popleft().set_result(True)


@registry.register
@dataclass
class INCR(RedisCommand):
    """
    https://redis.io/docs/latest/commands/incr/
    """

    key: str

    def __init__(self, *args: list[str]):
        match args:
            case [key]:
                self.key = key
            case _:
                raise ValueError

    async def execute(self):
        value = storage.get(self.key)
        try:
            value = int(value) if value is not None else 0
            value += 1
            storage.set(self.key, str(value))
            return encode(value)
        except ValueError:
            return encode(ValueError("value is not an integer or out of range"))


@registry.register
@dataclass
class MULTI(RedisCommand):
    """
    https://redis.io/docs/latest/commands/multi/
    """

    def __init__(self, *args: list[str]):
        pass

    async def execute(self):
        return encode_simple("OK")


@registry.register
@dataclass
class EXEC(RedisCommand):
    """
    https://redis.io/docs/latest/commands/exec/
    """

    def __init__(self, *args: list[str]):
        pass

    async def execute(self):
        return encode_simple("OK")


@registry.register
@dataclass
class DISCARD(RedisCommand):
    """
    https://redis.io/docs/latest/commands/discard/
    """

    def __init__(self, *args: list[str]):
        pass

    async def execute(self):
        return encode_simple("OK")


@registry.register
@dataclass
class INFO(RedisCommand):
    """
    https://redis.io/docs/latest/commands/info/
    """

    info: str
    REPLICATION: ClassVar[str] = "replication"

    def __init__(self, *args):
        match args:
            case [self.REPLICATION]:
                self.info = self.REPLICATION
            case _:
                raise ValueError

    async def execute(self):
        match self.info:
            case self.REPLICATION:
                if not parse_args().is_master():
                    return encode(
                        "\n".join(
                            [
                                "role:slave",
                            ]
                        )
                    )
                return encode(
                    "\n".join(
                        [
                            "role:master",
                            "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
                            "master_repl_offset:0",
                        ]
                    )
                )
            case _:
                raise ValueError


@registry.register
@dataclass
class REPLCONF(RedisCommand):
    """
    https://redis.io/docs/latest/commands/replconf/
    """

    is_get_ack: bool = False

    def __init__(self, *args: list[str]):
        match [arg.upper() for arg in args]:
            case []:
                pass
            case [kind, *_]:
                self.is_get_ack = kind.upper() == "GETACK"

    async def execute(self):
        if self.is_get_ack:
            return encode("REPLCONF ACK 0".split())
        return encode_simple("OK")


@registry.register
@dataclass
class PSYNC(RedisCommand):
    """
    https://redis.io/docs/latest/commands/psync/
    """

    def __init__(self, *_args: list[str]):
        pass

    async def execute(self) -> list[bytes]:
        return [
            encode_simple("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"),
            encode(
                bytes.fromhex(
                    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
                )
            ),
        ]
