from abc import ABC, abstractmethod
import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, ClassVar, Mapping, Self

from app.args import Args
from app.log import log
from app.resp import decode, encode, encode_simple

from app.storage import Stream, StreamEntry, storage


@dataclass
class Context:
    args: Args
    offset: int
    replicas: list[any]
    need_preplica_ack: bool

    def __init__(self, args: Args, offset: int = 0):
        self.args = args
        self.offset = offset
        self.replicas = []
        self.need_preplica_ack: bool = False


@dataclass
class Session:
    reader: asyncio.StreamReader | None = None
    writer: asyncio.StreamWriter | None = None
    channels: set[str] = field(default_factory=set)

    def subscribe(self, channel) -> bool:
        if channel not in self.channels:
            self.channels.add(channel)
            return True
        return False

    def unsubscribe(self, channel) -> bool:
        if channel in self.channels:
            self.channels.discard(channel)
            return True
        return False

    def subscriptions(self):
        return len(self.channels)


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
        self,
        transaction_id: int,
        context: Context,
        session: Session,
        command: str,
        *args: list[str],
    ) -> list[bytes]:
        log("execute", command, type(args), args)
        if session.subscriptions() > 0:
            if not self.is_allowed_in_subscription_mode(command):
                msg = f"Can't execute '{command.lower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
                return [encode(ValueError(msg))]

        response = await self.__execute(
            transaction_id, context, session, command, *args
        )
        match response:
            case [*payloads]:
                return payloads
            case bytes(payload):
                return [payload]

    async def __execute(
        self,
        transaction_id: int,
        context: Context,
        session: Session,
        command: str,
        *args: list[str],
    ) -> bytes:
        cmd = command.upper()
        if cmd in self:
            cmd_class = self[cmd]
            cmd_instance = cmd_class(*args).set_context(context).set_session(session)
            if cmd_class == MULTI:
                self.__transactions[transaction_id] = []

                return await cmd_instance.execute()
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

                return await cmd_instance.execute()
            elif transaction_id in self.__transactions:
                self.__transactions[transaction_id].append(cmd_class(*args))
                return encode_simple("QUEUED")
            else:
                return await cmd_instance.execute()
        raise Exception(f"Unknown command: {command}")

    def is_allowed_in_subscription_mode(self, cmd: str) -> bool:
        return self[cmd] in [
            SUBSCRIBE,
            UNSUBSCRIBE,
            PSUBSCRIBE,
            PUNSUBSCRIBE,
            PING,
            QUIT,
        ]

    def __getitem__(self, key):
        return self.__registry[key.upper()]

    def __contains__(self, key):
        return key.upper() in self.__registry

    def __iter__(self):
        return iter(self.__registry.keys())


registry = CommandRegistry()

waiting_queue: Mapping[str, asyncio.Future] = defaultdict(deque)


class RedisCommand(ABC):
    context: Context | None = None
    session: Session | None = None

    @abstractmethod
    def __init__(self, args: list[str]):
        """
        Instantiates a given command from arguments
        """

    def set_context(self, context: Context) -> Self:
        self.context = context
        return self

    def set_session(self, session: Session) -> Self:
        self.session = session
        return self

    def has_context(self) -> bool:
        return self.context is not None

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
        if self.context is None or self.context.args.is_master():
            if self.session is not None and self.session.subscriptions() > 0:
                return encode(["pong", ""])
            return encode_simple("PONG")
        return []


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
    args: list[str]

    def __init__(self, *args: list[str]):
        self.args = args

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

        if self.has_context():
            self.context.need_preplica_ack = len(self.context.replicas) > 0

            for _, writer in self.context.replicas:
                writer.write(encode(["SET", *self.args]))
                await writer.drain()

        if not self.has_context() or self.context.args.is_master():
            return encode_simple("OK")
        return b""


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
        while len(waiting_queue[key]) > 0:
            future = waiting_queue[key].popleft()
            if not future.cancelled() and not future.done():
                future.set_result(True)
                break


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
                if not self.context.args.is_master():
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
    port: int | None = None

    def __init__(self, *args: list[str]):
        match [arg.upper() for arg in args]:
            case []:
                pass
            case ["LISTENING-PORT", port]:
                self.port = int(port)
            case ["GETACK", *_]:
                self.is_get_ack = True

    async def execute(self):
        log(self.__class__, self.is_get_ack)
        if self.is_get_ack:
            if self.context is None:
                return encode("REPLCONF ACK 0".split())
            return encode(f"REPLCONF ACK {self.context.offset}".split())
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


@registry.register
@dataclass
class WAIT(RedisCommand):
    """
    https://redis.io/docs/latest/commands/wait/
    """

    min_replicas: int
    timeout: float

    def __init__(self, *args: list[str]):
        match args:
            case [min_replicas, timeout]:
                self.min_replicas = int(min_replicas)
                self.timeout = float(timeout) / 1_000 if float(timeout) != 0 else None

    async def execute(self):
        if self.has_context():
            if not self.context.need_preplica_ack:
                return encode(len(self.context.replicas))

            self.context.need_preplica_ack = False

            if len(self.context.replicas) == 0:
                return encode(0)

            async def wait_for_acknolegement(reader, writer):
                writer.write(encode("REPLCONF GETACK *".split()))
                await writer.drain()
                log("waiting for ack", id(reader))
                ack = await reader.read(1024)
                log("received ack", id(reader), ack)

            loop = asyncio.get_event_loop()
            tasks = set(
                loop.create_task(wait_for_acknolegement(r, w))
                for r, w in self.context.replicas
            )
            finished_tasks, _pending_tasks = await asyncio.wait(
                tasks,
                timeout=self.timeout,
                return_when=asyncio.ALL_COMPLETED,
            )
            for task in _pending_tasks:
                task.cancel()

            return encode(
                len([task for task in finished_tasks if not task.cancelled()])
            )
        return encode(0)


@registry.register
@dataclass
class CONFIG(RedisCommand):
    """
    https://redis.io/docs/latest/commands/config-get/
    """

    params: list[str]
    DIR: ClassVar[str] = "dir"
    DBFILENAME: ClassVar[str] = "dbfilename"

    def __init__(self, *args: list[str]):
        match [arg.lower() for arg in args]:
            case ["get", *params]:
                self.params = params
            case _:
                raise ValueError

    async def execute(self):
        configs = []

        if self.DIR in self.params:
            configs.extend([self.DIR, self.context.args.dir])
        if self.DBFILENAME in self.params:
            configs.extend([self.DBFILENAME, self.context.args.dbfilename])

        return encode(configs)


@registry.register
@dataclass
class KEYS(RedisCommand):
    """
    https://redis.io/docs/latest/commands/keys/
    """

    pattern: str

    def __init__(self, *args: list[str]):
        match args:
            case [pattern]:
                self.pattern = pattern
            case _:
                raise ValueError

    async def execute(self):
        return encode(storage.get_keys())


subscribers: dict[str, list[Session]] = defaultdict(list)


@registry.register
@dataclass
class SUBSCRIBE(RedisCommand):
    """
    https://redis.io/docs/latest/commands/subscribe/
    """

    channel: str

    def __init__(self, *args: list[str]):
        match args:
            case [channel]:
                self.channel = channel
            case _:
                raise ValueError

    async def execute(self):
        if self.session.subscribe(self.channel):
            subscribers[self.channel].append(self.session)

        return encode(["subscribe", self.channel, self.session.subscriptions()])


@registry.register
@dataclass
class UNSUBSCRIBE(RedisCommand):
    """
    https://redis.io/docs/latest/commands/unsubscribe/
    """

    channel: str

    def __init__(self, *args: list[str]):
        match args:
            case [channel]:
                self.channel = channel
            case _:
                raise ValueError

    async def execute(self):
        if self.session.unsubscribe(self.channel):
            subscribers[self.channel].remove(self.session)

        return encode(["unsubscribe", self.channel, self.session.subscriptions()])


@registry.register
@dataclass
class PSUBSCRIBE(RedisCommand):
    """
    https://redis.io/docs/latest/commands/psubscribe/
    """

    def __init__(self, *args: list[str]):
        pass

    async def execute(self):
        pass


@registry.register
@dataclass
class PUNSUBSCRIBE(RedisCommand):
    """
    https://redis.io/docs/latest/commands/punsubscribe/
    """

    def __init__(self, *args: list[str]):
        pass

    async def execute(self):
        pass


@registry.register
@dataclass
class QUIT(RedisCommand):
    """
    https://redis.io/docs/latest/commands/quit/
    """

    def __init__(self, *args: list[str]):
        pass

    async def execute(self):
        pass


@registry.register
@dataclass
class PUBLISH(RedisCommand):
    """
    https://redis.io/docs/latest/commands/publish/
    """

    channel: str
    message: str

    def __init__(self, *args):
        match args:
            case [channel, message]:
                self.channel = channel
                self.message = message
            case _:
                raise ValueError

    async def execute(self):
        count = 0
        for session in subscribers[self.channel]:
            try:
                session.writer.write(encode(["message", self.channel, self.message]))
                await session.writer.drain()
                count += 1
            except Exception as e:
                log(e)
        return encode(count)


@registry.register
@dataclass
class COMMAND(RedisCommand):
    """
    https://redis.io/docs/latest/commands/command/
    https://redis.io/docs/latest/commands/command-docs/
    https://redis.io/docs/latest/commands/command-count/
    """

    def __init__(self, *_args):
        pass

    async def execute(self):
        return encode([])


@registry.register
@dataclass
class ZADD(RedisCommand):
    """
    https://redis.io/docs/latest/commands/zadd/
    """

    set_name: str
    priority: float
    value: str

    def __init__(self, *args):
        match args:
            case [set_name, priority, value]:
                self.set_name = set_name
                self.priority = float(priority)
                self.value = value
            case _:
                raise ValueError

    async def execute(self):
        added = storage.add_to_sorted_set(self.set_name, self.priority, self.value)
        return encode(int(added))
