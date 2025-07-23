import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import ClassVar, Self

from .resp import decode, encode, encode_simple
from .storage import Storage
from .log import log


@dataclass
class Message:
    contents: list[str]
    SEPARATOR: ClassVar[str] = b"\r\n"

    @staticmethod
    def parse(payload: bytes) -> Self:
        return Message(contents=decode(payload))


storage = Storage()
waiting_queue = defaultdict(deque)


async def handle_echo(reader, writer):
    while len(data := await reader.read(1024)) > 0:
        message = Message.parse(data)
        command = message.contents[0].upper()
        if command == "PING":
            writer.write(encode_simple("PONG"))
        elif command == "ECHO":
            writer.write(encode_simple(" ".join(message.contents[1:])))
        elif command == "SET":
            key, value, *rest = message.contents[1:]
            if len(rest) > 0:
                _exp_command, duration = rest[0].upper(), int(rest[1])
                storage.set(key, value, duration)
            else:
                storage.set(key, value)
            writer.write(encode_simple("OK"))
        elif command == "GET":
            key, *_ = message.contents[1:]
            writer.write(encode(storage.get(key)))
        elif command == "RPUSH":
            key, *items = message.contents[1:]
            values = storage.get_list(key)
            values.extend(items)
            storage.set(key, values)
            writer.write(encode(len(values)))
            for _ in items:
                if len(waiting_queue[key]) > 0:
                    waiting_queue[key].popleft().set_result(True)
        elif command == "LPUSH":
            key, *items = message.contents[1:]
            values = storage.get_list(key)
            values = items[::-1] + values
            storage.set(key, values)
            writer.write(encode(len(values)))
        elif command == "LRANGE":
            key, start, end = message.contents[1:]
            start, end = int(start), int(end)
            values = storage.get_list(key)
            writer.write(encode(storage.get_list_range(key, start, end)))
        elif command == "LLEN":
            values = storage.get_list(message.contents[1])
            writer.write(encode(len(values)))
        elif command == "LPOP":
            values = storage.get_list(message.contents[1])
            number = 1 if len(message.contents) < 3 else int(message.contents[2])
            if not values:
                writer.write(encode(None))
            elif number == 1:
                writer.write(encode(values.pop(0)))
            else:
                popped = values[:number]
                log("popped", popped)
                values[:number] = []
                writer.write(encode(popped))
        elif command == "BLPOP":
            key, timeout = message.contents[1], float(message.contents[2])
            values = storage.get_list(key)
            if values:
                writer.write(encode([key, values.pop(0)]))
            else:
                loop = asyncio.get_event_loop()
                future = loop.create_future()
                waiting_queue[key].append(future)
                try:
                    _ = await asyncio.wait_for(future, timeout if timeout > 0 else None)
                    writer.write(encode([key, storage.get_list(key).pop(0)]))
                except TimeoutError:
                    writer.write(encode(None))
        else:
            raise Exception(f"Unknown command: {data}")

    await writer.drain()
    writer.close()
    await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handle_echo, "localhost", 6379)

    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"Serving on {addrs}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
