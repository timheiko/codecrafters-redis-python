import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
import queue
import socket  # noqa: F401
import sys
import threading
from typing import ClassVar, Self
from datetime import datetime, timedelta

from app.resp import encode, encode_simple
from app.storage import Storage


def log(*args: list[any]) -> None:
    print(*args, file=sys.stderr)


@dataclass
class Message:
    contents: list[str]
    SEPARATOR: ClassVar[str] = b"\r\n"

    @staticmethod
    def parse(payload: bytes) -> Self:
        log("payload <<< ", payload)
        n, i = len(payload), 0
        contents = []
        while i < n:
            if payload[i : i + 1] == b"*":
                new_line_sep_pos = payload.find(Message.SEPARATOR, i + 1)
                length = int(payload[i + 1 : new_line_sep_pos].decode())
                i = new_line_sep_pos + len(Message.SEPARATOR)
                for _ in range(length):
                    if payload[i : i + 1] == b"$":
                        text, i = Message.parse_text(payload, i)
                        contents.append(text)
            else:
                raise Exception(f"Unknown data type: {chr(payload[i])}")

        return Message(contents=contents)

    @staticmethod
    def parse_text(payload: bytes, offset: int) -> tuple[bytes, int]:
        if payload[offset : offset + 1] == "$".encode():
            new_line_sep_pos = payload.find(Message.SEPARATOR, offset + 1)
            length = int(payload[offset + 1 : new_line_sep_pos])
            text_start = new_line_sep_pos + len(Message.SEPARATOR)
            text_end = text_start + length
            text = payload[text_start:text_end].decode()
            return (text, text_end + len(Message.SEPARATOR))

        raise Exception(f"Cannot parse text from payload: {payload}")


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
            key = message.contents[1]
            values = storage.get_list(key)
            if values:
                writer.write(encode([key, values.pop(0)]))
            else:
                loop = asyncio.get_event_loop()
                future = loop.create_future()
                waiting_queue[key].append(future)
                _ = await asyncio.wait_for(future, None)
                writer.write(encode([key, storage.get_list(key).pop(0)]))
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
