import asyncio
from asyncio import StreamReader, StreamWriter
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import ClassVar, Self

from app.command import registry, waiting_queue, ECHO, GET, LLEN, LPOP, PING, SET

from app.resp import decode, encode, encode_simple
from app.storage import storage
from app.log import log


@dataclass
class Message:
    contents: list[str]
    SEPARATOR: ClassVar[str] = b"\r\n"

    @staticmethod
    def parse(payload: bytes) -> Self:
        return Message(contents=decode(payload))


async def handle_echo(reader: StreamReader, writer: StreamWriter):
    while len(data := await reader.read(1024)) > 0:
        message = Message.parse(data)
        command = message.contents[0].upper()
        args = message.contents[1:]
        if command in registry:
            cmd = registry[command](*args)
            writer.write(cmd.execute())
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
    log(f"Serving on {addrs}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
