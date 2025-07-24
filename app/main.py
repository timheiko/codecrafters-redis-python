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
            writer.write(await cmd.execute())
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
