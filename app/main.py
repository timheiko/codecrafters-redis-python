import asyncio
from asyncio import StreamReader, StreamWriter
from dataclasses import dataclass

from app.command import registry

from app.resp import decode
from app.log import log


async def handle_echo(reader: StreamReader, writer: StreamWriter):
    while len(data := await reader.read(1024)) > 0:
        command, *args = decode(data)
        writer.write(await registry.execute(command, *args))

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
