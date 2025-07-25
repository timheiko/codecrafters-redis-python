import argparse
import asyncio
from asyncio import StreamReader, StreamWriter

from app.command import registry

from app.resp import decode
from app.log import log


async def handle_echo(reader: StreamReader, writer: StreamWriter):
    while len(data := await reader.read(1024)) > 0:
        command, *args = decode(data)
        log(f"command {command} args: {args}")
        writer.write(await registry.execute(id(writer), command, *args))
        await writer.drain()

    writer.close()
    await writer.wait_closed()


async def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--port", type=int, default=6379)
    parsed = argparser.parse_args()

    server = await asyncio.start_server(handle_echo, "localhost", parsed.port)

    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    log(f"Serving on {addrs}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
