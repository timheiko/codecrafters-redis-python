import asyncio
from asyncio import StreamReader, StreamWriter

from app.args import Args, parse_args
from app.command import registry

from app.resp import decode, encode
from app.log import log


async def handle_echo(reader: StreamReader, writer: StreamWriter):
    while len(data := await reader.read(1024)) > 0:
        command, *args = decode(data)
        log(f"command {command} args: {args}")
        for payload in await registry.execute(id(writer), command, *args):
            writer.write(payload)
            await writer.drain()

    writer.close()
    await writer.wait_closed()


async def handshake(args: Args):
    if not args.is_master():
        host, port = args.replicaof.split()
        log("handshake started", host, port)
        reader, writer = await asyncio.open_connection(host, port)

        handshake_commands = [
            "PING",
            f"REPLCONF listening-port {args.port}",
            "REPLCONF capa psync2",
            "PSYNC ? -1",
        ]
        for cmd in handshake_commands:
            cmd_items = cmd.split()
            log("handshake stage request:", cmd_items)
            writer.write(encode(cmd_items))
            await writer.drain()
            response = decode(await reader.read(1024))
            log("handshake stage response:", response)

        writer.close()
        await writer.wait_closed()
        log("handshake finished")


async def main():
    args = parse_args()
    server = await asyncio.start_server(handle_echo, "localhost", args.port)

    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    log(f"Serving on {addrs}")

    await handshake(args)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
