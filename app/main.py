import asyncio
from asyncio import StreamReader, StreamWriter

from app.args import Args, parse_args
from app.command import GET, INFO, PSYNC, SET, registry

from app.resp import decode, encode
from app.log import log


config: Args = parse_args()
replicas = []


async def execute_command(reader: StreamReader, writer: StreamWriter):
    while len(data := await reader.read(1024)) > 0:
        for command in decode(data):
            cmd, *args = command
            log(f"command {cmd} args: {args}")
            payloads = await registry.execute(id(writer), cmd, *args)
            if registry[cmd] != SET or config.is_master():
                writer.write(b"".join(payloads))
                await writer.drain()

            if registry[cmd] == PSYNC:
                replicas.append((reader, writer))

            if registry[cmd] == SET:
                for _, w in replicas:
                    w.write(data)
                    await w.drain()


async def handle_commands(reader: StreamReader, writer: StreamWriter):
    await execute_command(reader, writer)

    if (reader, writer) not in replicas:
        writer.close()
        await writer.wait_closed()


async def handshake():
    if not config.is_master():
        host, port = config.replicaof.split()
        log("handshake started", host, port)
        reader, writer = await asyncio.open_connection(host, port)

        handshake_commands = [
            "PING",
            f"REPLCONF listening-port {config.port}",
            "REPLCONF capa psync2",
            "PSYNC ? -1",
        ]
        for cmd in handshake_commands:
            cmd_items = cmd.split()
            log("handshake stage request:", cmd_items)
            writer.write(encode(cmd_items))
            await writer.drain()
            try:
                response = decode(await reader.read(1024))
                log("handshake stage response:", response)
            except UnicodeDecodeError as e:
                log("fix me!", e)

        # rdb = await reader.read(1024)
        # log("rdb", rdb)

        log("handshake finished")
        await execute_command(reader, writer)


async def main():
    server = await asyncio.start_server(handle_commands, "localhost", config.port)

    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    log(f"Serving on {addrs}")

    await handshake()

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
