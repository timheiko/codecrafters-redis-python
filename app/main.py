import asyncio
from asyncio import StreamReader, StreamWriter

from app.args import Args, parse_args
from app.command import GET, INFO, PSYNC, SET, Context, registry

from app.resp import decode, decode_commands, encode
from app.log import log


config: Args = parse_args()
replicas = []


context = Context(is_master=config.is_master())


async def execute_command(
    reader: StreamReader,
    writer: StreamWriter,
    command: list[str] | str,
    *,
    offset_delta: int = 0,
):
    log("command", command)
    match command:
        case [cmd, *args]:
            log(f"cmd {cmd} args: {args}")
            payloads = await registry.execute(id(writer), context, cmd, *args)
            if registry[cmd] != SET or config.is_master():
                payload = b"".join(payloads)
                log("payload >>>", payload)
                writer.write(payload)
                await writer.drain()

            if registry[cmd] == PSYNC:
                replicas.append((reader, writer))

            if registry[cmd] == SET:
                for _, w in replicas:
                    w.write(encode(command))
                    await w.drain()

            context.offset += offset_delta
        case _:
            pass


async def handle_connection(reader: StreamReader, writer: StreamWriter):
    while len(data := await reader.read(1024)) > 0:
        commands = decode_commands(data)
        log("commands", commands)
        for command, offset_delta in commands:
            await execute_command(reader, writer, command, offset_delta=offset_delta)


async def handle_commands(reader: StreamReader, writer: StreamWriter):
    await handle_connection(reader, writer)

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
            responses = decode(await reader.read(1024))
            log("handshake stage response:", responses)
            for response in responses:
                match response:
                    case [_, *_]:
                        await execute_command(reader, writer, response)

        log("handshake finished")
        await handle_connection(reader, writer)


async def main():
    server = await asyncio.start_server(handle_connection, "localhost", config.port)

    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    log(f"Serving on {addrs}")

    await handshake()

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
