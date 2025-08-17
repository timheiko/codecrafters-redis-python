import asyncio
from asyncio import StreamReader, StreamWriter
import os

from app.args import parse_args
from app.command import PSYNC, Context, Session, registry

from app.resp import decode, decode_commands, encode
from app.log import log
from signal import SIGINT, SIGTERM

from app.storage import Storage


context = Context(args=parse_args(), storage=Storage())


async def execute_command(
    session: Session,
    command: list[str] | str,
    *,
    offset_delta: int = 0,
) -> None:
    log("command", command, id(session.reader))
    match command:
        case [cmd, *args] if cmd in registry:
            payloads = await registry.execute(
                id(session.reader), context, session, cmd, *args
            )
            payload = b"".join(payloads)
            log("payload >>>", payload)
            await session.write(payload)

            if registry[cmd] == PSYNC:
                context.replicas.append((session.reader, session.writer))
                await asyncio.sleep(3_000)

            context.offset += offset_delta
        case _:
            log("Unknown command", command)


async def handle_connection(reader: StreamReader, writer: StreamWriter) -> None:
    session = Session(reader, writer)
    while len(data := await reader.read(1024)) > 0:
        commands = decode_commands(data)
        log("commands", commands)
        for command, offset_delta in commands:
            log("execute_command", command, offset_delta)
            await execute_command(session, command, offset_delta=offset_delta)


async def handle_commands(reader: StreamReader, writer: StreamWriter) -> None:
    await handle_connection(reader, writer)

    if (reader, writer) not in context.replicas:
        writer.close()
        await writer.wait_closed()


async def handshake():
    if not context.args.is_master():
        host, port = context.args.replicaof.split()
        log("handshake started", host, port)
        reader, writer = await asyncio.open_connection(host, port)

        handshake_commands = [
            "PING",
            f"REPLCONF listening-port {context.args.port}",
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
                        await execute_command(
                            Session(reader, writer),
                            response,
                            offset_delta=len(encode(response)),
                        )

        log("handshake finished")
        await handle_connection(reader, writer)


async def main():
    await add_signal_handlers()

    server = await asyncio.start_server(
        handle_connection, "localhost", context.args.port
    )

    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    log(f"Serving on {addrs}")

    await handshake()
    context.storage.load_from_rdb_dump(context.args.dir, context.args.dbfilename)

    try:
        async with server:
            await server.serve_forever()
    except asyncio.CancelledError:
        log("Shutting down")


async def add_signal_handlers() -> None:
    log(f"Running process id: {os.getpid()}")

    loop = asyncio.get_running_loop()

    def signal_handler(sig: int) -> None:
        for task in asyncio.all_tasks(loop=loop):
            task.cancel()
        log(f"Got signal: {sig!s}, shutting down.")
        loop.remove_signal_handler(SIGTERM)
        loop.add_signal_handler(SIGINT, lambda: None)

    for sig in (SIGTERM, SIGINT):
        loop.add_signal_handler(sig, signal_handler, sig)


if __name__ == "__main__":
    asyncio.run(main())
