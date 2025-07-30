import asyncio
from asyncio import StreamReader, StreamWriter

from app.storage import storage
from app.args import parse_args
from app.command import PSYNC, Context, Session, registry

from app.resp import decode, decode_commands, encode
from app.log import log


context = Context(parse_args())


async def execute_command(
    session: Session,
    command: list[str] | str,
    *,
    offset_delta: int = 0,
):
    log("command", command, id(session.reader))
    match command:
        case [cmd, *args]:
            if session.subscriptions() > 0:
                if not registry.is_allowed_in_subscription_mode(cmd):
                    session.writer.write(
                        encode(
                            ValueError(
                                f"Can't execute '{cmd.lower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
                            )
                        )
                    )
                    await session.writer.drain()

                    return

            payloads = await registry.execute(
                id(session.reader), context, session, cmd, *args
            )
            payload = b"".join(payloads)
            log("payload >>>", payload)
            session.writer.write(payload)
            await session.writer.drain()

            if registry[cmd] == PSYNC:
                context.replicas.append((session.reader, session.writer))
                await asyncio.sleep(3_000)

            context.offset += offset_delta
        case _:
            pass


async def handle_connection(reader: StreamReader, writer: StreamWriter):
    session = Session(reader, writer)
    while len(data := await reader.read(1024)) > 0:
        commands = decode_commands(data)
        log("commands", commands)
        for command, offset_delta in commands:
            await execute_command(session, command, offset_delta=offset_delta)


async def handle_commands(reader: StreamReader, writer: StreamWriter):
    await handle_connection(reader, writer)

    # if (reader, writer) not in context.replicas:
    #     writer.close()
    #     await writer.wait_closed()


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
    server = await asyncio.start_server(
        handle_connection, "localhost", context.args.port
    )

    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    log(f"Serving on {addrs}")

    await handshake()
    storage.load_from_rdb_dump(context.args.dir, context.args.dbfilename)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
