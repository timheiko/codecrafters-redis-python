from dataclasses import dataclass
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


def handle_connection(connection, address):
    log("address ", address)
    with connection:
        while len(data := connection.recv(1024)) > 0:
            message = Message.parse(data)
            command = message.contents[0].upper()
            if command == "PING":
                connection.sendall(encode_simple("PONG"))
            elif command == "ECHO":
                connection.sendall(encode_simple(" ".join(message.contents[1:])))
            elif command == "SET":
                key, value, *rest = message.contents[1:]
                if len(rest) > 0:
                    _exp_command, duration = rest[0].upper(), int(rest[1])
                    storage.set(key, value, duration)
                else:
                    storage.set(key, value)
                connection.sendall(encode_simple("OK"))
            elif command == "GET":
                key, *_ = message.contents[1:]
                connection.sendall(encode(storage.get(key)))
            elif command == "RPUSH":
                key, *items = message.contents[1:]
                values = storage.get(key) or []
                values.extend(items)
                storage.set(key, values)
                connection.sendall(encode(len(values)))
            elif command == "LRANGE":
                key, start, end = message.contents[1:]
                start, end = int(start), int(end)
                values = storage.get(key)
                if values is None:
                    connection.sendall(encode([]))
                else:
                    connection.sendall(encode(storage.get_list_range(key, start, end)))
            else:
                raise Exception(f"Unknown command: {data}")


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        connection, address = server_socket.accept()
        threading.Thread(target=handle_connection, args=(connection, address)).start()


if __name__ == "__main__":
    main()
