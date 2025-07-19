from dataclasses import dataclass
import socket  # noqa: F401
import sys
import threading
from typing import ClassVar, Self


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
            if payload[i:i + 1] == b"*":
                new_line_sep_pos = payload.find(Message.SEPARATOR, i + 1)
                length = int(payload[i + 1:new_line_sep_pos].decode())
                i = new_line_sep_pos + len(Message.SEPARATOR)
                for _ in range(length):
                    if payload[i:i + 1] == b"$":
                        text, i = Message.parse_text(payload, i)
                        contents.append(text)
            else:
                raise Exception(f"Unknown data type: {chr(payload[i])}")

        return Message(contents=contents)
    

    @staticmethod
    def parse_text(payload: bytes, offset: int) -> tuple[bytes, int]:
        if payload[offset:offset + 1] == "$".encode():
            new_line_sep_pos = payload.find(Message.SEPARATOR, offset + 1)
            length = int(payload[offset + 1:new_line_sep_pos])
            text_start = new_line_sep_pos + len(Message.SEPARATOR)
            text_end = text_start + length
            text = payload[text_start:text_end].decode()
            return (text, text_end + len(Message.SEPARATOR))
        
        raise Exception(f"Cannot parse text from payload: {payload}")

    def encode(self, contents: list[any] = []) -> bytes:
        contents = contents or self.contents[1:]
        payload = b""
        if len(contents) == 0:
            payload = b"*0" + Message.SEPARATOR
        else:
            payload = b"+" + " ".join(contents).encode() + Message.SEPARATOR
        log("payload >>> ", payload)
        
        return payload


    def encode_bulk(self, contents: list[any] = []) -> bytes:
        contents = contents or self.contents[1:]
        payload = b""
        if len(contents) == 0:
            payload = b"$-1" + Message.SEPARATOR
        else:
            val = contents[0]
            if val is None:
                payload = b"$-1\r\n"
            else:
                payload = Message.SEPARATOR.join([
                    f"${len(contents[0])}".encode(),
                    contents[0].encode(),
                    b"",
                ])
        log("payload >>> ", payload)
        
        return payload



storage = {}


def handle_connection(connection, address):
    log("address ", address)
    with connection:
        while len(data := connection.recv(1024)) > 0:
            message = Message.parse(data)
            command = message.contents[0].upper()
            if command == "PING":
                connection.sendall(message.encode(["PONG"]))
            elif command == "ECHO":
                connection.sendall(message.encode())
            elif command == "SET":
                key, value, *_ = message.contents[1:]
                print("key", key, "value", value)
                storage[key] = value
                connection.sendall(message.encode(["OK"]))
            elif command == "GET":
                key, *_ = message.contents[1:]
                connection.sendall(message.encode_bulk([storage.get(key)]))
            else:
                raise Exception(f"Unknown command: {data}")


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        connection, address = server_socket.accept()
        threading.Thread(target=handle_connection, args=(connection, address)).start()


if __name__ == "__main__":
    main()
