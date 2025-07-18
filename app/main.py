import socket  # noqa: F401
import sys


def log(*args: list[any]) -> None:
    print(*args, file=sys.stderr)


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        connection, address = server_socket.accept()
        log("address ", address)
        
        connection.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
