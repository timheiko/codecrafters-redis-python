import socket  # noqa: F401
import sys
import threading


def log(*args: list[any]) -> None:
    print(*args, file=sys.stderr)


def handle_connection(connection, address):
    log("address ", address)
    with connection:
        while len(data := connection.recv(1024).decode()) > 0:
            if "PING" in data.upper():
                connection.sendall(b"+PONG\r\n")


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        connection, address = server_socket.accept()
        threading.Thread(target=handle_connection, args=(connection, address)).start()


if __name__ == "__main__":
    main()
