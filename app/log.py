import sys


def log(*args: list[any]) -> None:
    print(*args, file=sys.stderr)
