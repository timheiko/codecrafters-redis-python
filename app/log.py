import sys
from typing import Any


def log(*args: Any) -> None:
    print(*args, file=sys.stderr)
