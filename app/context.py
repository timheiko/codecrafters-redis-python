from dataclasses import dataclass, field
from typing import Any

from app.args import Args

from app.storage import Storage


@dataclass
class Context:
    args: Args
    offset: int = 0
    replicas: list[Any] = field(default_factory=list)
    need_preplica_ack: bool = False
    storage: Storage = field(default_factory=Storage)
