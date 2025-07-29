import argparse
from dataclasses import dataclass


@dataclass
class Args:
    port: int = 6379
    replicaof: str | None = None
    dir: str | None = None
    dbfilename: str | None = None

    def is_master(self) -> bool:
        return self.replicaof is None


def parse_args() -> Args:
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--port", type=int, required=False)
    argparser.add_argument("--replicaof", type=str, required=False)
    argparser.add_argument("--dir", type=str, required=False)
    argparser.add_argument("--dbfilename", type=str, required=False)
    namespace = Args()
    return argparser.parse_args(namespace=namespace)
