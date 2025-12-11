from contextlib import closing
import socket
import sys

from framework.util import BASE_HOST


def can_bind(port, host=BASE_HOST):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
        except socket.error:
            print(f"Cant bind to {port}. Skipping", file=sys.stderr)
            return False
        return True


class PortDistributor:
    def __init__(self, start: int, end: int) -> None:
        self.gen = iter(range(start, end))

    def get(self) -> int:
        for port in self.gen:
            if can_bind(port):
                return port

        # NB: if it's deemed not to be enough we can itertools.cycle the iterator
        raise Exception("No more free ports left, per worker range exhausted")
