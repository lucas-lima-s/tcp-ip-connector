from __future__ import annotations

import argparse
import signal
import sys
from types import FrameType

from tcpconn.sync import ServerConnection, TcpServer


def handle(conn: ServerConnection) -> None:
    for message in conn:
        conn.send(message)


def main() -> None:
    parser = argparse.ArgumentParser(description="tcpconn echo server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()

    server = TcpServer(args.host, args.port, handler=handle)
    server.start()
    print(f"listening on {server.address[0]}:{server.address[1]}", flush=True)

    def _on_sigint(signum: int, frame: FrameType | None) -> None:
        server.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _on_sigint)
    server.serve_forever()


if __name__ == "__main__":
    main()
