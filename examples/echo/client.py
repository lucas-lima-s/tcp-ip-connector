from __future__ import annotations

import argparse

from tcpconn.sync import TcpClient


def main() -> None:
    parser = argparse.ArgumentParser(description="tcpconn echo client")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    with TcpClient(args.host, args.port) as client:
        for line in (b"hello", b"world", b"goodbye"):
            client.send(line)
            reply = client.receive(timeout=5.0)
            print(f"echoed: {reply!r}", flush=True)


if __name__ == "__main__":
    main()
