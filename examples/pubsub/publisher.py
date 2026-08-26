from __future__ import annotations

import argparse

from tcpconn.serializers import JsonCodec
from tcpconn.sync import TcpClient


def main() -> None:
    parser = argparse.ArgumentParser(description="tcpconn pubsub publisher")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--message", required=True)
    args = parser.parse_args()

    with TcpClient(args.host, args.port, codec=JsonCodec()) as client:
        client.send({"action": "publish", "topic": args.topic, "message": args.message})
        print(f"published to {args.topic!r}: {args.message!r}", flush=True)


if __name__ == "__main__":
    main()
