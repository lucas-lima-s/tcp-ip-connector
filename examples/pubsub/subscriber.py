from __future__ import annotations

import argparse

from tcpconn.serializers import JsonCodec
from tcpconn.sync import TcpClient


def main() -> None:
    parser = argparse.ArgumentParser(description="tcpconn pubsub subscriber")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--count", type=int, default=1)
    args = parser.parse_args()

    with TcpClient(args.host, args.port, codec=JsonCodec()) as client:
        client.send({"action": "subscribe", "topic": args.topic})
        ack = client.receive(timeout=10.0)
        print(f"subscribed to {ack['topic']}", flush=True)
        received = 0
        while received < args.count:
            event = client.receive(timeout=10.0)
            if event.get("type") != "event":
                continue
            print(f"[{args.topic}] received: {event['message']!r}", flush=True)
            received += 1


if __name__ == "__main__":
    main()
