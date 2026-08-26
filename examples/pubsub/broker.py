from __future__ import annotations

import argparse
import signal
import sys
import threading
from types import FrameType

from tcpconn.serializers import JsonCodec
from tcpconn.sync import ServerConnection, TcpServer


class Broker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._topics: dict[str, set[ServerConnection]] = {}

    def handle(self, conn: ServerConnection) -> None:
        subscribed: set[str] = set()
        try:
            for message in conn:
                action = message.get("action")
                if action == "subscribe":
                    topic = message["topic"]
                    with self._lock:
                        self._topics.setdefault(topic, set()).add(conn)
                    subscribed.add(topic)
                    conn.send({"type": "ack", "topic": topic})
                elif action == "publish":
                    topic = message["topic"]
                    with self._lock:
                        subscribers = set(self._topics.get(topic, ()))
                    for subscriber in subscribers:
                        try:
                            subscriber.send(
                                {"type": "event", "topic": topic, "message": message["message"]}
                            )
                        except OSError:
                            continue
        finally:
            with self._lock:
                for topic in subscribed:
                    self._topics.get(topic, set()).discard(conn)


def main() -> None:
    parser = argparse.ArgumentParser(description="tcpconn pubsub broker")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()

    broker = Broker()
    server = TcpServer(args.host, args.port, handler=broker.handle, codec=JsonCodec())
    server.start()
    print(f"listening on {server.address[0]}:{server.address[1]}", flush=True)

    def _on_sigint(signum: int, frame: FrameType | None) -> None:
        server.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _on_sigint)
    server.serve_forever()


if __name__ == "__main__":
    main()
