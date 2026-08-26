from __future__ import annotations

import argparse
import signal
import sys
from types import FrameType
from typing import Any

from tcpconn.serializers import JsonCodec
from tcpconn.sync import ServerConnection, TcpServer

_METHODS = {
    "add": lambda params: params["a"] + params["b"],
    "echo": lambda params: params,
}


def _dispatch(request: dict[str, Any]) -> dict[str, Any]:
    request_id = request.get("id")
    method = request.get("method")
    params = request.get("params", {})
    handler = _METHODS.get(method) if isinstance(method, str) else None
    if handler is None:
        return {"id": request_id, "error": {"message": f"unknown method: {method}"}}
    try:
        result = handler(params)
    except (KeyError, TypeError) as exc:
        return {"id": request_id, "error": {"message": str(exc)}}
    return {"id": request_id, "result": result}


def handle(conn: ServerConnection) -> None:
    for request in conn:
        conn.send(_dispatch(request))


def main() -> None:
    parser = argparse.ArgumentParser(description="tcpconn RPC server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()

    server = TcpServer(args.host, args.port, handler=handle, codec=JsonCodec())
    server.start()
    print(f"listening on {server.address[0]}:{server.address[1]}", flush=True)

    def _on_sigint(signum: int, frame: FrameType | None) -> None:
        server.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _on_sigint)
    server.serve_forever()


if __name__ == "__main__":
    main()
