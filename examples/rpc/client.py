from __future__ import annotations

import argparse
import itertools

from tcpconn.serializers import JsonCodec
from tcpconn.sync import TcpClient

_calls = [
    {"method": "add", "params": {"a": 2, "b": 3}},
    {"method": "echo", "params": {"message": "hi"}},
    {"method": "unknown", "params": {}},
]


def main() -> None:
    parser = argparse.ArgumentParser(description="tcpconn RPC client")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    ids = itertools.count(1)
    with TcpClient(args.host, args.port, codec=JsonCodec()) as client:
        for call in _calls:
            request_id = next(ids)
            envelope = {"id": request_id, **call}
            response = client.request(envelope, timeout=5.0)
            if response["id"] != request_id:
                raise RuntimeError("response id does not match request id")
            if "error" in response:
                print(f"call {request_id} error: {response['error']}", flush=True)
            else:
                print(f"call {request_id} result: {response['result']}", flush=True)


if __name__ == "__main__":
    main()
