from __future__ import annotations

import argparse
import asyncio

from tcpconn.aio import AsyncTcpClient


async def main() -> None:
    parser = argparse.ArgumentParser(description="tcpconn async echo client")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    async with AsyncTcpClient(args.host, args.port) as client:
        for line in (b"hello", b"world", b"goodbye"):
            await client.send(line)
            reply = await client.receive(timeout=5.0)
            print(f"echoed: {reply!r}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
