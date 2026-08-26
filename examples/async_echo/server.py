from __future__ import annotations

import argparse
import asyncio
import signal

from tcpconn.aio import AsyncServerConnection, AsyncTcpServer


async def handle(conn: AsyncServerConnection) -> None:
    async for message in conn:
        await conn.send(message)


async def main() -> None:
    parser = argparse.ArgumentParser(description="tcpconn async echo server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()

    server = AsyncTcpServer(args.host, args.port, handler=handle)
    await server.start()
    print(f"listening on {server.address[0]}:{server.address[1]}", flush=True)

    loop = asyncio.get_running_loop()
    stop = loop.create_future()

    def _on_signal() -> None:
        if not stop.done():
            stop.set_result(None)

    try:
        loop.add_signal_handler(signal.SIGINT, _on_signal)
    except NotImplementedError:
        signal.signal(signal.SIGINT, lambda *_: _on_signal())

    await stop
    await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
