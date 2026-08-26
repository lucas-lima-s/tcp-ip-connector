from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

from tcpconn._socketio import recv_some
from tcpconn.errors import ConnectionLost, FrameError, ReconnectFailed, SerializationError
from tcpconn.protocol import MessageKind, unwrap_kind

if TYPE_CHECKING:
    from tcpconn.sync.client import TcpClient


class ReaderThread(threading.Thread):
    def __init__(self, client: TcpClient) -> None:
        super().__init__(daemon=True, name="tcpconn-reader")
        self._client = client

    def run(self) -> None:
        client = self._client
        while not client._stop_event.is_set():
            try:
                self._read_until_broken()
                last_error: BaseException = ConnectionLost("socket was closed")
            except (ConnectionLost, FrameError, SerializationError) as exc:
                last_error = exc
            self._mark_disconnected()
            if client._stop_event.is_set():
                self._set_failure(last_error)
                return
            if client._reconnect_policy is None:
                self._set_failure(last_error)
                return
            if not self._reconnect():
                return

    def _read_until_broken(self) -> None:
        client = self._client
        while True:
            sock = client._sock
            if sock is None:
                raise ConnectionLost("socket was closed")
            chunk = recv_some(sock)
            for frame in client._decoder.feed(chunk):
                self._handle_frame(frame)

    def _handle_frame(self, frame: bytes) -> None:
        client = self._client
        client._last_liveness = time.monotonic()
        if client._control_channel:
            kind, payload = unwrap_kind(frame)
        else:
            kind, payload = MessageKind.DATA, frame
        if kind is MessageKind.PING:
            client._send_kind(MessageKind.PONG)
            return
        if kind is MessageKind.PONG:
            return
        message = client._codec.loads(payload)
        client._inbox.put(message)

    def _set_failure(self, exc: BaseException) -> None:
        client = self._client
        with client._state_lock:
            client._failure = exc
            client._connected = False

    def _mark_disconnected(self) -> None:
        client = self._client
        with client._state_lock:
            client._connected = False

    def _reconnect(self) -> bool:
        client = self._client
        policy = client._reconnect_policy
        if policy is None:
            return False
        for attempt, delay in enumerate(policy.backoff.delays(), start=1):
            if client._stop_event.wait(delay):
                return False
            try:
                client._do_connect()
            except OSError:
                continue
            else:
                if policy.on_reconnect is not None:
                    policy.on_reconnect(attempt)
                return True
        self._set_failure(ReconnectFailed(f"failed to reconnect to {client.host}:{client.port}"))
        return False
