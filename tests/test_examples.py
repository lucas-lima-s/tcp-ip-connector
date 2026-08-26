from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LISTENING_RE = re.compile(r"^listening on (127\.0\.0\.1):(\d+)$")


def _start_server(script: str) -> tuple[subprocess.Popen[str], str, int]:
    args = [sys.executable, str(REPO_ROOT / script), "--port", "0"]
    proc = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=REPO_ROOT,
    )
    assert proc.stdout is not None
    first_line = proc.stdout.readline().strip()
    match = LISTENING_RE.match(first_line)
    if match is None:
        proc.kill()
        raise AssertionError(f"server did not print a listening line, got: {first_line!r}")
    return proc, match.group(1), int(match.group(2))


def _stop(proc: subprocess.Popen[str]) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5.0)
    if proc.stdout is not None:
        proc.stdout.close()


def _run_client(script: str, host: str, port: int, *extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(REPO_ROOT / script), "--host", host, "--port", str(port), *extra],
        capture_output=True,
        text=True,
        timeout=15.0,
        cwd=REPO_ROOT,
        check=False,
    )


def test_echo_example() -> None:
    server, host, port = _start_server("examples/echo/server.py")
    try:
        result = _run_client("examples/echo/client.py", host, port)
        assert result.returncode == 0, result.stdout + result.stderr
        assert "echoed: b'hello'" in result.stdout
        assert "echoed: b'world'" in result.stdout
        assert "echoed: b'goodbye'" in result.stdout
    finally:
        _stop(server)


def test_rpc_example() -> None:
    server, host, port = _start_server("examples/rpc/server.py")
    try:
        result = _run_client("examples/rpc/client.py", host, port)
        assert result.returncode == 0, result.stdout + result.stderr
        assert "call 1 result: 5" in result.stdout
        assert "call 2 result:" in result.stdout
        assert "call 3 error:" in result.stdout
    finally:
        _stop(server)


def test_async_echo_example() -> None:
    server, host, port = _start_server("examples/async_echo/server.py")
    try:
        result = _run_client("examples/async_echo/client.py", host, port)
        assert result.returncode == 0, result.stdout + result.stderr
        assert "echoed: b'hello'" in result.stdout
        assert "echoed: b'world'" in result.stdout
        assert "echoed: b'goodbye'" in result.stdout
    finally:
        _stop(server)


def test_pubsub_example() -> None:
    broker, host, port = _start_server("examples/pubsub/broker.py")
    try:
        subscriber = subprocess.Popen(
            [
                sys.executable,
                str(REPO_ROOT / "examples/pubsub/subscriber.py"),
                "--host",
                host,
                "--port",
                str(port),
                "--topic",
                "weather",
                "--count",
                "1",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=REPO_ROOT,
        )
        try:
            assert subscriber.stdout is not None
            ack_line = subscriber.stdout.readline().strip()
            assert ack_line == "subscribed to weather", ack_line

            publish_result = _run_client(
                "examples/pubsub/publisher.py",
                host,
                port,
                "--topic",
                "weather",
                "--message",
                "sunny",
            )
            assert publish_result.returncode == 0, publish_result.stdout + publish_result.stderr
            assert "published to 'weather': 'sunny'" in publish_result.stdout

            rest_of_stdout, _ = subscriber.communicate(timeout=15.0)
            assert subscriber.returncode == 0, ack_line + rest_of_stdout
            assert "[weather] received: 'sunny'" in rest_of_stdout
        finally:
            if subscriber.poll() is None:
                subscriber.kill()
                subscriber.wait(timeout=5.0)
            if subscriber.stdout is not None:
                subscriber.stdout.close()
    finally:
        _stop(broker)
