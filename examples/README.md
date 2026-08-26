# Examples

Every server prints `listening on <host>:<port>` as its first line of
output once it is bound and ready — scripts and tests can parse that line
to discover the port when `--port 0` was used. Every server shuts down
cleanly on `SIGINT` (Ctrl+C).

## echo

The minimum viable client/server pair: the server sends every message
straight back.

Terminal 1:

```
uv run python examples/echo/server.py --port 0
listening on 127.0.0.1:54321
```

Terminal 2 (use the port printed above):

```
uv run python examples/echo/client.py --port 54321
echoed: b'hello'
echoed: b'world'
echoed: b'goodbye'
```

## rpc

A JSON-RPC-flavoured request/response pair. Requests carry an `id`; the
server replies with the same `id` and either a `result` or an `error`.
The client demonstrates `request()`, correlating each reply by `id`.

Terminal 1:

```
uv run python examples/rpc/server.py --port 0
listening on 127.0.0.1:54322
```

Terminal 2:

```
uv run python examples/rpc/client.py --port 54322
call 1 result: 5
call 2 result: {'message': 'hi'}
call 3 error: {'message': 'unknown method: unknown'}
```

## pubsub

A broker that fans out published messages to subscribers of the same
topic, using `broadcast`-style delivery scoped per topic.

Terminal 1 (broker):

```
uv run python examples/pubsub/broker.py --port 0
listening on 127.0.0.1:54323
```

Terminal 2 (subscriber, blocks waiting for one message on `weather`):

```
uv run python examples/pubsub/subscriber.py --port 54323 --topic weather --count 1
subscribed to weather
[weather] received: 'sunny'
```

Terminal 3 (publisher):

```
uv run python examples/pubsub/publisher.py --port 54323 --topic weather --message sunny
published to 'weather': 'sunny'
```

A subscriber on a different topic (e.g. `--topic traffic`) never sees the
`weather` message.

## async_echo

The asyncio counterpart to `echo`, proving the async model is not
documentation-only.

Terminal 1:

```
uv run python examples/async_echo/server.py --port 0
listening on 127.0.0.1:54324
```

Terminal 2:

```
uv run python examples/async_echo/client.py --port 54324
echoed: b'hello'
echoed: b'world'
echoed: b'goodbye'
```
