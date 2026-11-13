# Notification server logging

All logs go to stdout/stderr (`kubectl logs -f <notifications-node-pod>` / `docker logs -f notifications-node`).

## Settings

| Variable               | Values                                    | Default  | Meaning                                                                 |
| ---------------------- | ----------------------------------------- | -------- | ----------------------------------------------------------------------- |
| `LOG_LEVEL`            | `error`, `warn`, `info`, `debug`, `trace` | `info`   | `debug` adds every streamed chunk, request headers and full LLM payloads; `trace` adds every OpenSearch poll, raw upstream SSE frame and heartbeat |
| `LOG_FORMAT`           | `pretty`, `json`                          | `pretty` | `json` = one JSON object per line, for Loki/OpenSearch/ELK              |
| `LOG_CONTENT`          | `true`, `false`                           | `true`   | `false` hides message texts and payloads (only lengths are logged)      |
| `LOG_MAX_VALUE_LENGTH` | number                                    | `2000`   | longer strings are cut with `…[+N chars]`                               |

Secrets are always redacted: cookies, `Authorization`, passwords, client secrets, API keys, tokens, OpenSearch URL
credentials. Instead of the value a short fingerprint is logged (`[REDACTED fp=72fd9c916d]`, `session=72fd9c916d`), so
calls belonging to one session can be matched without exposing it.

## What a line looks like

```
2026-11-13T09:40:08.279Z INFO  [http-in] → POST /channels/ch1/orchestrate/stream req=a1b2c3d4e5f6 ip=10.0.0.4 body={"message":"Kuidas taotleda passi?",...}
2026-11-13T09:40:08.280Z INFO  [http-out] ⇢ llm-orchestrator POST http://llm-orchestration-service:8100/orchestrate/stream req=a1b2c3d4e5f6 body={...}
2026-11-13T09:40:08.313Z INFO  [http-out] ⇠ llm-orchestrator POST .../orchestrate/stream 200 (stream opened) req=a1b2c3d4e5f6 durationMs=33.6
2026-11-13T09:40:08.314Z INFO  [sse] ⇒ SSE stream_start req=a1b2c3d4e5f6 connectionId=df70… channelId=ch1
2026-11-13T09:40:08.434Z INFO  [sse] LLM answer stream_end req=a1b2c3d4e5f6 chunks=4 chars=26 durationMs=120 answer="Tere kodanik, siin vastus."
2026-11-13T09:40:08.434Z INFO  [http-in] ← POST /channels/ch1/orchestrate/stream 200 req=a1b2c3d4e5f6 durationMs=155.6 response={...}
```

Arrows: `→`/`←` request into this server and its response, `⇢`/`⇠` call from this server to another service
(Ruuter, LLM orchestrator, Azure) and its result, `⇒` event written to a browser over SSE, `×` connection closed.

## Following one request

Every line caused by an HTTP request carries `req=<id>` (the incoming `X-Request-Id` header, or a generated id, which is
also returned in the `X-Request-Id` response header). Timers started by that request (chat termination, logout) keep the
same id. Lines about one SSE connection carry `connectionId=`, lines about a chat carry `channelId=` / `chatId=`.

```sh
kubectl logs deploy/notifications-node | grep 'req=a1b2c3d4e5f6'
kubectl logs deploy/notifications-node | grep 'channelId=ch1'
kubectl logs deploy/notifications-node | grep -E 'WARN|ERROR'
```

## Components

`http-in` incoming HTTP, `http-out` outgoing HTTP, `sse` browser connections and events sent, `opensearch` notification
delivery, bulk writes and chat queue, `chat-queue` queue position updates, `llm` widget LLM / Azure flow, `llm-gui` LLM
Module GUI streaming, `azure-openai`, `azure-agentic`, `azure-auth`, `stream-queue` requests waiting for an SSE
connection, `termination-queue`, `logout-queue`, `connections`, `server`, `startup`.

## Personal data

With `LOG_CONTENT=true` the logs contain what citizens wrote and what the bot answered. Use `LOG_CONTENT=false` in
production unless log retention and access are covered for personal data.
