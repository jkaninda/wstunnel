# wstunnel

Shared WebSocket + [yamux](https://github.com/hashicorp/yamux) tunnel used across
the Miabi ecosystem: one outbound, NAT-friendly WebSocket carries many
independent multiplexed streams. It is the **single source of truth for the wire
protocol** so the control plane and the agents/runners that dial into it always
agree on framing and keepalive — the two ends drifting is what silently breaks a
tunnel, which is exactly what this module prevents.

It depends only on `gorilla/websocket` and `hashicorp/yamux` (no logging, ORM,
or Docker deps), so the lean agent/runner binaries stay small.

## Roles

- **Control plane** opens streams → `wstunnel.Client(ws)`
- **Agent / runner** accepts streams → `wstunnel.Server(ws)` (or `Dial`/`Serve`)

## Usage

Control plane (accepts the WebSocket, drives streams):

```go
ws, _ := upgrader.Upgrade(w, r, nil)
sess, _ := wstunnel.Client(ws)
stream, _ := sess.OpenStream() // one per request / job lease
```

Agent or runner (dials out, services accepted streams, auto-reconnects):

```go
opts := wstunnel.ClientOptions{
    URL:    wstunnel.URL(controlURL, "/api/v1/runner/connect"),
    Header: http.Header{"Authorization": {"Bearer " + token}},
    OnConnect: func() { log.Info("connected") },
    OnError:   func(err error) { log.Warn("disconnected", err) },
}
_ = wstunnel.Serve(ctx, opts, func(ctx context.Context, sess *yamux.Session) error {
    for {
        stream, err := sess.AcceptStream()
        if err != nil {
            return err
        }
        go handle(stream) // pipe to Docker, service a job lease, etc.
    }
})
```

## API

| Symbol | Purpose |
|---|---|
| `NewConn(ws) net.Conn` | Adapt a `*websocket.Conn` to a `net.Conn` carrying yamux framing |
| `Config() *yamux.Config` | The shared keepalive/timeout config both ends use |
| `Client(ws)` / `Server(ws)` | Open the stream-opening / stream-accepting session |
| `URL(base, path)` | Normalize an http(s) base + path to the ws(s) endpoint |
| `Dial(ctx, opts)` | One-shot dial → accepting session |
| `Serve(ctx, opts, handle)` | Reconnecting accept loop with exponential backoff |

Licensed under Apache-2.0.
