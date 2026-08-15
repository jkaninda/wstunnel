# wstunnel

[![Go Reference](https://pkg.go.dev/badge/github.com/jkaninda/wstunnel.svg)](https://pkg.go.dev/github.com/jkaninda/wstunnel)
[![Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

One outbound WebSocket carrying many independent streams — [yamux](https://github.com/hashicorp/yamux)
over a `net.Conn` adapter.

A machine behind NAT, a home router or a corporate firewall usually cannot accept
inbound connections, but it can always make outbound ones. wstunnel inverts the
direction: the agent dials out once, and the server opens as many streams back
down that single connection as it needs — one per request, job or session.

It depends only on `gorilla/websocket` and `hashicorp/yamux`. No logging, no ORM,
no framework, so the binaries that embed it stay small.

## Install

```sh
go get github.com/jkaninda/wstunnel
```

```go
import "github.com/jkaninda/wstunnel"
```

Requires Go 1.25 or newer.

## How it works

The side that **opens** streams calls `Client`; the side that **accepts** them
calls `Server`. That is the only asymmetry — and it is deliberately independent
of who dialed, which is what lets the machine behind NAT be the one taking work.

Both ends must use this package: yamux framing and keepalive settings have to
match exactly, and a mismatch does not fail loudly — it hangs.

## Example

### The server (public, accepts the WebSocket)

```go
package main

import (
	"io"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
	"github.com/jkaninda/wstunnel"
)

var (
	upgrader = websocket.Upgrader{}
	// One connected agent, to keep the example short. A real server keys
	// sessions by the identity it authenticated on the handshake.
	agent atomic.Pointer[yamux.Session]
)

func main() {
	http.HandleFunc("/connect", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		// Client: this end OPENS streams, even though the agent dialed us.
		sess, err := wstunnel.Client(ws)
		if err != nil {
			return
		}
		agent.Store(sess)
		log.Println("agent connected")
	})

	// Every request to :8080 is forwarded down one new stream.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		sess := agent.Load()
		if sess == nil {
			http.Error(w, "no agent connected", http.StatusServiceUnavailable)
			return
		}
		stream, err := sess.OpenStream()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer func() { _ = stream.Close() }()

		if err := r.Write(stream); err != nil { // send the request as-is
			return
		}
		_, _ = io.Copy(w, stream) // stream the response back
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

### The agent (behind NAT, dials out)

```go
package main

import (
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/hashicorp/yamux"
	"github.com/jkaninda/wstunnel"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	opts := wstunnel.ClientOptions{
		URL:       wstunnel.URL("https://tunnel.example.com", "/connect"),
		Header:    http.Header{"Authorization": {"Bearer secret"}},
		OnConnect: func() { log.Println("connected") },
		OnError:   func(err error) { log.Println("disconnected:", err) },
	}

	// Serve blocks until ctx is cancelled, reconnecting with backoff in between.
	err := wstunnel.Serve(ctx, opts, func(_ context.Context, sess *yamux.Session) error {
		for {
			stream, err := sess.AcceptStream()
			if err != nil {
				return err // session ended: Serve reconnects
			}
			go proxy(stream, "127.0.0.1:3000") // whatever this machine runs
		}
	})
	if err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}

// proxy pipes one tunnel stream to a local service.
func proxy(stream net.Conn, addr string) {
	defer stream.Close()
	local, err := net.Dial("tcp", addr)
	if err != nil {
		return
	}
	defer local.Close()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(local, stream); done <- struct{}{} }()
	go func() { _, _ = io.Copy(stream, local); done <- struct{}{} }()
	<-done
}
```

Run the server anywhere with a public address, run the agent on the machine that
has the service, and `http://<server>:8080` reaches it — with no port forwarding,
no inbound firewall rule and no public IP on the agent.

## API

| Symbol | Purpose |
|---|---|
| `NewConn(ws) net.Conn` | Adapt a `*websocket.Conn` to a `net.Conn` carrying yamux framing |
| `Client(ws)` | Session that **opens** streams |
| `Server(ws)` | Session that **accepts** streams |
| `Dial(ctx, opts)` | One-shot dial → an accepting session |
| `Serve(ctx, opts, handle)` | Reconnecting accept loop with exponential backoff |
| `URL(base, path)` | Normalize an `http(s)` base + path to the `ws(s)` endpoint |
| `Config() *yamux.Config` | The shared keepalive/timeout config both ends use |

### ClientOptions

| Field | Default | Notes |
|---|---|---|
| `URL` | — | The `ws(s)` endpoint; build it with `URL()`. |
| `Header` | — | Sent on the handshake — this is where auth goes. |
| `Dialer` | default dialer | Override for a custom TLS config or proxy. |
| `Insecure` | `false` | Skips TLS verification. Development only; ignored when `Dialer` is set. |
| `MinBackoff` / `MaxBackoff` | 1s / 30s | Bound the reconnect delay. |
| `OnConnect` / `OnError` | no-op | Callbacks instead of a logging dependency, so the caller keeps its own logger. |

Keepalive is 20s with a 15s write timeout (`KeepAliveInterval`,
`ConnectionWriteTimeout`), exported so a custom `yamux.Config` can start from the
same baseline.

## Notes

- **Authenticate on the handshake.** Once the WebSocket is up, every stream on it
  belongs to whoever connected — the tunnel carries no per-stream identity.
- **One writer at a time.** `NewConn` serializes writes internally, because
  gorilla allows a single concurrent writer; yamux multiplexing happens above it.
- **`Serve` owns the reconnect loop.** Return from your handler to end a session
  and it dials again; cancel the context to stop for good.

## Used by

[Miabi](https://github.com/miabi-io/miabi) — its node agent and CI runners both
reach the control plane through this tunnel, which is why the wire protocol lives
in one module rather than being reimplemented at each end.

## License

[Apache-2.0](LICENSE).
