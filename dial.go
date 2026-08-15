// Copyright 2026 Jonas Kaninda
// SPDX-License-Identifier: Apache-2.0

package wstunnel

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
)

// ClientOptions configures the accepting (agent/runner) side of the tunnel.
type ClientOptions struct {
	// URL is the fully-resolved ws(s) endpoint (see URL()).
	URL string
	// Header carries auth and metadata on the WebSocket handshake (e.g.
	// "Authorization: Bearer <token>", "X-Runner-Version").
	Header http.Header
	// Dialer overrides the WebSocket dialer (nil uses a copy of the default).
	Dialer *websocket.Dialer
	// Insecure skips TLS verification (dev only). Ignored when Dialer is set.
	Insecure bool
	// MinBackoff / MaxBackoff bound the reconnect delay (defaults 1s / 30s).
	MinBackoff, MaxBackoff time.Duration
	// OnConnect fires after each successful session is established; OnError fires
	// with the cause each time a session ends or a dial fails. Both are optional
	// (nil = no-op), keeping this package free of any logging dependency.
	OnConnect func()
	OnError   func(error)
}

// Dial opens one WebSocket to opts.URL and returns the accepting (Server-side)
// yamux session. The caller owns closing the session (which closes the ws).
func Dial(ctx context.Context, opts ClientOptions) (*yamux.Session, error) {
	dialer := opts.Dialer
	if dialer == nil {
		d := *websocket.DefaultDialer
		if opts.Insecure {
			d.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // opt-in dev flag
		}
		dialer = &d
	}
	ws, _, err := dialer.DialContext(ctx, opts.URL, opts.Header)
	if err != nil {
		return nil, err
	}
	sess, err := Server(ws)
	if err != nil {
		_ = ws.Close()
		return nil, err
	}
	return sess, nil
}

// Serve runs the accepting side until ctx is cancelled, reconnecting with
// exponential backoff. For each live session it calls handle(ctx, sess), which
// should block for the session's lifetime (e.g. accepting and servicing
// streams) and return when it ends. A clean session (handle returns nil) resets
// the backoff. handle owns the session; Serve closes it before retrying.
func Serve(ctx context.Context, opts ClientOptions, handle func(ctx context.Context, sess *yamux.Session) error) error {
	minB, maxB := opts.MinBackoff, opts.MaxBackoff
	if minB <= 0 {
		minB = time.Second
	}
	if maxB <= 0 {
		maxB = 30 * time.Second
	}
	backoff := minB
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		sess, err := Dial(ctx, opts)
		if err == nil {
			if opts.OnConnect != nil {
				opts.OnConnect()
			}
			backoff = minB // a successful connect resets the backoff
			connCtx, cancel := context.WithCancel(ctx)
			go closeOnDone(connCtx, sess)
			err = handle(connCtx, sess)
			cancel()
			_ = sess.Close()
		}
		if err != nil && opts.OnError != nil && ctx.Err() == nil {
			opts.OnError(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxB {
			if backoff *= 2; backoff > maxB {
				backoff = maxB
			}
		}
	}
}

// closeOnDone closes the session when the connection context is cancelled, so a
// handler blocked on AcceptStream unblocks on shutdown.
func closeOnDone(ctx context.Context, sess *yamux.Session) {
	<-ctx.Done()
	_ = sess.Close()
}
