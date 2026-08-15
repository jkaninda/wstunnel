// Copyright 2026 Jonas Kaninda
// SPDX-License-Identifier: Apache-2.0

package wstunnel

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestClientServerRoundTrip drives a byte stream from the control-plane (Client)
// side through the WebSocket+yamux tunnel to the accepting (Server) side and
// back, exercising the whole transport in one process.
func TestClientServerRoundTrip(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	// Control plane: on connect, become the yamux client and open a stream that
	// echoes upper-cased what the accepting side writes back... actually we drive
	// the request from the client side: open a stream, write, read the echo.
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		sess, err := Client(ws)
		if err != nil {
			return
		}
		stream, err := sess.OpenStream()
		if err != nil {
			return
		}
		if _, err := stream.Write([]byte("ping")); err != nil {
			return
		}
		buf := make([]byte, 4)
		_, _ = io.ReadFull(stream, buf)
		_ = stream.Close()
		if string(buf) != "PING" {
			t.Errorf("round-trip = %q, want PING (upper-cased echo)", buf)
		}
		_ = sess.Close()
	}))
	defer cp.Close()

	// Accepting side (agent/runner): dial, become the yamux server, accept the
	// stream, upper-case the request and echo it.
	sess, err := Dial(context.Background(), ClientOptions{URL: "ws://" + strings.TrimPrefix(cp.URL, "http://")})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer sess.Close()

	stream, err := sess.AcceptStream()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if _, err := stream.Write([]byte(strings.ToUpper(string(buf)))); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Give the control-plane handler a moment to assert before teardown.
	time.Sleep(100 * time.Millisecond)
	_ = stream.Close()
}

func TestURL(t *testing.T) {
	cases := []struct{ base, path, want string }{
		{"https://cp.example.com", "/api/v1/runner/connect", "wss://cp.example.com/api/v1/runner/connect"},
		{"http://localhost:9000/", "api/v1/agent/connect", "ws://localhost:9000/api/v1/agent/connect"},
		{"wss://cp.example.com", "", "wss://cp.example.com"},
	}
	for _, tc := range cases {
		if got := URL(tc.base, tc.path); got != tc.want {
			t.Errorf("URL(%q,%q) = %q, want %q", tc.base, tc.path, got, tc.want)
		}
	}
}
