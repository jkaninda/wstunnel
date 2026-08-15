// Copyright 2026 Jonas Kaninda
// SPDX-License-Identifier: Apache-2.0

// Package wstunnel adapts a single WebSocket into a multiplexed byte stream
// (yamux over WebSocket), so one outbound, NAT-friendly connection can carry
// many independent streams. It is the shared wire protocol for the Miabi
// control plane and the agents/runners that dial into it: the control plane
// OPENS streams (Client), the agent/runner ACCEPTS them (Server). Both ends
// MUST use this package so the yamux framing and keepalive settings match
// exactly — a mismatch silently breaks the tunnel.
package wstunnel

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hashicorp/yamux"
)

// Keepalive/timeout defaults for the yamux session. Exported so callers can
// build a custom Config from the same baseline if needed.
const (
	KeepAliveInterval      = 20 * time.Second
	ConnectionWriteTimeout = 15 * time.Second
)

// wsConn adapts a gorilla *websocket.Conn to a net.Conn: yamux frames travel as
// binary messages and are reassembled into an ordered byte stream on read.
type wsConn struct {
	ws  *websocket.Conn
	r   io.Reader  // current message reader, nil between messages
	wmu sync.Mutex // serializes writers (gorilla allows one concurrent writer)
}

// NewConn wraps a WebSocket connection as a net.Conn carrying the yamux framing.
func NewConn(ws *websocket.Conn) net.Conn { return &wsConn{ws: ws} }

func (c *wsConn) Read(p []byte) (int, error) {
	for {
		if c.r == nil {
			mt, r, err := c.ws.NextReader()
			if err != nil {
				return 0, err
			}
			if mt != websocket.BinaryMessage {
				continue
			}
			c.r = r
		}
		n, err := c.r.Read(p)
		if err == io.EOF {
			c.r = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (c *wsConn) Write(p []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	w, err := c.ws.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return 0, err
	}
	n, err := w.Write(p)
	if err != nil {
		_ = w.Close()
		return n, err
	}
	return n, w.Close()
}

func (c *wsConn) Close() error                       { return c.ws.Close() }
func (c *wsConn) LocalAddr() net.Addr                { return c.ws.LocalAddr() }
func (c *wsConn) RemoteAddr() net.Addr               { return c.ws.RemoteAddr() }
func (c *wsConn) SetReadDeadline(t time.Time) error  { return c.ws.SetReadDeadline(t) }
func (c *wsConn) SetWriteDeadline(t time.Time) error { return c.ws.SetWriteDeadline(t) }
func (c *wsConn) SetDeadline(t time.Time) error {
	_ = c.ws.SetReadDeadline(t)
	return c.ws.SetWriteDeadline(t)
}

// Config returns the shared yamux config: keepalive enabled and logging muted.
// Both ends use the same values, so the framing and liveness detection agree.
func Config() *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.EnableKeepAlive = true
	cfg.KeepAliveInterval = KeepAliveInterval
	cfg.ConnectionWriteTimeout = ConnectionWriteTimeout
	cfg.LogOutput = io.Discard
	return cfg
}

// Client opens the stream-opening side of the session over ws (the control
// plane): it OPENS streams (e.g. one per Docker request or job lease).
func Client(ws *websocket.Conn) (*yamux.Session, error) {
	return yamux.Client(NewConn(ws), Config())
}

// Server opens the stream-accepting side of the session over ws (an agent or
// runner): it ACCEPTS streams opened by the control plane.
func Server(ws *websocket.Conn) (*yamux.Session, error) {
	return yamux.Server(NewConn(ws), Config())
}
