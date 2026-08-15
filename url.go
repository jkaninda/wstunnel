// Copyright 2026 Jonas Kaninda
// SPDX-License-Identifier: Apache-2.0

package wstunnel

import "strings"

// URL converts an http(s) control-plane base URL and a connect path into the
// ws(s) endpoint a client dials. It normalizes the scheme (http→ws, https→wss)
// and joins the path with exactly one slash, so callers pass their own connect
// path (e.g. "/api/v1/agent/connect" or "/api/v1/runner/connect").
func URL(base, path string) string {
	base = strings.TrimRight(base, "/")
	switch {
	case strings.HasPrefix(base, "https://"):
		base = "wss://" + strings.TrimPrefix(base, "https://")
	case strings.HasPrefix(base, "http://"):
		base = "ws://" + strings.TrimPrefix(base, "http://")
	}
	if path == "" {
		return base
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return base + path
}
