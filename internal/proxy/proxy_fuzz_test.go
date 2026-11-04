package proxy

import (
	"net/http"
	"strings"
	"testing"
)

func FuzzHeaders(f *testing.F) {
	// Seed corpus with common headers
	f.Add("Connection", "keep-alive, X-Foo")
	f.Add("Keep-Alive", "timeout=5")
	f.Add("Upgrade", "websocket")
	f.Add("X-Custom", "value123")
	f.Add("Transfer-Encoding", "chunked")
	f.Add("Proxy-Authorization", "secret")

	f.Fuzz(func(t *testing.T, key, val string) {
		h := make(http.Header)
		h.Set(key, val)
		h.Set("Connection", key)

		StripHopByHopHeaders(h)

		// Invariant: no hop-by-hop header remains
		for hop := range hopByHopHeaders {
			if h.Get(hop) != "" {
				t.Fatalf("hop-by-hop header %q was not stripped", hop)
			}
		}
		// Invariant: any header mentioned in Connection was removed
		if strings.EqualFold(key, "Connection") {
			tokens := strings.Split(val, ",")
			for _, tok := range tokens {
				trimmed := strings.TrimSpace(tok)
				if trimmed != "" && h.Get(trimmed) != "" {
					t.Fatalf("header %q named in Connection was not stripped", trimmed)
				}
			}
		}
	})
}
