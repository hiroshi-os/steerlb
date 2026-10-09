package proxy

import (
	"net/http"
	"strings"
	"testing"
)

// Hop-by-hop headers per RFC 9110 §7.6.1
var hopByHopHeaders = map[string]bool{
	"connection":          true,
	"keep-alive":          true,
	"proxy-authenticate":  true,
	"proxy-authorization": true,
	"te":                  true,
	"trailer":             true,
	"transfer-encoding":   true,
	"upgrade":             true,
}

// StripHopByHopHeaders strips RFC 9110 hop-by-hop headers and any header named in the Connection header.
func StripHopByHopHeaders(h http.Header) {
	if conn := h.Get("Connection"); conn != "" {
		for _, token := range strings.Split(conn, ",") {
			trimmed := strings.TrimSpace(token)
			if trimmed != "" {
				h.Del(trimmed)
			}
		}
	}
	for k := range hopByHopHeaders {
		h.Del(k)
	}
}

func FuzzHeaders(f *testing.F) {
	// Seed corpus with common headers
	f.Add("Connection", "keep-alive, X-Foo")
	f.Add("Keep-Alive", "timeout=5")
	f.Add("Upgrade", "websocket")
	f.Add("X-Custom", "value123")

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
