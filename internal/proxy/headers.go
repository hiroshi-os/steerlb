package proxy

import (
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/http"
	"strings"
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

// StripHopByHopHeaders strips RFC 9110 hop-by-hop headers and any header named in Connection.
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

// AppendXForwardedFor appends the remote address client IP to existing X-Forwarded-For.
func AppendXForwardedFor(h http.Header, remoteAddr string) {
	clientIP := remoteAddr
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		clientIP = host
	}

	if prior := h.Get("X-Forwarded-For"); prior != "" {
		h.Set("X-Forwarded-For", prior+", "+clientIP)
	} else {
		h.Set("X-Forwarded-For", clientIP)
	}
}

// GenerateRequestID generates a random 16-byte hex string.
func GenerateRequestID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "0000000000000000"
	}
	return hex.EncodeToString(b)
}
