package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"image/png"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const publicTestURL = "https://mcp.example.com/mcp"

func shortSetupDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "wa-setup-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func configureTunnelForTest(t *testing.T, dir string) string {
	t.Helper()
	token := base64.StdEncoding.EncodeToString([]byte(`{"t":"example-tunnel","s":"test-connector-secret","a":"test-account"}`))
	path := filepath.Join(t.TempDir(), "connector-token")
	if err := os.WriteFile(path, []byte(token+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	command := testCLICommand(t, "setup", "--data-dir", dir, "--tunnel-url", "https://mcp.example.com/", "--tunnel-token-file", path)
	if out, err := command.CombinedOutput(); err != nil || strings.Contains(string(out), token) || !strings.Contains(string(out), publicTestURL) {
		t.Fatalf("setup did not save and report its URL safely (error: %v)", err)
	}
	return token
}

func TestSetupTunnelConfiguration(t *testing.T) {
	dir := shortSetupDir(t)
	connectorToken := configureTunnelForTest(t, dir)
	config, err := readTunnelConfig(dir)
	if err != nil || config == nil || config.URL != publicTestURL || config.Token != connectorToken {
		t.Fatal("setup did not persist the URL and connector token")
	}
	info, err := os.Stat(filepath.Join(dir, tunnelConfigFilename))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("tunnel configuration is not private")
	}
	mcpTokenPath := filepath.Join(dir, mcpTokenFilename)
	before, err := readMCPToken(mcpTokenPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "session.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("tunnel-only setup modified the WhatsApp session")
	}

	tokenPath := filepath.Join(t.TempDir(), "replacement")
	if err := os.WriteFile(tokenPath, []byte(connectorToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	err = run([]string{"setup", "--data-dir", dir, "--tunnel-url", "https://random.trycloudflare.com", "--tunnel-token-file", tokenPath})
	if err == nil || !strings.Contains(err.Error(), "Quick Tunnel") {
		t.Fatal("setup accepted a temporary URL")
	}
	preserved, err := readTunnelConfig(dir)
	if err != nil || preserved == nil || *preserved != *config {
		t.Fatal("failed setup replaced working tunnel settings")
	}
	if err := run([]string{"setup", "--data-dir", dir, "--remove-tunnel"}); err != nil {
		t.Fatal(err)
	}
	if cfg, err := readTunnelConfig(dir); err != nil || cfg != nil {
		t.Fatal("tunnel configuration was not removed")
	}
	after, err := readMCPToken(mcpTokenPath)
	if err != nil || before != after {
		t.Fatal("removing the tunnel changed the MCP token")
	}
}

func TestSetupPairsThroughRunningServiceAndReusesPairing(t *testing.T) {
	t.Setenv("PATH", t.TempDir()) // Force the QR opener's fallback on macOS.
	dir := shortSetupDir(t)
	qrPath := filepath.Join(dir, "pairing.png")
	listener, err := net.Listen("unix", filepath.Join(dir, "bridge.sock"))
	if err != nil {
		t.Fatal(err)
	}
	var paired atomic.Bool
	var attempts atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(status{Paired: paired.Load()})
	})
	mux.HandleFunc("POST /pair", func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		_ = json.NewEncoder(w).Encode(pairing{State: "starting"})
	})
	mux.HandleFunc("GET /pair", func(w http.ResponseWriter, _ *http.Request) {
		qr, err := os.Open(qrPath)
		if errors.Is(err, os.ErrNotExist) {
			_ = json.NewEncoder(w).Encode(pairing{State: "code", Code: "synthetic pairing QR"})
			return
		}
		if err != nil {
			http.Error(w, "could not read QR image", http.StatusInternalServerError)
			return
		}
		defer qr.Close()
		if _, err := png.Decode(qr); err != nil {
			http.Error(w, "invalid QR image", http.StatusInternalServerError)
			return
		}
		paired.Store(true)
		_ = json.NewEncoder(w).Encode(pairing{State: "success"})
	})
	server := &http.Server{Handler: mux}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
	defer cancel()
	for range 2 {
		if err := runSetup(ctx, dir, setupOptions{pair: true}); err != nil {
			t.Fatal(err)
		}
	}
	if attempts.Load() != 1 || !paired.Load() {
		t.Fatal("setup did not pair exactly once through the existing service")
	}
	if _, err := os.Stat(qrPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("setup left the QR image after pairing")
	}
}

func TestNamedTunnelRestartAndStatus(t *testing.T) {
	dir := shortSetupDir(t)
	secret := configureTunnelForTest(t, dir)
	mcpToken, err := readMCPToken(filepath.Join(dir, mcpTokenFilename))
	if err != nil {
		t.Fatal(err)
	}
	portReservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := portReservation.Addr().String()
	portReservation.Close()
	active := filepath.Join(dir, "cloudflared-active")
	proxy := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: addr})
	direct := proxy.Director
	proxy.Director = func(r *http.Request) { direct(r); r.Host = addr }
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := os.Stat(active); err != nil {
			http.Error(w, "fake tunnel offline", http.StatusBadGateway)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	defer gateway.Close()

	cert := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: gateway.Certificate().Raw}))
	gatewayAddr := gateway.Listener.Addr().String()
	t.Setenv("WHATSAPP_MCP_TEST_PUBLIC_ADDR", gatewayAddr)
	t.Setenv("WHATSAPP_MCP_TEST_PUBLIC_CERT", cert)
	t.Setenv("WHATSAPP_MCP_TEST_CONNECTOR_TOKEN", secret)
	t.Setenv("WHATSAPP_MCP_TEST_CONNECTOR_ACTIVE", active)
	t.Setenv("TUNNEL_TOKEN", "unrelated-token")
	t.Setenv("TUNNEL_TOKEN_FILE", "/unrelated")
	t.Setenv("PATH", stubCloudflared(t)+string(os.PathListSeparator)+os.Getenv("PATH"))
	transport := publicTestTransport(t, gatewayAddr, cert)
	defer transport.CloseIdleConnections()
	for range 2 {
		endpoint, stop := waitForStartURL(t, startTestCommand(t, dir, "--listen", addr), "Cloudflare HTTPS endpoint: ")
		if endpoint != publicTestURL {
			t.Fatalf("public URL changed to %q", endpoint)
		}
		authenticatedStatusForTest(t, publicTestURL, mcpToken, transport)
		out, err := testCLICommand(t, "status", "--listen", addr, "--data-dir", dir).Output()
		var report cliStatus
		if err != nil || json.Unmarshal(out, &report) != nil || !report.ServiceAvailable ||
			!report.LocalMCP.Available || !report.Tunnel.Configured || !report.Tunnel.Available || report.Tunnel.URL != publicTestURL {
			t.Fatalf("status did not report all three components ready: %v\n%s", err, out)
		}
		stop()
		if _, err := os.Stat(active); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("start exited before cloudflared shut down")
		}
		saved, err := readMCPToken(filepath.Join(dir, mcpTokenFilename))
		if err != nil || saved != mcpToken {
			t.Fatal("start rotated the Grok authentication token")
		}
	}

	endpoint, stop := startCommandForTest(t, dir, "--local")
	startEndpointStatus(t, endpoint, mcpToken)
	stop()
	if _, err := os.Stat(active); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("start --local launched a configured tunnel")
	}
	t.Setenv("WHATSAPP_MCP_TEST_CONNECTOR_FAIL", "1")
	out, err := startTestCommand(t, dir).CombinedOutput()
	if err == nil || !strings.Contains(string(out), "cloudflared stopped") || strings.Contains(string(out), "Cloudflare HTTPS endpoint:") {
		t.Fatalf("tunnel failure was not reported correctly: %v\n%s", err, out)
	}
}

// Exercise the real child process lifecycle and TLS MCP verification against a
// local reverse proxy. No request contacts Cloudflare or WhatsApp.
func stubCloudflared(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	stub := `#!/bin/sh
set -eu
[ "$#" -eq 9 ]
[ "$1" = "tunnel" ] && [ "$9" = "run" ]
[ "$TUNNEL_TOKEN" = "$WHATSAPP_MCP_TEST_CONNECTOR_TOKEN" ]
[ -z "${TUNNEL_TOKEN_FILE:-}" ]
[ -z "${WHATSAPP_MCP_TEST_CONNECTOR_FAIL:-}" ] || exit 9
marker="$WHATSAPP_MCP_TEST_CONNECTOR_ACTIVE"
trap 'exit 0' TERM
trap '/bin/rm -f "$marker"' EXIT
: > "$marker"
for wait in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do /bin/sleep 1; done
`
	if err := os.WriteFile(filepath.Join(dir, "cloudflared"), []byte(stub), 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func publicTestTransport(t *testing.T, address, cert string) *http.Transport {
	t.Helper()
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(cert)) {
		t.Fatal("invalid TLS fixture certificate")
	}
	return &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: roots},
		DialContext: func(ctx context.Context, network, target string) (net.Conn, error) {
			if target == "mcp.example.com:443" {
				target = address
			}
			return (&net.Dialer{}).DialContext(ctx, network, target)
		},
	}
}

func TestProbeMCPDoesNotFollowRedirects(t *testing.T) {
	var redirected atomic.Bool
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer sink.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("WWW-Authenticate", mcpAuthChallenge)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		http.Redirect(w, r, sink.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	if err := probeMCP(t.Context(), server.URL+mcpHTTPPath, strings.Repeat("x", 40)); err == nil || redirected.Load() {
		t.Fatal("probe followed a redirect from the configured MCP URL")
	}
}
