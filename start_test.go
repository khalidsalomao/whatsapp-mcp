package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const cliTestArgs = "WHATSAPP_MCP_TEST_CLI_ARGS"

// Each child runs the real command with a temporary account. Unpaired services
// never dial WhatsApp, and the reuse case uses an existing synthetic service.
func TestStartCommand(t *testing.T) {
	if input := os.Getenv(cliTestArgs); input != "" {
		if addr := os.Getenv("WHATSAPP_MCP_TEST_PUBLIC_ADDR"); addr != "" {
			mcpProbeClient.Transport = publicTestTransport(t, addr, os.Getenv("WHATSAPP_MCP_TEST_PUBLIC_CERT"))
		}
		var args []string
		if err := json.Unmarshal([]byte(input), &args); err != nil {
			t.Fatal(err)
		}
		if err := run(args); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}

	t.Run("help succeeds and shows relevant options", func(t *testing.T) {
		output, err := startTestCommand(t, t.TempDir(), "--help").CombinedOutput()
		if err != nil {
			t.Fatalf("help failed: %v %s", err, output)
		}
		text := string(output)
		if !strings.Contains(text, "-listen") || strings.Contains(text, "-open") || strings.Contains(text, "-tool") {
			t.Fatalf("start help shows the wrong options: %s", text)
		}
	})

	t.Run("starts and stops its service and preserves token on restart", func(t *testing.T) {
		dir, err := os.MkdirTemp("/tmp", "wa-start-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(dir) })
		var savedToken string
		for range 2 {
			endpoint, stop := startCommandForTest(t, dir)
			token, err := readMCPToken(filepath.Join(dir, "mcp-http.token"))
			if err != nil {
				t.Fatal(err)
			}
			if savedToken != "" && token != savedToken {
				t.Fatal("restart rotated the token")
			}
			savedToken = token
			current := startEndpointStatus(t, endpoint, token)
			if current.Paired || current.Connected {
				t.Fatal("temporary service unexpectedly paired")
			}
			stop()
			client := newLocalClient(dir)
			_, err = readServiceStatus(t.Context(), client)
			client.http.CloseIdleConnections()
			if err == nil {
				t.Fatal("start left its service running after Ctrl+C")
			}
			if _, err = os.Stat(filepath.Join(dir, "session.db")); err != nil {
				t.Fatal("session database was lost on shutdown")
			}
		}
	})

	t.Run("reuses existing service and custom token", func(t *testing.T) {
		b := testBridge(t)
		dir := serveTestSocketDir(t, b)
		token := strings.Repeat("b", 40)
		path := filepath.Join(t.TempDir(), "custom.token")
		if err := os.WriteFile(path, []byte(token), 0600); err != nil {
			t.Fatal(err)
		}
		endpoint, stop := startCommandForTest(t, dir, "--token-file", path)
		startEndpointStatus(t, strings.Replace(endpoint, "127.0.0.1", "localhost", 1), token)
		stop()
		client := newLocalClient(dir)
		defer client.http.CloseIdleConnections()
		if _, err := readServiceStatus(t.Context(), client); err != nil {
			t.Fatal("start stopped the service it reused")
		}
		if _, err := os.Stat(filepath.Join(dir, "mcp-http.token")); !os.IsNotExist(err) {
			t.Fatal("custom token was ignored")
		}
	})

	t.Run("startup failure prints no endpoint", func(t *testing.T) {
		dir, err := os.MkdirTemp("/tmp", "wa-fail-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(dir) })
		path := filepath.Join(dir, "bridge.sock")
		if err := os.WriteFile(path, []byte("existing file"), 0600); err != nil {
			t.Fatal(err)
		}
		output, err := startTestCommand(t, dir).CombinedOutput()
		if err == nil || !strings.Contains(string(output), "bridge.sock exists and is not a socket") || strings.Contains(string(output), "http://") {
			t.Fatalf("failed service advertised an endpoint: %v %s", err, output)
		}
		if contents, err := os.ReadFile(path); err != nil || string(contents) != "existing file" {
			t.Fatal("startup failure replaced the existing socket-path file")
		}
	})

	t.Run("occupied HTTP port leaves existing listener alone", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		dir := t.TempDir()
		output, err := startTestCommand(t, dir, "--listen", listener.Addr().String()).CombinedOutput()
		if err == nil || strings.Contains(string(output), "http://") {
			t.Fatalf("occupied port advertised an endpoint: %v %s", err, output)
		}
		if _, err := os.Stat(filepath.Join(dir, "bridge.sock")); !os.IsNotExist(err) {
			t.Fatal("service was started despite the occupied HTTP port")
		}
		if _, err := os.Stat(filepath.Join(dir, "mcp-http.token")); !os.IsNotExist(err) {
			t.Fatal("occupied port created an unused token")
		}
		connection, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
		if err != nil {
			t.Fatal("existing listener stopped")
		}
		connection.Close()
	})
}

func startTestCommand(t *testing.T, dir string, extra ...string) *exec.Cmd {
	t.Helper()
	args := append([]string{"start", "--data-dir", dir, "--listen", "127.0.0.1:0"}, extra...)
	return testCLICommand(t, args...)
}

func testCLICommand(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	input, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStartCommand$")
	cmd.Env = append(os.Environ(), cliTestArgs+"="+string(input))
	return cmd
}

func startCommandForTest(t *testing.T, dir string, extra ...string) (string, func()) {
	t.Helper()
	endpoint, stop := waitForStartURL(t, startTestCommand(t, dir, extra...), "Authenticated MCP HTTP endpoint: ")
	if !strings.HasPrefix(endpoint, "http://127.0.0.1:") || !strings.HasSuffix(endpoint, "/mcp") {
		t.Fatalf("incomplete endpoint URL %q", endpoint)
	}
	return endpoint, stop
}

func waitForStartURL(t *testing.T, cmd *exec.Cmd, label string) (string, func()) {
	t.Helper()
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan string, 1)
	done := make(chan struct{})
	var processErr error
	var lines []string
	go func() {
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			line := scanner.Text()
			lines = append(lines, line)
			if endpoint, ok := strings.CutPrefix(line, label); ok {
				ready <- endpoint
			}
		}
		processErr = cmd.Wait()
		close(done)
	}()
	t.Cleanup(func() { _ = cmd.Process.Kill(); <-done })
	stop := func() {
		t.Helper()
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
			if processErr != nil {
				t.Fatalf("Ctrl+C failed: %v\n%s", processErr, strings.Join(lines, "\n"))
			}
		case <-time.After(7 * time.Second):
			t.Fatal("Ctrl+C did not stop start")
		}
	}
	select {
	case endpoint := <-ready:
		return endpoint, stop
	case <-done:
		t.Fatalf("start exited before its endpoint was ready: %v\n%s", processErr, strings.Join(lines, "\n"))
	case <-time.After(10 * time.Second):
		t.Fatal("start did not print its endpoint URL")
	}
	return "", nil
}

func startEndpointStatus(t *testing.T, endpoint, token string) status {
	t.Helper()
	return authenticatedStatusForTest(t, endpoint, token, http.DefaultTransport)
}

func authenticatedStatusForTest(t *testing.T, endpoint, token string, transport http.RoundTripper) status {
	t.Helper()
	httpClient := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	response, err := httpClient.Get(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatal("start exposed an unauthenticated endpoint")
	}
	httpClient.Transport = eventRoundTripper(func(r *http.Request) (*http.Response, error) {
		req := r.Clone(r.Context())
		req.Header.Set("Authorization", "Bearer "+token)
		return transport.RoundTrip(req)
	})
	client := mcp.NewClient(&mcp.Implementation{Name: "start-test", Version: "1"}, nil)
	session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: endpoint, HTTPClient: httpClient, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	var current status
	mustCallMCP(t, t.Context(), session, "get_status", struct{}{}, &current)
	return current
}
