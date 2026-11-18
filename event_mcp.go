package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	defaultMCPListen = "127.0.0.1:8765"
	mcpTokenFilename = "mcp-http.token"
	mcpHTTPPath      = "/mcp"
	mcpAuthChallenge = `Bearer realm="local-whatsapp"`
)

type eventMCPResult struct {
	mcp.ResultBase
	Raw json.RawMessage
}

func (r *eventMCPResult) MarshalJSON() ([]byte, error) { return r.Raw, nil }

// The upstream SDK supports custom methods but its capability struct does not
// yet have the draft Events field. Preserve the entire SDK result and add only
// capabilities.events at the serialization boundary.
type eventCapabilitiesResult struct{ mcp.Result }

func (r *eventCapabilitiesResult) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(r.Result)
	if err != nil {
		return nil, err
	}
	var out map[string]json.RawMessage
	if err = json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	var capabilities map[string]any
	if err = json.Unmarshal(out["capabilities"], &capabilities); err != nil {
		return nil, err
	}
	if capabilities == nil {
		capabilities = map[string]any{}
	}
	capabilities["events"] = map[string]any{}
	out["capabilities"], err = json.Marshal(capabilities)
	if err != nil {
		return nil, err
	}
	return json.Marshal(out)
}

func addMCPEvents(s *mcp.Server, c *localClient) {
	for _, method := range []string{"list", "subscribe", "unsubscribe"} {
		err := mcp.AddReceivingCustomMethod(s, "events/"+method, func(ctx context.Context, _ *mcp.ServerSession, in *eventParams) (*eventMCPResult, error) {
			var reply eventReply
			if err := c.call(ctx, "POST", "/events/"+method, in, &reply); err != nil {
				return nil, err
			}
			if reply.Error != nil {
				return nil, reply.Error
			}
			return &eventMCPResult{Raw: reply.Result}, nil
		})
		if err != nil {
			panic(err)
		} // Constant custom method names cannot conflict.
	}
	s.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, request mcp.Request) (mcp.Result, error) {
			result, err := next(ctx, method, request)
			if err == nil && (method == "initialize" || method == "server/discover") {
				return &eventCapabilitiesResult{Result: result}, nil
			}
			return result, err
		}
	})
}

func readMCPToken(path string) (string, error) {
	b, err := readPrivateFile(path, 1024)
	if err != nil {
		return "", fmt.Errorf("read MCP token: %w", err)
	}
	token := strings.TrimSpace(string(b))
	if len(token) < 32 || strings.ContainsAny(token, " \t\r\n") {
		return "", errors.New("MCP token must contain at least 32 non-whitespace characters")
	}
	return token, nil
}

func initMCPToken(dir string) error {
	if err := privateDir(dir); err != nil {
		return err
	}
	path := filepath.Join(dir, mcpTokenFilename)
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		return err
	}
	encoded := base64.RawURLEncoding.EncodeToString(key[:])
	err := writePrivateFile(path, []byte(encoded+"\n"), false)
	if errors.Is(err, os.ErrExist) {
		if _, err = readMCPToken(path); err != nil {
			return err
		}
		fmt.Println("Existing private MCP token file:", path)
		return nil
	}
	if err != nil {
		return err
	}
	fmt.Println("Created private MCP token file:", path)
	return nil
}

func authenticatedMCP(c *localClient, token string) http.Handler {
	s := newMCP(c)
	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path != mcpHTTPPath {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Origin") != "" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		auth := r.Header.Get("Authorization")
		if subtle.ConstantTimeCompare([]byte(auth), []byte("Bearer "+token)) != 1 {
			w.Header().Set("WWW-Authenticate", mcpAuthChallenge)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		mcpHandler.ServeHTTP(w, r)
	})
}

func validateMCPListen(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return errors.New("listen address must be an explicit loopback IP and port")
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || !ip.IsLoopback() {
		return errors.New("HTTP MCP must bind to loopback; use an authenticated HTTPS proxy for remote access")
	}
	return nil
}

func listenMCPHTTP(address string) (net.Listener, error) {
	if err := validateMCPListen(address); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", address)
	if errors.Is(err, syscall.EADDRINUSE) {
		return nil, fmt.Errorf("MCP address %s is already in use; stop the other listener or choose a different --listen address: %w", address, err)
	}
	return listener, err
}

func runMCPHTTP(ctx context.Context, dir, address, tokenPath string) error {
	if tokenPath == "" {
		return errors.New("--token-file is required; run events-init to create a private token")
	}
	token, err := readMCPToken(tokenPath)
	if err != nil {
		return err
	}
	l, err := listenMCPHTTP(address)
	if err != nil {
		return err
	}
	defer l.Close()
	return serveMCPHTTP(ctx, dir, l, token)
}

func serveMCPHTTP(ctx context.Context, dir string, l net.Listener, token string) error {
	c := newLocalClient(dir)
	defer c.http.CloseIdleConnections()
	s := &http.Server{Handler: authenticatedMCP(c, token), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: mediaRequestTimeout + 10*time.Second, IdleTimeout: 60 * time.Second}
	fmt.Fprintln(os.Stderr, "Authenticated MCP HTTP endpoint:", "http://"+l.Addr().String()+mcpHTTPPath)
	return serveHTTP(ctx, s, l)
}
