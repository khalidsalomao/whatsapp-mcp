package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
	"unicode"
)

const (
	tunnelConfigFilename = "cloudflare-tunnel.json"
	maxTunnelTokenBytes  = 4096
)

type tunnelConfig struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}

func normalizeTunnelURL(value string) (string, error) {
	endpoint, err := url.Parse(strings.TrimSpace(value))
	if err != nil || endpoint.Scheme != "https" || endpoint.User != nil ||
		endpoint.Host == "" || endpoint.RawQuery != "" || endpoint.ForceQuery ||
		endpoint.Fragment != "" || endpoint.RawPath != "" {
		return "", errors.New("enter an HTTPS URL for the published Cloudflare hostname, such as https://whatsapp.example.com/mcp")
	}
	host := strings.ToLower(endpoint.Hostname())
	if endpoint.Host != endpoint.Hostname() || !strings.Contains(host, ".") || net.ParseIP(host) != nil || strings.HasSuffix(host, ".") {
		return "", errors.New("use the published Cloudflare hostname without a custom port")
	}
	if host == "trycloudflare.com" || strings.HasSuffix(host, ".trycloudflare.com") {
		return "", errors.New("Quick Tunnel URLs change on restart; use a named Cloudflare Tunnel and a published hostname")
	}
	switch endpoint.Path {
	case "", "/", mcpHTTPPath, mcpHTTPPath + "/":
		return (&url.URL{Scheme: "https", Host: host, Path: mcpHTTPPath}).String(), nil
	default:
		return "", errors.New("the Cloudflare URL must end in /mcp or have no path")
	}
}

func newTunnelConfig(endpoint, secret string) (*tunnelConfig, error) {
	endpoint, err := normalizeTunnelURL(endpoint)
	if err != nil {
		return nil, err
	}
	token := strings.TrimSpace(secret)
	if !validTunnelToken(token) {
		return nil, errors.New("enter the Cloudflare tunnel connector token (the value after --token in the dashboard command)")
	}
	return &tunnelConfig{URL: endpoint, Token: token}, nil
}

func validTunnelToken(token string) bool {
	if len(token) > maxTunnelTokenBytes || strings.ContainsFunc(token, unicode.IsSpace) {
		return false
	}
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return false
	}
	var payload struct {
		Account string `json:"a"`
		Tunnel  string `json:"t"`
		Secret  string `json:"s"`
	}
	return json.Unmarshal(decoded, &payload) == nil && payload.Account != "" && payload.Tunnel != "" && payload.Secret != ""
}

func readTunnelConfig(dir string) (*tunnelConfig, error) {
	data, err := readPrivateFile(filepath.Join(dir, tunnelConfigFilename), 2*maxTunnelTokenBytes)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read Cloudflare tunnel settings: %w", err)
	}
	var settings tunnelConfig
	if err = json.Unmarshal(data, &settings); err != nil {
		return nil, errors.New("invalid Cloudflare tunnel settings; run setup again with a tunnel URL and token")
	}
	return newTunnelConfig(settings.URL, settings.Token)
}

func saveTunnelConfig(dir string, cfg *tunnelConfig) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return writePrivateFile(filepath.Join(dir, tunnelConfigFilename), data, true)
}

func removeTunnelConfig(dir string) error {
	path := filepath.Join(dir, tunnelConfigFilename)
	err := syscall.Unlink(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return &os.PathError{Op: "remove tunnel config", Path: path, Err: err}
	}
	return nil
}

func cloudflaredPath() (string, error) {
	path, err := exec.LookPath("cloudflared")
	if err != nil {
		return "", errors.New("cloudflared is required for the configured Cloudflare tunnel. Install it (on macOS: brew install cloudflared) or run start --local")
	}
	return path, nil
}

func launchTunnel(ctx context.Context, binary string, config *tunnelConfig) (*exec.Cmd, error) {
	// The dashboard manages ingress. Ignore unrelated local cloudflared
	// config and tunnel environment variables, and keep the token out of argv.
	cmd := exec.CommandContext(ctx, binary, "tunnel", "--config", "", "--no-autoupdate",
		"--metrics", "127.0.0.1:0", "--grace-period", "5s", "run")
	cmd.Env = append(slices.DeleteFunc(os.Environ(), func(entry string) bool {
		return strings.HasPrefix(entry, "TUNNEL_")
	}), "TUNNEL_TOKEN="+config.Token)
	// The parent forwards cancellation exactly once, giving cloudflared time
	// to finish in-flight requests before the local listener is shut down.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 6 * time.Second
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start cloudflared: %w", err)
	}
	return cmd, nil
}

func runTunnel(ctx context.Context, binary string, config *tunnelConfig, token string) error {
	childCtx, stopChild := context.WithCancel(ctx)
	defer stopChild()
	cmd, err := launchTunnel(childCtx, binary, config)
	if err != nil {
		return err
	}
	startupCtx, stopStartup := context.WithCancel(childCtx)
	defer stopStartup()
	exited := make(chan error, 1)
	go func() {
		exited <- cmd.Wait()
		stopStartup()
	}()

	fmt.Fprintln(os.Stderr, "Cloudflare Tunnel connecting. Checking the published MCP URL…")
	if err := waitForPublicMCP(startupCtx, config.URL, token); err == nil {
		fmt.Fprintln(os.Stderr, "Cloudflare HTTPS endpoint:", config.URL)
	} else if startupCtx.Err() == nil {
		stopChild()
		<-exited
		return fmt.Errorf("Cloudflare HTTPS endpoint failed verification: %w; check the published route, or run start --local", err)
	}
	err = <-exited
	if ctx.Err() != nil {
		return nil
	}
	if err == nil {
		err = errors.New("process exited")
	}
	return fmt.Errorf("cloudflared stopped: %w; check the Cloudflare tunnel token and route, or run start --local", err)
}

func waitForPublicMCP(ctx context.Context, endpoint, token string) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		err := probeMCP(ctx, endpoint, token)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-ticker.C:
		}
	}
}
