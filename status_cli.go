package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var mcpProbeClient = &http.Client{
	Timeout: 3 * time.Second,
	CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// A published URL should challenge anonymous callers before we send the bearer
// token. Initialize verifies the actual MCP endpoint without reading WhatsApp.
func probeMCP(ctx context.Context, endpoint, token string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := mcpProbeClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized || resp.Header.Get("WWW-Authenticate") != mcpAuthChallenge {
		return fmt.Errorf("expected the WhatsApp MCP authentication challenge, got HTTP %d; check the origin and Host header", resp.StatusCode)
	}

	const initialize = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"whatsapp-mcp-status","version":"1"}}}`
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(initialize))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err = mcpProbeClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("authenticated MCP request returned HTTP %d; check the token and Cloudflare origin Host header", resp.StatusCode)
	}
	var reply struct {
		Result struct {
			ServerInfo struct {
				Name string `json:"name"`
			} `json:"serverInfo"`
		} `json:"result"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&reply); err != nil || reply.Result.ServerInfo.Name != mcpServerName {
		return errors.New("endpoint did not return the WhatsApp MCP initialization response")
	}
	return nil
}

type endpointStatus struct {
	URL       string `json:"url,omitempty"`
	Available bool   `json:"available"`
	Error     string `json:"error,omitempty"`
}

type cliStatus struct {
	status
	ServiceAvailable bool           `json:"service_available"`
	ServiceError     string         `json:"service_error,omitempty"`
	LocalMCP         endpointStatus `json:"local_mcp"`
	Tunnel           struct {
		Configured bool `json:"configured"`
		endpointStatus
	} `json:"tunnel"`
}

func checkEndpoint(ctx context.Context, endpoint, token string, tokenErr error) endpointStatus {
	result := endpointStatus{URL: endpoint}
	if tokenErr != nil {
		result.Error = tokenErr.Error()
		return result
	}
	if err := probeMCP(ctx, endpoint, token); err != nil {
		result.Error = err.Error()
		return result
	}
	result.Available = true
	return result
}

func runStatus(ctx context.Context, dir, address, tokenPath string) error {
	if err := validateMCPListen(address); err != nil {
		return err
	}
	if tokenPath == "" {
		tokenPath = filepath.Join(dir, mcpTokenFilename)
	}
	token, tokenErr := readMCPToken(tokenPath)
	if errors.Is(tokenErr, os.ErrNotExist) {
		tokenErr = errors.New("MCP token file not found; run whatsapp-mcp start")
	}
	tunnel, configErr := readTunnelConfig(dir)
	var report cliStatus
	var checks sync.WaitGroup
	checks.Go(func() {
		report.LocalMCP = checkEndpoint(ctx, "http://"+address+mcpHTTPPath, token, tokenErr)
	})
	if configErr != nil {
		report.Tunnel.Error = configErr.Error()
	} else if tunnel != nil {
		report.Tunnel.Configured = true
		checks.Go(func() {
			report.Tunnel.endpointStatus = checkEndpoint(ctx, tunnel.URL, token, tokenErr)
		})
	}
	client := newLocalClient(dir)
	defer client.http.CloseIdleConnections()
	current, serviceErr := readServiceStatus(ctx, client)
	report.status = current
	report.ServiceAvailable = serviceErr == nil
	if serviceErr != nil {
		report.ServiceError = "WhatsApp service unavailable; run whatsapp-mcp start"
	}
	checks.Wait()
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return err
	}
	if serviceErr != nil {
		return errors.New(report.ServiceError)
	}
	return nil
}
