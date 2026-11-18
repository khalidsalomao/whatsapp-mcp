package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func runStart(ctx context.Context, dir, address, tokenPath string, localOnly bool) error {
	var config *tunnelConfig
	var cloudflared string
	if !localOnly {
		var err error
		config, err = readTunnelConfig(dir)
		if err != nil {
			return err
		}
		if config != nil {
			cloudflared, err = cloudflaredPath()
			if err != nil {
				return err
			}
		}
	}
	listener, err := listenMCPHTTP(address)
	if err != nil {
		return err
	}
	defer listener.Close()
	if tokenPath == "" {
		if err := initMCPToken(dir); err != nil {
			return err
		}
		tokenPath = filepath.Join(dir, mcpTokenFilename)
	} else {
		fmt.Fprintln(os.Stderr, "Authentication token file:", tokenPath)
	}
	token, err := readMCPToken(tokenPath)
	if err != nil {
		return err
	}
	service, err := acquireService(ctx, dir)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer service.stop()
	if service.done == nil {
		fmt.Fprintln(os.Stderr, "Using the running WhatsApp service.")
	}
	current := service.current
	switch {
	case current.AccountMismatch:
		fmt.Fprintln(os.Stderr, "WhatsApp account mismatch. Use a separate --data-dir for this account.")
	case current.Connected:
		fmt.Fprintln(os.Stderr, "WhatsApp connected.")
	case current.Paired:
		fmt.Fprintln(os.Stderr, "WhatsApp pairing saved; reconnecting.")
	default:
		executable, err := os.Executable()
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "To pair WhatsApp, run in another terminal:\n  %s setup --pair --data-dir %s\n", shellQuote(executable), shellQuote(dir))
	}
	fmt.Fprintln(os.Stderr, "Set the Authorization header to Bearer <contents of the token file>.")
	fmt.Fprintln(os.Stderr, "Press Ctrl+C to stop the components started by this command.")

	// Shutdown in reverse order: tunnel, HTTP, and then the owned WhatsApp
	// service. That keeps the backend available while accepted requests drain.
	httpCtx, stopHTTP := context.WithCancel(context.WithoutCancel(ctx))
	httpDone := make(chan error, 1)
	go func() {
		defer close(httpDone)
		httpDone <- serveMCPHTTP(httpCtx, dir, listener, token)
	}()
	defer func() { stopHTTP(); <-httpDone }()

	var tunnelDone <-chan error
	if config != nil {
		tunnelCtx, stopTunnel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			defer close(done)
			done <- runTunnel(tunnelCtx, cloudflared, config, token)
		}()
		tunnelDone = done
		defer func() { stopTunnel(); <-done }()
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-tunnelDone:
		if ctx.Err() != nil {
			return nil
		}
		return err
	case err := <-httpDone:
		return err
	case err := <-service.done:
		if err != nil {
			return err
		}
		return errors.New("WhatsApp service stopped")
	}
}

type runningService struct {
	current status
	done    <-chan error
	stop    func()
}

// Use an existing service if available. The caller explicitly releases any
// newly started service after its clients are done with it.
func acquireService(ctx context.Context, dir string) (runningService, error) {
	client := newLocalClient(dir)
	defer client.http.CloseIdleConnections()
	if current, err := readServiceStatus(ctx, client); err == nil {
		return runningService{current: current, stop: func() {}}, nil
	}
	if err := ctx.Err(); err != nil {
		return runningService{}, err
	}
	serviceCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- runService(serviceCtx, dir)
	}()
	release := func() { stop(); <-done }
	current, err := waitForService(ctx, client, done)
	if err != nil {
		release()
		return runningService{}, err
	}
	return runningService{current: current, done: done, stop: release}, nil
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func readServiceStatus(ctx context.Context, c *localClient) (status, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var current status
	err := c.call(ctx, "GET", "/status", nil, &current)
	return current, err
}

func waitForService(ctx context.Context, c *localClient, done <-chan error) (status, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return status{}, fmt.Errorf("waiting for the WhatsApp service: %w", ctx.Err())
		case err := <-done:
			if err == nil {
				err = errors.New("WhatsApp service stopped during startup")
			}
			return status{}, err
		case <-ticker.C:
			current, err := readServiceStatus(ctx, c)
			if err == nil {
				return current, nil
			}
		}
	}
}
