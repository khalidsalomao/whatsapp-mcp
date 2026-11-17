package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// start owns the HTTP listener and any service it starts. An already running
// service keeps its own lifecycle, including when this command is stopped.
func runStart(ctx context.Context, dir, address, tokenPath string) error {
	listener, err := listenMCPHTTP(address)
	if err != nil {
		return err
	}
	defer listener.Close()
	if tokenPath == "" {
		if err := initMCPToken(dir); err != nil {
			return err
		}
		tokenPath = filepath.Join(dir, "mcp-http.token")
	} else {
		fmt.Fprintln(os.Stderr, "Authentication token file:", tokenPath)
	}
	token, err := readMCPToken(tokenPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	// Keep the owned service alive until HTTP requests have drained on shutdown.
	serviceCtx, stopService := context.WithCancel(context.WithoutCancel(ctx))
	var workers sync.WaitGroup
	defer func() {
		cancel()
		stopService()
		workers.Wait()
	}()
	c := newLocalClient(dir)
	defer c.http.CloseIdleConnections()
	serviceDone := make(chan error, 1)
	current, err := readServiceStatus(ctx, c)
	if err != nil {
		workers.Add(1)
		go func() {
			defer workers.Done()
			serviceDone <- runService(serviceCtx, dir)
		}()
		current, err = waitForService(ctx, c, serviceDone)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		fmt.Fprintln(os.Stderr, "Ctrl+C stops WhatsApp and this MCP endpoint.")
	} else {
		fmt.Fprintln(os.Stderr, "Using the running WhatsApp service. Ctrl+C stops this MCP endpoint.")
	}
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
		fmt.Fprintf(os.Stderr, "To pair WhatsApp, run in another terminal:\n  %s pair --open --data-dir %s\n", shellQuote(executable), shellQuote(dir))
	}
	fmt.Fprintln(os.Stderr, "Set the Authorization header to Bearer <contents of the token file>.")
	httpDone := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		httpDone <- serveMCPHTTP(ctx, dir, listener, token)
	}()
	select {
	case <-ctx.Done():
		return <-httpDone
	case err := <-httpDone:
		return err
	case err := <-serviceDone:
		if ctx.Err() != nil {
			return <-httpDone
		}
		if err != nil {
			return err
		}
		return errors.New("WhatsApp service stopped")
	}
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
