package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/skip2/go-qrcode"
)

const version = "0.4.0"

func main() {
	log.SetOutput(os.Stderr)
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "whatsapp-mcp:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		fmt.Println("whatsapp-mcp " + version + "\nCommands: serve, mcp, mcp-http, events-init, call, pair, status, logout, version\nOptions (after command): --data-dir PATH; pair also accepts --open\ncall: --tool NAME, with one JSON object on stdin; returns JSON on stdout\nmcp-http: --listen 127.0.0.1:8765 --token-file PATH\nevents-init creates a private HTTP MCP token; it does not expose a network endpoint.\nDefault data directory: ~/Library/Application Support/WhatsAppMCP on macOS\nPairing and logout are local CLI commands, never MCP tools.")
		return nil
	}
	if args[0] == "version" {
		fmt.Println(version)
		return nil
	}
	config, err := os.UserConfigDir()
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	dir := fs.String("data-dir", filepath.Join(config, "WhatsAppMCP"), "private account data directory")
	openImage := fs.Bool("open", false, "open a pairing QR image locally")
	listen := fs.String("listen", "127.0.0.1:8765", "loopback HTTP MCP listen address")
	tokenFile := fs.String("token-file", "", "private file containing the HTTP MCP bearer token")
	toolName := fs.String("tool", "", "tool name for call; parameters are read from stdin")
	if err = fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected arguments")
	}
	*dir, err = filepath.Abs(*dir)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	switch args[0] {
	case "serve":
		return runService(ctx, *dir)
	case "mcp":
		return runMCP(ctx, *dir)
	case "mcp-http":
		return runMCPHTTP(ctx, *dir, *listen, *tokenFile)
	case "events-init":
		return initMCPToken(*dir)
	case "pair":
		return runPair(ctx, *dir, *openImage)
	case "call":
		c := newLocalClient(*dir)
		defer c.http.CloseIdleConnections()
		return callLocalTool(ctx, c, *toolName, os.Stdin, os.Stdout)
	case "status", "logout":
		c := newLocalClient(*dir)
		defer c.http.CloseIdleConnections()
		method, path := "GET", "/status"
		if args[0] == "logout" {
			method, path = "POST", "/logout"
		}
		var out any
		if err = c.call(ctx, method, path, nil, &out); err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runPair(ctx context.Context, dir string, openImage bool) error {
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	c := newLocalClient(dir)
	defer c.http.CloseIdleConnections()
	var out any
	if err := c.call(ctx, "POST", "/pair", nil, &out); err != nil {
		return err
	}
	path := filepath.Join(dir, "pairing.png")
	defer os.Remove(path)
	last := ""
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return errors.New("pairing stopped or timed out; run pair again")
		case <-tick.C:
		}
		var p pairing
		if err := c.call(ctx, "GET", "/pair", nil, &p); err != nil {
			return err
		}
		switch p.State {
		case "starting", "idle":
			continue
		case "code":
			if p.Code == last {
				continue
			}
			last = p.Code
			png, err := qrcode.Encode(p.Code, qrcode.Medium, 480)
			if err != nil {
				return err
			}
			// Atomic replacement avoids partially read images and never follows a symlink.
			tmp, err := os.CreateTemp(dir, ".qr-*.png")
			if err != nil {
				return err
			}
			name := tmp.Name()
			_, err = tmp.Write(png)
			closeErr := tmp.Close()
			if err == nil {
				err = closeErr
			}
			if err == nil {
				err = os.Rename(name, path)
			}
			if err != nil {
				os.Remove(name)
				return err
			}
			fmt.Fprintln(os.Stderr, "Scan with WhatsApp → Settings → Linked Devices → Link a Device. QR image:", path)
			if openImage {
				if err = exec.Command("open", path).Run(); err != nil {
					return errors.New("could not open QR image; open pairing.png manually")
				}
			}
		case "success":
			fmt.Fprintln(os.Stderr, "WhatsApp paired successfully. History synchronization may take a few minutes.")
			return nil
		default:
			return fmt.Errorf("pairing ended (%s); run pair again", p.State)
		}
	}
}
