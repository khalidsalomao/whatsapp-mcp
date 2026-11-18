package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/term"
)

type setupOptions struct {
	pair            bool
	tunnelURL       string
	tunnelTokenFile string
	removeTunnel    bool
}

type setupPlan struct {
	pair         bool
	changeTunnel bool
	tunnel       *tunnelConfig
	copyHeader   bool
}

func runSetup(ctx context.Context, dir string, options setupOptions) error {
	plan, err := planSetup(ctx, dir, options)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("setup canceled")
		}
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if plan.changeTunnel {
		if plan.tunnel == nil {
			if err := removeTunnelConfig(dir); err != nil {
				return err
			}
			fmt.Println("Tunnel configuration removed. Restart whatsapp-mcp to apply the change.")
		} else {
			if err := initMCPToken(dir); err != nil {
				return err
			}
			if err := saveTunnelConfig(dir, plan.tunnel); err != nil {
				return err
			}
			fmt.Println("Cloudflare tunnel configuration saved.")
		}
	}
	if plan.pair {
		if err := pairDuringSetup(ctx, dir); err != nil {
			return err
		}
	}
	if plan.tunnel != nil {
		path := filepath.Join(dir, mcpTokenFilename)
		fmt.Println("Grok Bot connector type: Remote HTTPS")
		fmt.Println("Server URL:", plan.tunnel.URL)
		fmt.Println("HTTP header: Authorization")
		fmt.Println("Header value: Bearer <contents of", path+">")
		fmt.Println("Leave Client ID empty.")
		if plan.copyHeader {
			token, err := readMCPToken(path)
			if err != nil {
				return err
			}
			copy := exec.CommandContext(ctx, "/usr/bin/pbcopy")
			copy.Stdin = strings.NewReader("Bearer " + token)
			if err := copy.Run(); err != nil {
				return fmt.Errorf("copy MCP header: %w", err)
			}
			fmt.Println("Authorization value copied. Paste it into the connector's Value field.")
		}
		if _, err := cloudflaredPath(); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
	fmt.Println("Run whatsapp-mcp start to bring the configured services online.")
	return nil
}

func planSetup(ctx context.Context, dir string, options setupOptions) (setupPlan, error) {
	if options == (setupOptions{}) {
		return interactiveSetup(ctx, dir)
	}
	plan := setupPlan{pair: options.pair, changeTunnel: options.removeTunnel}
	if options.removeTunnel && (options.tunnelURL != "" || options.tunnelTokenFile != "") {
		return plan, errors.New("--remove-tunnel cannot be combined with tunnel URL or token options")
	}
	if (options.tunnelURL == "") != (options.tunnelTokenFile == "") {
		return plan, errors.New("use --tunnel-url and --tunnel-token-file together, or run setup in a terminal")
	}
	if options.tunnelURL != "" {
		secret, err := readPrivateFile(options.tunnelTokenFile, maxTunnelTokenBytes)
		if err != nil {
			return plan, fmt.Errorf("read Cloudflare connector token: %w", err)
		}
		cfg, err := newTunnelConfig(options.tunnelURL, string(secret))
		if err != nil {
			return plan, err
		}
		plan.tunnel, plan.changeTunnel = cfg, true
	}
	return plan, nil
}

func interactiveSetup(ctx context.Context, dir string) (setupPlan, error) {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		return setupPlan{}, errors.New("setup needs an interactive terminal; for scripts, see setup --help")
	}
	cfg, err := readTunnelConfig(dir)
	if err != nil {
		return setupPlan{}, fmt.Errorf("%w; use setup --tunnel-url with --tunnel-token-file to replace the settings, or setup --remove-tunnel to clear them", err)
	}
	plan := setupPlan{tunnel: cfg}
	client := newLocalClient(dir)
	current, statusErr := readServiceStatus(ctx, client)
	client.http.CloseIdleConnections()
	if statusErr != nil {
		fmt.Println("WhatsApp service unavailable. The pairing step will check for any saved session.")
	} else if current.Paired {
		fmt.Println("WhatsApp already paired.")
	}
	if cfg != nil {
		fmt.Println("Cloudflare URL:", cfg.URL)
	}

	original, err := term.MakeRaw(fd)
	if err != nil {
		return plan, err
	}
	defer term.Restore(fd, original)
	terminal := term.NewTerminal(struct {
		io.Reader
		io.Writer
	}{os.Stdin, os.Stderr}, "")
	if width, height, err := term.GetSize(fd); err == nil && width > 0 && height > 0 {
		_ = terminal.SetSize(width, height)
	}
	prompt := func(label string) (string, error) {
		terminal.SetPrompt(label)
		answer, err := terminal.ReadLine()
		return strings.TrimSpace(answer), err
	}
	if !current.Paired {
		answer, err := prompt("Pair WhatsApp now? [y/N] ")
		if err != nil {
			return plan, err
		}
		plan.pair = strings.EqualFold(answer, "y") || strings.EqualFold(answer, "yes")
	}
	question := "Configure a named Cloudflare Tunnel? [y/N] "
	if cfg != nil {
		question = "Cloudflare Tunnel settings: [K]eep, [c]hange, [r]emove? "
	}
	answer, err := prompt(question)
	if err != nil {
		return plan, err
	}
	answer = strings.ToLower(answer)
	if (cfg == nil && (answer == "y" || answer == "yes")) || (cfg != nil && answer == "c") {
		fmt.Fprintf(terminal, "In Cloudflare, create a tunnel with a published hostname.\nSet its service URL to http://%s and its HTTP Host Header to %s.\n", defaultMCPListen, defaultMCPListen)
		endpoint, err := prompt("Published HTTPS URL: ")
		if err != nil {
			return plan, err
		}
		endpoint, err = normalizeTunnelURL(endpoint)
		if err != nil {
			return plan, err
		}
		token, err := terminal.ReadPassword("Cloudflare connector token (input hidden): ")
		if err != nil {
			return plan, err
		}
		plan.tunnel, err = newTunnelConfig(endpoint, token)
		if err != nil {
			return plan, err
		}
		plan.changeTunnel = true
	} else if cfg != nil && answer == "r" {
		plan.tunnel, plan.changeTunnel = nil, true
	}
	if plan.tunnel != nil && runtime.GOOS == "darwin" {
		answer, err := prompt("Copy the MCP Authorization value to the clipboard? [y/N] ")
		if err != nil {
			return plan, err
		}
		plan.copyHeader = strings.EqualFold(answer, "y") || strings.EqualFold(answer, "yes")
	}
	return plan, nil
}

func pairDuringSetup(ctx context.Context, dir string) error {
	service, err := acquireService(ctx, dir)
	if err != nil {
		return err
	}
	defer service.stop()
	if service.current.AccountMismatch {
		return errors.New("WhatsApp account mismatch; use a separate --data-dir for this account")
	}
	if service.current.Paired {
		fmt.Println("WhatsApp pairing already saved.")
		return nil
	}
	return runPair(ctx, dir, runtime.GOOS == "darwin")
}
