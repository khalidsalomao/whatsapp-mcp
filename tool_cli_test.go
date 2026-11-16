package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestLocalToolCLIUsesSameHandlersAndCatalogue(t *testing.T) {
	b, _ := monitoredBridge(t)
	c := serveTestSocket(t, b)
	defer c.http.CloseIdleConnections()
	ctx := context.Background()
	var out bytes.Buffer
	if err := callLocalTool(ctx, c, "list_monitors", strings.NewReader(`{"consumer_id":"codex:test"}`), &out); err != nil {
		t.Fatal(err)
	}
	var watches []monitor
	if err := json.Unmarshal(out.Bytes(), &watches); err != nil || len(watches) != 1 {
		t.Fatalf("CLI output: %s %v", out.String(), err)
	}
	for _, tc := range []struct{ tool, input string }{
		{"logout", `{}`}, {"get_messages", `{}`}, {"search_messages", `{}`},
		{"get_status", `{"unexpected":true}`}, {"get_status", `{} {}`}, {"get_status", `null`},
		{"start_monitor", `{"chat_id":"123@s.whatsapp.net","consumer_id":"other","instructions":"reply"}`},
	} {
		if err := callLocalTool(ctx, c, tc.tool, strings.NewReader(tc.input), &out); err == nil {
			t.Errorf("accepted %s %s", tc.tool, tc.input)
		}
	}
	st, ct := mcp.NewInMemoryTransports()
	ss, err := newMCP(c).Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "catalog-test", Version: "1"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	catalog, err := cs.ListTools(ctx, nil)
	if err != nil || len(catalog.Tools) != len(localToolPaths) {
		t.Fatal("MCP and CLI catalogues differ")
	}
	for _, tool := range catalog.Tools {
		if localToolPaths[tool.Name] == "" {
			t.Fatalf("missing CLI route for %s", tool.Name)
		}
	}
}
