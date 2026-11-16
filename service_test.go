package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

func testBridge(t *testing.T) *bridge {
	t.Helper()
	a := testArchive(t)
	if err := a.saveContact(contact{ID: "123@s.whatsapp.net", Name: "Test Contact"}); err != nil {
		t.Fatal(err)
	}
	return &bridge{a: a, ctx: context.Background(), connectedFn: func() bool { return true }, sendFn: func(context.Context, types.JID, *waE2E.Message, whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		return whatsmeow.SendResponse{Timestamp: time.Now()}, nil
	}}
}

func TestSendExactlyOnceOnConcurrentRetries(t *testing.T) {
	b := testBridge(t)
	var calls atomic.Int32
	b.sendFn = func(_ context.Context, j types.JID, m *waE2E.Message, e whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		calls.Add(1)
		if j.String() != "123@s.whatsapp.net" || m.GetConversation() != "hello" || e.ID == "" {
			t.Error("wrong outbound arguments")
		}
		return whatsmeow.SendResponse{Timestamp: time.Now()}, nil
	}
	in := sendInput{Recipient: "123@s.whatsapp.net", outboundContent: outboundContent{Text: "hello"}, RequestID: "same-request-1"}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := b.send(context.Background(), in)
			if err != nil || out.State != "acknowledged" {
				t.Errorf("send: %+v %v", out, err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("sent %d times", calls.Load())
	}
	in.Text = "different"
	if _, err := b.send(context.Background(), in); err == nil {
		t.Fatal("request ID reused for different message")
	}
}

func TestUncertainSendNeverAutomaticallyRetries(t *testing.T) {
	b := testBridge(t)
	var calls int
	b.sendFn = func(context.Context, types.JID, *waE2E.Message, whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		calls++
		return whatsmeow.SendResponse{}, context.DeadlineExceeded
	}
	in := sendInput{Recipient: "123@s.whatsapp.net", outboundContent: outboundContent{Text: "hello"}, RequestID: "uncertain-1"}
	for i := 0; i < 2; i++ {
		out, err := b.send(context.Background(), in)
		if err != nil || out.State != "uncertain" {
			t.Fatalf("uncertain send: %+v %v", out, err)
		}
	}
	if calls != 1 {
		t.Fatal("uncertain send retried")
	}
}

func TestSendValidatesBeforeNetwork(t *testing.T) {
	b := testBridge(t)
	b.sendFn = func(context.Context, types.JID, *waE2E.Message, whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		t.Fatal("unexpected send")
		return whatsmeow.SendResponse{}, nil
	}
	for _, recipient := range []string{"John", "+5511999999999", "status@broadcast", "123:1@s.whatsapp.net", "123_0:0@s.whatsapp.net", "999@s.whatsapp.net", "abc@g.us"} {
		_, err := b.send(context.Background(), sendInput{Recipient: recipient, outboundContent: outboundContent{Text: "hello"}, RequestID: "validate-1"})
		if err == nil {
			t.Fatalf("accepted %s", recipient)
		}
	}
	b.connectedFn = func() bool { return false }
	_, err := b.send(context.Background(), sendInput{Recipient: "123@s.whatsapp.net", outboundContent: outboundContent{Text: "hello"}, RequestID: "offline-1"})
	if err == nil {
		t.Fatal("offline send accepted")
	}
	var count int
	if err = b.a.db.QueryRow("SELECT COUNT(*) FROM outbox").Scan(&count); err != nil || count != 0 {
		t.Fatal("validation reserved request")
	}
}

func TestReplyAndDisappearingTimer(t *testing.T) {
	b := testBridge(t)
	m := fixture("original")
	mustSave(t, b.a, m)
	if err := b.a.saveTimer(m.ChatID, 86400, 10); err != nil {
		t.Fatal(err)
	}
	b.sendFn = func(_ context.Context, _ types.JID, m *waE2E.Message, _ whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		ci := m.GetExtendedTextMessage().GetContextInfo()
		if ci.GetStanzaID() != "original" || ci.GetParticipant() != "123@s.whatsapp.net" || ci.GetExpiration() != 86400 {
			t.Errorf("invalid reply context: %v", ci)
		}
		return whatsmeow.SendResponse{Timestamp: time.Now()}, nil
	}
	out, err := b.send(context.Background(), sendInput{Recipient: m.ChatID, outboundContent: outboundContent{Text: "reply"}, RequestID: "reply-0001", ReplyTo: m.ID})
	if err != nil {
		t.Fatal(err)
	}
	sent, err := b.a.getMessage(context.Background(), m.ChatID, out.MessageID)
	if err != nil || sent.Expires == 0 {
		t.Fatal("outgoing expiration lost")
	}
	_, err = b.send(context.Background(), sendInput{Recipient: m.ChatID, outboundContent: outboundContent{Text: "reply"}, RequestID: "reply-0002", ReplyTo: "not-in-this-chat"})
	if err == nil {
		t.Fatal("unknown quote accepted")
	}
}

func TestPrivateHTTPRejectsOriginsAndMalformedBodies(t *testing.T) {
	b := testBridge(t)
	for _, test := range []struct {
		host, origin, body string
		code               int
	}{
		{"evil", "", `{}`, 403}, {"local", "https://evil.example", `{}`, 403},
		{"local", "", `{"unknown":1}`, 400}, {"local", "", `{} {}`, 400}, {"local", "", strings.Repeat("x", 70000), 400},
		{"local", "", `{}`, 200},
	} {
		r := httptest.NewRequest("POST", "http://"+test.host+"/messages", strings.NewReader(test.body))
		r.Header.Set("Origin", test.origin)
		w := httptest.NewRecorder()
		b.handler().ServeHTTP(w, r)
		if w.Code != test.code {
			t.Fatalf("HTTP %d, wanted %d: %s", w.Code, test.code, w.Body.String())
		}
	}
}

func serveTestSocket(t *testing.T, b *bridge) *localClient {
	t.Helper()
	return newLocalClient(serveTestSocketDir(t, b))
}

func serveTestSocketDir(t *testing.T, b *bridge) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "wamcp-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	listener, err := net.Listen("unix", filepath.Join(dir, "bridge.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: b.handler()}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	return dir
}

func TestMCPWireDiscoveryAndTools(t *testing.T) {
	b := testBridge(t)
	mustSave(t, b.a, fixture("tool-message"))
	local := serveTestSocket(t, b)
	defer local.http.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv := newMCP(local)
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := srv.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer serverSession.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "integration-test", Version: "1"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	list, err := session.ListTools(ctx, nil)
	if err != nil || len(list.Tools) != 15 {
		t.Fatalf("tools: %+v %v", list, err)
	}
	for _, tool := range list.Tools {
		switch tool.Name {
		case "send_message", "start_monitor", "stop_monitor", "claim_reply_jobs", "draft_reply", "send_reply", "skip_reply":
			if tool.Annotations.ReadOnlyHint {
				t.Fatalf("mutation declared read-only: %s", tool.Name)
			}
		default:
			if !tool.Annotations.ReadOnlyHint {
				t.Fatalf("read tool missing annotation: %s", tool.Name)
			}
		}
	}
	for _, test := range []struct {
		name string
		args map[string]any
		fail bool
	}{
		{"get_status", map[string]any{}, false},
		{"search_contacts", map[string]any{"query": "Test"}, false},
		{"list_chats", map[string]any{}, false},
		{"get_messages", map[string]any{"chat_id": "123@s.whatsapp.net"}, false},
		{"get_message", map[string]any{"chat_id": "123@s.whatsapp.net", "message_id": "tool-message"}, false},
		{"get_message", map[string]any{"chat_id": "123@s.whatsapp.net", "message_id": "absent"}, true},
		{"search_messages", map[string]any{"query": "hello"}, false},
		{"get_messages", map[string]any{}, true},
		{"search_messages", map[string]any{}, true},
		{"send_message", map[string]any{"recipient_id": "123@s.whatsapp.net", "text": "wire test", "request_id": "wire-test-01"}, false},
	} {
		out, err := session.CallTool(ctx, &mcp.CallToolParams{Name: test.name, Arguments: test.args})
		if err != nil {
			t.Fatalf("%s transport: %v", test.name, err)
		}
		if out.IsError != test.fail {
			t.Fatalf("%s IsError=%v: %+v", test.name, out.IsError, out)
		}
	}
}

func TestUnavailableDaemonReturnsActionableError(t *testing.T) {
	c := newLocalClient("/tmp/does-not-exist-whatsapp-mcp")
	var out status
	err := c.call(context.Background(), "GET", "/status", nil, &out)
	if err == nil || !strings.Contains(err.Error(), "service unavailable") {
		t.Fatalf("unhelpful error: %v", err)
	}
}

func TestPrivateDirectoryRejectsSymlinks(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := privateDir(link); err == nil {
		t.Fatal("accepted symlink data directory")
	}
	if err := os.Symlink(filepath.Join(dir, "unrelated"), filepath.Join(target, "messages.db")); err != nil {
		t.Fatal(err)
	}
	if err := privateDir(target); err == nil {
		t.Fatal("accepted symlink database")
	}
}

func TestLocalClientDoesNotFollowRedirects(t *testing.T) {
	// No TCP listener is ever used by the local client. Even a redirect dials its configured socket.
	c := newLocalClient("/tmp/does-not-exist-whatsapp-mcp")
	_, err := c.http.Get("http://example.com/status")
	if err == nil {
		t.Fatal("expected socket connection failure")
	}
	if errors.Is(err, io.EOF) {
		t.Fatal("unexpected remote response")
	}
}
