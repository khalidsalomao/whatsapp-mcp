package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.mau.fi/whatsmeow/types/events"
)

type eventRoundTripper func(*http.Request) (*http.Response, error)

func (f eventRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type capturedWebhook struct {
	ID, Subscription, Signature, Stamp string
	Body                               []byte
}
type fakeEventReceiver struct {
	mu                   sync.Mutex
	verifications        int
	verificationRequests []capturedWebhook
	events               []capturedWebhook
	status               int
	wrongChallenge       bool
	largeAck             bool
}

func (f *fakeEventReceiver) client(t *testing.T) *http.Client {
	t.Helper()
	return &http.Client{Transport: eventRoundTripper(func(r *http.Request) (*http.Response, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		var control struct{ Type, Challenge string }
		if err = json.Unmarshal(body, &control); err != nil {
			t.Errorf("bad webhook JSON: %v", err)
		}
		status, response := 200, `{}`
		if control.Type == "verification" {
			f.verifications++
			f.verificationRequests = append(f.verificationRequests, capturedWebhook{ID: r.Header.Get("webhook-id"), Subscription: r.Header.Get("X-MCP-Subscription-Id"), Signature: r.Header.Get("webhook-signature"), Stamp: r.Header.Get("webhook-timestamp"), Body: body})
			if control.Challenge == "" {
				t.Error("empty challenge")
			}
			if f.wrongChallenge {
				control.Challenge = "wrong"
			}
			b, _ := json.Marshal(map[string]string{"challenge": control.Challenge})
			response = string(b)
		} else {
			f.events = append(f.events, capturedWebhook{ID: r.Header.Get("webhook-id"), Subscription: r.Header.Get("X-MCP-Subscription-Id"), Signature: r.Header.Get("webhook-signature"), Stamp: r.Header.Get("webhook-timestamp"), Body: body})
			if f.status != 0 {
				status = f.status
			}
			if f.largeAck {
				response = strings.Repeat("x", 5000)
			}
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response)), Request: r}, nil
	})}
}

func testEventHub(t *testing.T) (*eventHub, *fakeEventReceiver) {
	t.Helper()
	a := testArchive(t)
	if err := a.setMeta("account", "999@s.whatsapp.net"); err != nil {
		t.Fatal(err)
	}
	if err := a.saveContact(contact{ID: "123@s.whatsapp.net"}); err != nil {
		t.Fatal(err)
	}
	h := newEventHub(a, func() bool { return true })
	receiver := &fakeEventReceiver{}
	h.http = receiver.client(t)
	return h, receiver
}
func eventInput(chat string) *eventParams {
	args, _ := json.Marshal(eventArguments{ChatID: chat})
	return &eventParams{Name: messageEventName, Arguments: args, Delivery: eventDestination{Mode: "webhook", URL: "https://receiver.example.com/callback", Secret: "whsec_" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{42}, 32))}}
}
func subscribeForTest(t *testing.T, h *eventHub, in *eventParams) subscribeResult {
	t.Helper()
	reply := h.call(context.Background(), "subscribe", in)
	if reply.Error != nil {
		t.Fatal(reply.Error)
	}
	var out subscribeResult
	if err := json.Unmarshal(reply.Result, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func eventCount(t *testing.T, a *archive, table string) int {
	t.Helper()
	var count int
	if table != "event_deliveries" && table != "event_subscriptions" {
		t.Fatal("bad test table")
	}
	if err := a.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
func saveLive(t *testing.T, a *archive, m message) {
	t.Helper()
	if err := a.saveObserved(m, true); err != nil {
		t.Fatal(err)
	}
}
func deliverForTest(t *testing.T, h *eventHub) bool {
	t.Helper()
	more, err := h.deliverOne(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return more
}
func expectSignature(t *testing.T, event capturedWebhook, secret string) {
	t.Helper()
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, "whsec_"))
	if err != nil {
		t.Fatal(err)
	}
	m := hmac.New(sha256.New, key)
	fmt.Fprintf(m, "%s.%s.", event.ID, event.Stamp)
	m.Write(event.Body)
	want := "v1," + base64.StdEncoding.EncodeToString(m.Sum(nil))
	for _, signature := range strings.Fields(event.Signature) {
		if hmac.Equal([]byte(signature), []byte(want)) {
			return
		}
	}
	t.Fatal("Standard Webhooks signature does not cover the exact ID, timestamp and body")
}

func TestEventsSubscriptionLifecycleAndRotation(t *testing.T) {
	h, r := testEventHub(t)
	in := eventInput("123@s.whatsapp.net")
	in.TTL = json.RawMessage("60000")
	before := time.Now()
	first := subscribeForTest(t, h, in)
	expectSignature(t, r.verificationRequests[0], in.Delivery.Secret)
	expiry, err := time.Parse(time.RFC3339Nano, first.RefreshBefore)
	if err != nil || expiry.Before(before) || expiry.After(time.Now().Add(time.Minute)) {
		t.Fatal("requested TTL was not honored")
	}
	second := subscribeForTest(t, h, in)
	if first.ID != second.ID || eventCount(t, h.a, "event_subscriptions") != 1 || r.verifications != 1 {
		t.Fatal("subscribe not idempotent or verification cache missing")
	}
	oldSecret := in.Delivery.Secret
	in.Delivery.Secret = "whsec_" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{43}, 32))
	if subscribeForTest(t, h, in).ID != first.ID || r.verifications != 2 {
		t.Fatal("rotation changed identity or skipped verification")
	}
	saveLive(t, h.a, fixture("rotated"))
	deliverForTest(t, h)
	if len(r.events) != 1 {
		t.Fatal("event not delivered")
	}
	expectSignature(t, r.events[0], oldSecret)
	expectSignature(t, r.events[0], in.Delivery.Secret)
	if r.events[0].Subscription != first.ID {
		t.Fatal("wrong subscription header")
	}
	if bytes.Contains(r.events[0].Body, []byte("hello")) || bytes.Contains(r.events[0].Body, []byte(`"text":`)) {
		t.Fatal("event copied message content")
	}
	var event messageEvent
	if err = json.Unmarshal(r.events[0].Body, &event); err != nil || event.ID != r.events[0].ID || event.Name != messageEventName || event.Data.MessageID != "rotated" || event.Cursor != nil {
		t.Fatal("invalid event envelope")
	}
	// Unsubscribing never needs the signing secret and is idempotent.
	in.Delivery.Secret = ""
	for i := 0; i < 2; i++ {
		if reply := h.call(context.Background(), "unsubscribe", in); reply.Error != nil {
			t.Fatal(reply.Error)
		}
	}
	saveLive(t, h.a, fixture("after-unsubscribe"))
	if eventCount(t, h.a, "event_subscriptions") != 0 || eventCount(t, h.a, "event_deliveries") != 0 {
		t.Fatal("unsubscribe retained active work")
	}
}

func TestEventsValidationAndFailedVerification(t *testing.T) {
	h, r := testEventHub(t)
	for _, mutate := range []func(*eventParams){
		func(p *eventParams) { p.Name = "unknown" }, func(p *eventParams) {
			p.Arguments = json.RawMessage(`{"chat_id":"123@s.whatsapp.net","wildcard":true}`)
		},
		func(p *eventParams) { p.Arguments = json.RawMessage(`{"chat_id":"99999@s.whatsapp.net"}`) },
		func(p *eventParams) { p.Delivery.Secret = "whsec_eA==" }, func(p *eventParams) { p.TTL = json.RawMessage("0") },
		func(p *eventParams) { p.TTL = json.RawMessage("1.5") }, func(p *eventParams) { v := "old"; p.Cursor = &v },
		func(p *eventParams) { p.Delivery.URL = "http://receiver.example.com/callback" },
	} {
		p := eventInput("123@s.whatsapp.net")
		mutate(p)
		if reply := h.call(context.Background(), "subscribe", p); reply.Error == nil || reply.Error.Code != -32602 {
			t.Fatal("invalid subscription accepted")
		}
	}
	if r.verifications != 0 {
		t.Fatal("invalid request reached callback")
	}
	r.wrongChallenge = true
	if reply := h.call(context.Background(), "subscribe", eventInput("123@s.whatsapp.net")); reply.Error == nil || reply.Error.Code != -32015 {
		t.Fatal("unverified callback accepted")
	}
	if eventCount(t, h.a, "event_subscriptions") != 0 {
		t.Fatal("failed verification was persisted")
	}
	h.active = func() bool { return false }
	if reply := h.call(context.Background(), "subscribe", eventInput("123@s.whatsapp.net")); reply.Error == nil {
		t.Fatal("subscribed while disconnected")
	}
}

func TestEventsOnlyNewIncomingMessagesAndAtomicCommit(t *testing.T) {
	h, _ := testEventHub(t)
	subscribeForTest(t, h, eventInput("123@s.whatsapp.net"))
	for i, change := range []func(*message){
		func(m *message) { m.FromMe = true }, func(m *message) { m.Deleted = true }, func(m *message) { m.Edited = true },
		func(m *message) { m.Expires = time.Now().Unix() - 1 }, func(m *message) { m.ChatID = "777@s.whatsapp.net" },
	} {
		m := fixture(fmt.Sprintf("filtered-%d", i))
		change(&m)
		saveLive(t, h.a, m)
	}
	mustSave(t, h.a, fixture("history"))
	saveLive(t, h.a, fixture("history"))
	if eventCount(t, h.a, "event_deliveries") != 0 {
		t.Fatal("non-new content triggered an event")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := h.a.saveObserved(fixture("concurrent"), true); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if eventCount(t, h.a, "event_deliveries") != 1 {
		t.Fatal("duplicate delivery reserved more than once")
	}
	if _, err := h.a.db.Exec(`CREATE TRIGGER fail_event BEFORE INSERT ON event_deliveries BEGIN SELECT RAISE(ABORT,'synthetic failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := h.a.saveObserved(fixture("must-rollback"), true); err == nil {
		t.Fatal("event enqueue failure ignored")
	}
	var n int
	if err := h.a.db.QueryRow("SELECT COUNT(*) FROM messages WHERE id='must-rollback'").Scan(&n); err != nil || n != 0 {
		t.Fatal("message committed without its event")
	}
	if _, err := h.a.db.Exec("DROP TRIGGER fail_event"); err != nil {
		t.Fatal(err)
	}
	saveLive(t, h.a, fixture("must-rollback"))
	if eventCount(t, h.a, "event_deliveries") != 2 {
		t.Fatal("retry after rollback was lost")
	}
}

func TestEventsIngestExcludesHistoryAndOwnEcho(t *testing.T) {
	b, _, _ := contactSyncBridge(t)
	if !b.allowedAccount() {
		t.Fatal("fixture account not bound")
	}
	if err := b.a.saveContact(contact{ID: "123@s.whatsapp.net"}); err != nil {
		t.Fatal(err)
	}
	r := &fakeEventReceiver{}
	b.connectedFn = func() bool { return true }
	b.eventHub.http = r.client(t)
	subscribeForTest(t, b.eventHub, eventInput("123@s.whatsapp.net"))
	e := eventFixture()
	e.Info.ID = "history"
	b.ingestObserved(e, false)
	e = eventFixture()
	e.Info.ID = "own"
	e.Info.IsFromMe = true
	b.event(e)
	e = eventFixture()
	e.Info.ID = "live"
	b.event(e)
	b.event(e)
	if eventCount(t, b.a, "event_deliveries") != 1 {
		t.Fatal("ingestion emitted history, own echo, or duplicate")
	}
	b.event(&events.LoggedOut{})
	if b.eventHub.active() {
		t.Fatal("revoked session still authorizes event delivery")
	}
	if eventCount(t, b.a, "event_subscriptions") != 0 || eventCount(t, b.a, "event_deliveries") != 0 {
		t.Fatal("revocation retained subscriptions")
	}
}

func TestEventsPersistRetryAndKeepPayloadStable(t *testing.T) {
	dir := t.TempDir()
	a, err := openArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.setMeta("account", "999@s.whatsapp.net"); err != nil {
		t.Fatal(err)
	}
	if err = a.saveContact(contact{ID: "123@s.whatsapp.net"}); err != nil {
		t.Fatal(err)
	}
	r := &fakeEventReceiver{status: 503}
	h := newEventHub(a, func() bool { return true })
	h.http = r.client(t)
	in := eventInput("123@s.whatsapp.net")
	sub := subscribeForTest(t, h, in)
	saveLive(t, a, fixture("restart"))
	deliverForTest(t, h)
	if len(r.events) != 1 || deliverForTest(t, h) {
		t.Fatal("retry ignored its backoff")
	}
	if err = a.db.Close(); err != nil {
		t.Fatal(err)
	}
	a, err = openArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer a.db.Close()
	h = newEventHub(a, func() bool { return true })
	h.http = r.client(t)
	if eventCount(t, a, "event_subscriptions") != 1 || eventCount(t, a, "event_deliveries") != 1 {
		t.Fatal("pending state did not survive restart")
	}
	if _, err = a.db.Exec("UPDATE event_deliveries SET next_attempt=0"); err != nil {
		t.Fatal(err)
	}
	r.status = 204
	deliverForTest(t, h)
	if len(r.events) != 2 || r.events[1].Subscription != sub.ID || r.events[1].ID != r.events[0].ID || !bytes.Equal(r.events[0].Body, r.events[1].Body) {
		t.Fatal("retry identity or payload changed")
	}
	expectSignature(t, r.events[1], in.Delivery.Secret)
	if eventCount(t, a, "event_deliveries") != 0 {
		t.Fatal("acknowledged event still pending")
	}
	// Replayed inbound data after restart must not recreate a completed event.
	saveLive(t, a, fixture("restart"))
	if eventCount(t, a, "event_deliveries") != 0 {
		t.Fatal("restart lost inbound deduplication")
	}
}

func TestEventsSuppressDeletedExpiredAndClearedMessages(t *testing.T) {
	for _, mode := range []string{"deleted", "expired", "cleared"} {
		t.Run(mode, func(t *testing.T) {
			h, r := testEventHub(t)
			subscribeForTest(t, h, eventInput("123@s.whatsapp.net"))
			m := fixture("unavailable")
			saveLive(t, h.a, m)
			switch mode {
			case "deleted":
				m.Deleted = true
				mustSave(t, h.a, m)
			case "expired":
				if _, err := h.a.db.Exec("UPDATE messages SET expires=?", time.Now().Unix()-1); err != nil {
					t.Fatal(err)
				}
			case "cleared":
				if err := h.a.removeChat(m.ChatID, time.Now(), nil, false); err != nil {
					t.Fatal(err)
				}
			}
			deliverForTest(t, h)
			if len(r.events) != 0 || eventCount(t, h.a, "event_deliveries") != 0 {
				t.Fatal("unavailable content triggered an event")
			}
		})
	}
}

func TestEventsAliasMigrationAndSubscriptionRefresh(t *testing.T) {
	h, r := testEventHub(t)
	lid := "456@lid"
	if err := h.a.saveContact(contact{ID: lid}); err != nil {
		t.Fatal(err)
	}
	in := eventInput(lid)
	sub := subscribeForTest(t, h, in)
	m := fixture("aliased")
	m.ChatID = lid
	m.Sender = lid
	saveLive(t, h.a, m)
	if err := h.a.alias(lid, "123@s.whatsapp.net"); err != nil {
		t.Fatal(err)
	}
	if subscribeForTest(t, h, in).ID != sub.ID {
		t.Fatal("alias migration changed subscription identity")
	}
	deliverForTest(t, h)
	if len(r.events) != 1 {
		t.Fatal("pending event lost in alias migration")
	}
	var event messageEvent
	if err := json.Unmarshal(r.events[0].Body, &event); err != nil {
		t.Fatal(err)
	}
	got, err := h.a.getMessage(context.Background(), event.Data.ChatID, event.Data.MessageID)
	if err != nil || got.ChatID != "123@s.whatsapp.net" || got.Text != "hello" {
		t.Fatal("event IDs no longer resolve")
	}
	saveLive(t, h.a, fixture("after-alias"))
	deliverForTest(t, h)
	if len(r.events) != 2 {
		t.Fatal("subscription filter did not migrate")
	}
}

func TestEventsExpiryDisconnectAndTerminalResponses(t *testing.T) {
	for _, status := range []int{410, 413, 400, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			h, r := testEventHub(t)
			r.status = status
			subscribeForTest(t, h, eventInput("123@s.whatsapp.net"))
			saveLive(t, h.a, fixture("failure"))
			for i := 0; i < 10; i++ {
				if _, err := h.a.db.Exec("UPDATE event_deliveries SET next_attempt=0"); err != nil {
					t.Fatal(err)
				}
				deliverForTest(t, h)
			}
			want := 1
			if status == 503 {
				want = 8
			}
			if len(r.events) != want {
				t.Fatalf("got %d attempts, want %d", len(r.events), want)
			}
			if status == 410 && eventCount(t, h.a, "event_subscriptions") != 0 {
				t.Fatal("410 did not stop subscription")
			}
		})
	}
	h, r := testEventHub(t)
	subscribeForTest(t, h, eventInput("123@s.whatsapp.net"))
	saveLive(t, h.a, fixture("paused"))
	h.active = func() bool { return false }
	deliverForTest(t, h)
	if len(r.events) != 0 || eventCount(t, h.a, "event_deliveries") != 1 {
		t.Fatal("disconnection lost or delivered pending work")
	}
	h.active = func() bool { return true }
	if _, err := h.a.db.Exec("UPDATE event_subscriptions SET expires=0"); err != nil {
		t.Fatal(err)
	}
	deliverForTest(t, h)
	if len(r.events) != 0 || eventCount(t, h.a, "event_deliveries") != 0 {
		t.Fatal("expired subscription delivered")
	}
}

func TestEventsCallbackNetworkBoundaries(t *testing.T) {
	for _, address := range []string{"127.0.0.1", "10.0.0.1", "172.16.1.2", "192.168.1.1", "169.254.169.254", "100.64.0.1", "0.0.0.0", "192.0.2.1", "198.18.0.1", "224.0.0.1", "240.0.0.1", "::1", "::ffff:127.0.0.1", "fe80::1", "fc00::1", "64:ff9b::a00:1", "2001:db8::1", "2002:a00:1::1"} {
		if publicCallbackIP(netip.MustParseAddr(address)) {
			t.Fatalf("non-public address accepted: %s", address)
		}
	}
	for _, address := range []string{"1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"} {
		if !publicCallbackIP(netip.MustParseAddr(address)) {
			t.Fatalf("public address rejected: %s", address)
		}
	}
	for _, raw := range []string{"http://example.com/x", "https://user:pass@example.com/x", "https://127.0.0.1/x", "https://localhost/x", "https://a.local/x", "https://[::1]/x", "https://example.com:8080/x", "https://example.com/x#fragment"} {
		if validateCallback(raw) == nil {
			t.Fatalf("unsafe callback accepted: %s", raw)
		}
	}
	c := webhookClient()
	defer c.CloseIdleConnections()
	if c.Transport.(*http.Transport).Proxy != nil || !c.Transport.(*http.Transport).DisableKeepAlives {
		t.Fatal("callback transport can bypass per-connection DNS checks")
	}
	if c.CheckRedirect(nil, nil) == nil {
		t.Fatal("callback redirects followed")
	}
	if _, err := c.Transport.(*http.Transport).DialContext(context.Background(), "tcp", "127.0.0.1:443"); err == nil {
		t.Fatal("dial accepted loopback")
	}
}

func TestEventsHTTPDiscoveryAndSubscriptionWire(t *testing.T) {
	b := testBridge(t)
	if err := b.a.setMeta("account", "999@s.whatsapp.net"); err != nil {
		t.Fatal(err)
	}
	r := &fakeEventReceiver{}
	b.eventHub = newEventHub(b.a, func() bool { return true })
	b.eventHub.http = r.client(t)
	c := serveTestSocket(t, b)
	defer c.http.CloseIdleConnections()
	token := strings.Repeat("a", 40)
	s := httptest.NewServer(authenticatedMCP(c, token))
	defer s.Close()
	rpc := func(method string, params any, auth, origin, version string) (int, eventReply) {
		t.Helper()
		if version == "2026-07-28" {
			encoded, _ := json.Marshal(params)
			var p map[string]any
			if err := json.Unmarshal(encoded, &p); err != nil {
				t.Fatal(err)
			}
			p["_meta"] = map[string]any{mcp.MetaKeyProtocolVersion: version, mcp.MetaKeyClientInfo: map[string]string{"name": "wire-test", "version": "1"}, mcp.MetaKeyClientCapabilities: map[string]any{}}
			params = p
		}
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		req, _ := http.NewRequest("POST", s.URL+"/mcp", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		if version != "" {
			req.Header.Set("MCP-Protocol-Version", version)
		}
		if version == "2026-07-28" {
			req.Header.Set("Mcp-Method", method)
			if method == "tools/call" {
				req.Header.Set("Mcp-Name", params.(map[string]any)["name"].(string))
			}
		}
		resp, err := s.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		var out eventReply
		if strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") && json.Unmarshal(data, &out) != nil {
			t.Fatalf("invalid HTTP MCP JSON: %s", data)
		}
		return resp.StatusCode, out
	}
	if code, _ := rpc("events/list", map[string]any{}, "", "", ""); code != 401 {
		t.Fatal("HTTP endpoint did not require authentication")
	}
	if code, _ := rpc("events/list", map[string]any{}, "Bearer "+token, "https://evil.example", ""); code != 403 {
		t.Fatal("browser origin accepted")
	}
	for _, version := range []string{"2025-06-18", "2026-07-28"} {
		t.Run(version, func(t *testing.T) {
			method, params := "server/discover", map[string]any{}
			if version == "2025-06-18" {
				method = "initialize"
				params = map[string]any{"protocolVersion": version, "capabilities": map[string]any{}, "clientInfo": map[string]string{"name": "test", "version": "1"}}
			}
			code, reply := rpc(method, params, "Bearer "+token, "", version)
			var discovered struct {
				Capabilities map[string]json.RawMessage `json:"capabilities"`
			}
			if code != 200 || reply.Error != nil || json.Unmarshal(reply.Result, &discovered) != nil || discovered.Capabilities["events"] == nil || discovered.Capabilities["tools"] == nil {
				t.Fatalf("events capability missing: code=%d reply=%+v", code, reply)
			}
			code, reply = rpc("events/list", map[string]any{}, "Bearer "+token, "", version)
			if code != 200 || reply.Error != nil || !bytes.Contains(reply.Result, []byte(messageEventName)) {
				t.Fatalf("events/list failed: %d %+v", code, reply)
			}
			in := eventInput("123@s.whatsapp.net")
			code, reply = rpc("events/subscribe", in, "Bearer "+token, "", version)
			if code != 200 || reply.Error != nil {
				t.Fatalf("subscribe failed: %d %+v", code, reply)
			}
			saveLive(t, b.a, fixture("http-"+version))
			deliverForTest(t, b.eventHub)
			code, reply = rpc("tools/call", map[string]any{"name": "get_message", "arguments": getMessageInput{ChatID: "123@s.whatsapp.net", MessageID: "http-" + version}}, "Bearer "+token, "", version)
			if code != 200 || reply.Error != nil || !bytes.Contains(reply.Result, []byte("hello")) {
				t.Fatal("event message could not be retrieved")
			}
			code, reply = rpc("events/unsubscribe", in, "Bearer "+token, "", version)
			if code != 200 || reply.Error != nil {
				t.Fatal("unsubscribe failed")
			}
			in.Delivery.Secret = "bad"
			_, reply = rpc("events/subscribe", in, "Bearer "+token, "", version)
			if reply.Error == nil || reply.Error.Code != -32602 {
				t.Fatal("structured subscription error lost over MCP")
			}
		})
	}
	if len(r.events) != 2 {
		t.Fatal("HTTP MCP lifecycle did not deliver both synthetic events")
	}
}

func TestEventsTokenAndHTTPBindBoundaries(t *testing.T) {
	dir := t.TempDir()
	if err := initMCPToken(dir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "mcp-http.token")
	first, err := readMCPToken(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = initMCPToken(dir); err != nil {
		t.Fatal(err)
	}
	second, err := readMCPToken(path)
	if err != nil || first != second {
		t.Fatal("token unexpectedly rotated")
	}
	if err = os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = readMCPToken(path); err == nil {
		t.Fatal("public token file accepted")
	}
	if err = runMCPHTTP(context.Background(), dir, "0.0.0.0:8765", path); err == nil {
		t.Fatal("public bind accepted")
	}
	if err = runMCPHTTP(context.Background(), dir, "127.0.0.1:0", ""); err == nil {
		t.Fatal("HTTP mode started without a token")
	}
}

func TestEventsWorkerWakeAndAcknowledgement(t *testing.T) {
	h, r := testEventHub(t)
	r.largeAck = true
	subscribeForTest(t, h, eventInput("123@s.whatsapp.net"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.run(ctx, func(message string) {
			if ctx.Err() == nil {
				t.Error(message)
			}
		})
	}()
	saveLive(t, h.a, fixture("worker"))
	h.wake()
	deadline := time.After(3 * time.Second)
	for {
		r.mu.Lock()
		delivered := len(r.events) > 0
		r.mu.Unlock()
		if delivered && eventCount(t, h.a, "event_deliveries") == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("worker did not acknowledge the event")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event worker did not stop")
	}
	if eventCount(t, h.a, "event_deliveries") != 0 {
		t.Fatal("2xx receipt retried because its response body was large")
	}
}

func TestEventsUnsubscribeWaitsForInflightDelivery(t *testing.T) {
	h, _ := testEventHub(t)
	in := eventInput("123@s.whatsapp.net")
	subscribeForTest(t, h, in)
	saveLive(t, h.a, fixture("inflight"))
	started, release := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	h.http = &http.Client{Transport: eventRoundTripper(func(r *http.Request) (*http.Response, error) {
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		return &http.Response{StatusCode: 204, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}
	deliveryDone := make(chan error, 1)
	go func() { _, err := h.deliverOne(ctx); deliveryDone <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("delivery did not start")
	}
	unsubscribed := make(chan eventReply, 1)
	go func() { unsubscribed <- h.call(ctx, "unsubscribe", in) }()
	select {
	case <-unsubscribed:
		t.Fatal("unsubscribe returned with a delivery still in flight")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-deliveryDone; err != nil {
		t.Fatal(err)
	}
	if reply := <-unsubscribed; reply.Error != nil {
		t.Fatal(reply.Error)
	}
	saveLive(t, h.a, fixture("after-stop"))
	if eventCount(t, h.a, "event_deliveries") != 0 || eventCount(t, h.a, "event_subscriptions") != 0 {
		t.Fatal("unsubscribe did not cancel future delivery")
	}
}
