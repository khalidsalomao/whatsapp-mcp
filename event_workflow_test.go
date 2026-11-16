package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

// The example host sleeps on pushed callbacks. It has no timer, long poll or
// scheduled task. Production hosts must persist accepted callbacks before 2xx
// and resume interrupted work from their durable queue.
func TestPushedEventsDriveMCPRepliesWithoutClientPolling(t *testing.T) {
	b, pn, _ := contactSyncBridge(t)
	b.connectedFn = func() bool { return true }
	if !b.allowedAccount() {
		t.Fatal("account fixture not bound")
	}
	if err := b.a.saveContact(contact{ID: pn.String()}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	local := serveTestSocket(t, b)
	defer local.http.CloseIdleConnections()
	st, ct := mcp.NewInMemoryTransports()
	ss, err := newMCP(local).Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	client, err := mcp.NewClient(&mcp.Implementation{Name: "event-capable-host", Version: "test"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	call := func(name string, args, out any) error {
		result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			return err
		}
		if result.IsError {
			return fmt.Errorf("%s returned an error: %v", name, result.Content)
		}
		for _, content := range result.Content {
			if text, ok := content.(*mcp.TextContent); ok {
				return json.Unmarshal([]byte(text.Text), out)
			}
		}
		return fmt.Errorf("%s returned no JSON content", name)
	}
	var watch monitor
	if err = call("start_monitor", startMonitorInput{pn.String(), "agent:event-test", "Reply to each incoming greeting"}, &watch); err != nil {
		t.Fatal(err)
	}
	callbacks := make(chan messageEvent, 10)
	input := eventInput(pn.String())
	b.eventHub.http = &http.Client{Transport: eventRoundTripper(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		expectSignature(t, capturedWebhook{ID: r.Header.Get("webhook-id"), Stamp: r.Header.Get("webhook-timestamp"), Signature: r.Header.Get("webhook-signature"), Body: body}, input.Delivery.Secret)
		var control struct{ Type, Challenge string }
		if err = json.Unmarshal(body, &control); err != nil {
			return nil, err
		}
		response := `{}`
		if control.Type == "verification" {
			encoded, _ := json.Marshal(map[string]string{"challenge": control.Challenge})
			response = string(encoded)
		} else {
			var event messageEvent
			if err = json.Unmarshal(body, &event); err != nil {
				return nil, err
			}
			// Include a duplicate callback to exercise host/job deduplication.
			callbacks <- event
			callbacks <- event
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response)), Request: r}, nil
	})}
	subscribeForTest(t, b.eventHub, input)
	var sends, claims atomic.Int32
	b.sendFn = func(_ context.Context, recipient types.JID, msg *waE2E.Message, _ whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		if recipient.String() != pn.String() || msg.GetExtendedTextMessage().GetText() != "Hello from the test agent" {
			return whatsmeow.SendResponse{}, fmt.Errorf("wrong reply arguments")
		}
		sends.Add(1)
		return whatsmeow.SendResponse{Timestamp: time.Now()}, nil
	}
	workerDone, deliveryDone := make(chan struct{}), make(chan struct{})
	processed := make(chan int, 10)
	workerError := make(chan error, 1)
	go func() {
		defer close(workerDone)
		seen := map[string]bool{}
		completed := 0
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-callbacks:
				if seen[event.ID] {
					continue
				}
				seen[event.ID] = true
				// Drain all available jobs for this consumer after a wake-up. This
				// handles batching and messages arriving while the agent is busy.
				for {
					var jobs []replyJob
					claims.Add(1)
					if err := call("claim_reply_jobs", claimJobsInput{ConsumerID: watch.ConsumerID, WaitSeconds: 0}, &jobs); err != nil {
						workerError <- err
						return
					}
					if len(jobs) == 0 {
						break
					}
					j := jobs[0]
					var draft, sent replyJob
					if err := call("draft_reply", draftReplyInput{j.ID, j.ClaimToken, outboundContent{Text: "Hello from the test agent"}}, &draft); err != nil {
						workerError <- err
						return
					}
					if err := call("send_reply", jobClaimInput{j.ID, j.ClaimToken}, &sent); err != nil || sent.State != "sent" {
						workerError <- fmt.Errorf("send result %s: %v", sent.State, err)
						return
					}
					completed++
					processed <- completed
				}
			}
		}
	}()
	go func() {
		defer close(deliveryDone)
		b.eventHub.run(ctx, func(reason string) { t.Error(reason) })
	}()
	defer func() { cancel(); <-workerDone; <-deliveryDone }()
	if claims.Load() != 0 {
		t.Fatal("host called tools before receiving an event")
	}
	for _, id := range []string{"push-first", "push-second"} {
		event := eventFixture()
		event.Info.ID = id
		b.event(event)
	}
	for {
		select {
		case n := <-processed:
			if n != 2 {
				continue
			}
			if sends.Load() != 2 {
				t.Fatalf("duplicate callback caused %d sends", sends.Load())
			}
			return
		case err := <-workerError:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal("pushed events did not complete the reply workflow")
		}
	}
}
