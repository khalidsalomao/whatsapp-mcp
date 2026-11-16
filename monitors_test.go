package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

func monitoredBridge(t *testing.T) (*bridge, monitor) {
	t.Helper()
	b := testBridge(t)
	if err := b.a.setMeta("account", "555@s.whatsapp.net"); err != nil {
		t.Fatal(err)
	}
	m, err := b.a.startMonitor(context.Background(), startMonitorInput{ChatID: "123@s.whatsapp.net", ConsumerID: "codex:test", Instructions: "Reply briefly to routine greetings. Ask me about commitments."})
	if err != nil {
		t.Fatal(err)
	}
	return b, m
}

func incomingJob(t *testing.T, b *bridge, id string) replyJob {
	t.Helper()
	if err := b.a.saveObserved(fixture(id), true); err != nil {
		t.Fatal(err)
	}
	jobs, err := b.pollJobs(context.Background(), claimJobsInput{ConsumerID: "codex:test"})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: %+v %v", jobs, err)
	}
	return jobs[0]
}

func saveDraft(t *testing.T, b *bridge, j replyJob) replyJob {
	t.Helper()
	draft, err := b.a.prepareJob(context.Background(), jobClaimInput{j.ID, j.ClaimToken}, "draft", savedReply{Draft: "Hello there!"}, "")
	if err != nil || draft.State != "drafted" {
		t.Fatalf("draft: %+v %v", draft, err)
	}
	return draft
}

func TestMonitorOnlyLiveIncomingAndAssignedConsumer(t *testing.T) {
	b, watch := monitoredBridge(t)
	ctx := context.Background()
	again, err := b.a.startMonitor(ctx, startMonitorInput{watch.ChatID, watch.ConsumerID, watch.Instructions})
	if err != nil || again.ID != watch.ID {
		t.Fatal("repeated watch creation was not idempotent")
	}
	if _, err = b.a.startMonitor(ctx, startMonitorInput{watch.ChatID, "grok:bot", "Reply"}); err == nil {
		t.Fatal("allowed two responders for one chat")
	}
	for _, kind := range []string{"history", "outgoing", "edit", "deleted", "expired", "other-chat", "live"} {
		m := fixture(kind)
		m.FromMe = kind == "outgoing"
		m.Edited = kind == "edit"
		m.Deleted = kind == "deleted"
		if kind == "expired" {
			m.Expires = time.Now().Add(-time.Second).Unix()
		}
		if kind == "other-chat" {
			m.ChatID = "789@s.whatsapp.net"
		}
		for attempt := 0; attempt < 2; attempt++ {
			if err = b.a.saveObserved(m, kind != "history"); err != nil {
				t.Fatal(err)
			}
		}
	}
	wrong, err := b.pollJobs(ctx, claimJobsInput{ConsumerID: "grok:bot"})
	if err != nil || len(wrong) != 0 {
		t.Fatal("other consumer got work")
	}
	jobs, err := b.pollJobs(ctx, claimJobsInput{ConsumerID: watch.ConsumerID, Limit: 20})
	if err != nil || len(jobs) != 1 || jobs[0].MessageID != "live" || jobs[0].Message.Text != "hello" || jobs[0].Instructions != watch.Instructions {
		t.Fatalf("live job: %+v %v", jobs, err)
	}
}

func TestMonitorConcurrentWorkersAndSends(t *testing.T) {
	b, _ := monitoredBridge(t)
	if err := b.a.saveObserved(fixture("live"), true); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var wins atomic.Int32
	var winner replyJob
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			jobs, err := b.pollJobs(ctx, claimJobsInput{ConsumerID: "codex:test"})
			if err != nil {
				t.Error(err)
			}
			if len(jobs) > 0 {
				wins.Add(1)
				mu.Lock()
				winner = jobs[0]
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d workers claimed one message", wins.Load())
	}
	j := saveDraft(t, b, winner)
	var sends atomic.Int32
	b.sendFn = func(_ context.Context, jid types.JID, m *waE2E.Message, _ whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		sends.Add(1)
		if jid.String() != j.ChatID || m.GetExtendedTextMessage().GetText() != j.Draft || m.GetExtendedTextMessage().GetContextInfo().GetStanzaID() != j.MessageID {
			t.Error("reply routed incorrectly")
		}
		return whatsmeow.SendResponse{Timestamp: time.Now()}, nil
	}
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := b.sendReply(ctx, jobClaimInput{j.ID, j.ClaimToken})
			if err != nil || out.State != "sent" {
				t.Errorf("reply: %+v %v", out, err)
			}
		}()
	}
	wg.Wait()
	if sends.Load() != 1 {
		t.Fatalf("%d network sends", sends.Load())
	}
}

func TestMonitorStopAndExpiredClaims(t *testing.T) {
	b, watch := monitoredBridge(t)
	j := saveDraft(t, b, incomingJob(t, b, "leased"))
	ctx := context.Background()
	if _, err := b.a.db.Exec(`UPDATE reply_jobs SET lease_until=0 WHERE id=?`, j.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := b.sendReply(ctx, jobClaimInput{j.ID, j.ClaimToken}); err == nil {
		t.Fatal("expired worker sent a reply")
	}
	jobs, err := b.pollJobs(ctx, claimJobsInput{ConsumerID: watch.ConsumerID})
	if err != nil || len(jobs) != 1 || jobs[0].ClaimToken == j.ClaimToken || jobs[0].Draft != j.Draft {
		t.Fatalf("reclaim did not retain draft and rotate token: %+v %v", jobs, err)
	}
	j = jobs[0]
	stopped, err := b.a.stopMonitor(ctx, watch.ID)
	if err != nil || stopped.Cancelled != 1 {
		t.Fatalf("stop: %+v %v", stopped, err)
	}
	if _, err = b.sendReply(ctx, jobClaimInput{j.ID, j.ClaimToken}); err == nil {
		t.Fatal("stopped monitor sent a reply")
	}
	if err = b.a.saveObserved(fixture("after-stop"), true); err != nil {
		t.Fatal(err)
	}
	all, err := b.a.listJobs(ctx, jobsFilter{})
	if err != nil || len(all) != 1 || all[0].State != "cancelled" || all[0].Draft != "" || all[0].ClaimToken != "" {
		t.Fatalf("stopped jobs: %+v %v", all, err)
	}
}

func TestMonitorRevalidatesChangedAndDeletedSources(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(map[bool]string{true: "deleted", false: "edited"}[deleted], func(t *testing.T) {
			b, _ := monitoredBridge(t)
			j := saveDraft(t, b, incomingJob(t, b, "changed"))
			m := fixture("changed")
			m.Edited, m.Deleted, m.Text, m.Revision = !deleted, deleted, "changed content", 100
			mustSave(t, b.a, m)
			if _, err := b.sendReply(context.Background(), jobClaimInput{j.ID, j.ClaimToken}); err == nil {
				t.Fatal("sent stale draft")
			}
			jobs, err := b.pollJobs(context.Background(), claimJobsInput{ConsumerID: "codex:test"})
			if err != nil {
				t.Fatal(err)
			}
			if deleted && len(jobs) != 0 {
				t.Fatal("deleted source reclaimed")
			}
			if !deleted && (len(jobs) != 1 || jobs[0].Draft != "" || jobs[0].Message.Text != "changed content") {
				t.Fatalf("edit did not clear draft: %+v", jobs)
			}
		})
	}
}

func TestMonitorUncertainSendIsTerminal(t *testing.T) {
	b, _ := monitoredBridge(t)
	j := saveDraft(t, b, incomingJob(t, b, "uncertain"))
	var sends int
	b.sendFn = func(context.Context, types.JID, *waE2E.Message, whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		sends++
		return whatsmeow.SendResponse{}, context.DeadlineExceeded
	}
	for range 2 {
		out, err := b.sendReply(context.Background(), jobClaimInput{j.ID, j.ClaimToken})
		if err != nil || out.State != "uncertain" {
			t.Fatalf("uncertain: %+v %v", out, err)
		}
	}
	jobs, err := b.pollJobs(context.Background(), claimJobsInput{ConsumerID: "codex:test"})
	if err != nil || len(jobs) != 0 || sends != 1 {
		t.Fatal("uncertain reply was retried")
	}
}

func TestMonitorSerializesConversationAndOfflineRetry(t *testing.T) {
	b, _ := monitoredBridge(t)
	for _, id := range []string{"first", "second"} {
		if err := b.a.saveObserved(fixture(id), true); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	jobs, err := b.pollJobs(ctx, claimJobsInput{ConsumerID: "codex:test", Limit: 20})
	if err != nil || len(jobs) != 1 || jobs[0].MessageID != "first" {
		t.Fatalf("conversation order: %+v %v", jobs, err)
	}
	j := saveDraft(t, b, jobs[0])
	b.connectedFn = func() bool { return false }
	if _, err := b.sendReply(ctx, jobClaimInput{j.ID, j.ClaimToken}); err == nil {
		t.Fatal("offline send succeeded")
	}
	b.connectedFn = func() bool { return true }
	out, err := b.sendReply(ctx, jobClaimInput{j.ID, j.ClaimToken})
	if err != nil || out.State != "sent" {
		t.Fatalf("safe offline retry: %+v %v", out, err)
	}
	jobs, err = b.pollJobs(ctx, claimJobsInput{ConsumerID: "codex:test"})
	if err != nil || len(jobs) != 1 || jobs[0].MessageID != "second" {
		t.Fatal("next message not released")
	}
}

func TestMonitorRestartRecoversSendLedger(t *testing.T) {
	for _, state := range []string{"absent", "sending", "acknowledged"} {
		t.Run(state, func(t *testing.T) {
			b, _ := monitoredBridge(t)
			j := saveDraft(t, b, incomingJob(t, b, "crash"))
			ctx := context.Background()
			if _, err := b.a.prepareJob(ctx, jobClaimInput{j.ID, j.ClaimToken}, "send", savedReply{}, ""); err != nil {
				t.Fatal(err)
			}
			if state != "absent" {
				if _, err := b.a.db.Exec(`INSERT INTO outbox VALUES(?,?,?,?,?,?)`, j.RequestID, "fingerprint", j.ChatID, "sent-id", state, time.Now().Unix()); err != nil {
					t.Fatal(err)
				}
			}
			var seq int
			var name, path string
			if err := b.a.db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
				t.Fatal(err)
			}
			b.a.db.Close()
			reopened, err := openArchive(filepath.Dir(path))
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.db.Close()
			jobs, err := reopened.listJobs(ctx, jobsFilter{})
			want := map[string]string{"absent": "pending", "sending": "uncertain", "acknowledged": "sent"}[state]
			if err != nil || len(jobs) != 1 || jobs[0].State != want {
				t.Fatalf("recovery: %+v %v", jobs, err)
			}
		})
	}
}

func TestMonitorAliasConflictAndRevocation(t *testing.T) {
	b, _ := monitoredBridge(t)
	ctx := context.Background()
	if err := b.a.saveContact(contact{ID: "987@lid", Name: "Alias"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.a.startMonitor(ctx, startMonitorInput{"987@lid", "grok:test", "Reply to greetings"}); err != nil {
		t.Fatal(err)
	}
	if err := b.a.alias("987@lid", "123@s.whatsapp.net"); err != nil {
		t.Fatal(err)
	}
	watches, err := b.a.listMonitors(ctx, monitorFilter{})
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range watches {
		if w.Active {
			t.Fatal("conflicting alias responders were not stopped")
		}
	}
	if _, err = b.a.startMonitor(ctx, startMonitorInput{"123@s.whatsapp.net", "codex:test", "Reply"}); err != nil {
		t.Fatal(err)
	}
	j := incomingJob(t, b, "revoked")
	if err = b.a.revokeMonitors(); err != nil {
		t.Fatal(err)
	}
	if _, err = b.sendReply(ctx, jobClaimInput{j.ID, j.ClaimToken}); err == nil {
		t.Fatal("revoked monitor sent")
	}
}

func TestMonitorMCPWorkflowAcrossClients(t *testing.T) {
	b := testBridge(t)
	if err := b.a.setMeta("account", "555@s.whatsapp.net"); err != nil {
		t.Fatal(err)
	}
	local := serveTestSocket(t, b)
	t.Cleanup(local.http.CloseIdleConnections)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	call := func(name string, args any, out any) {
		t.Helper()
		// Each call uses a fresh MCP client to prove durable state is shared.
		st, ct := mcp.NewInMemoryTransports()
		ss, err := newMCP(local).Connect(ctx, st, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ss.Close()
		cs, err := mcp.NewClient(&mcp.Implementation{Name: "grok-compatible-test", Version: "1"}, nil).Connect(ctx, ct, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer cs.Close()
		r, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil || r.IsError {
			t.Fatalf("%s: %+v %v", name, r, err)
		}
		for _, content := range r.Content {
			if text, ok := content.(*mcp.TextContent); ok {
				if err = json.Unmarshal([]byte(text.Text), out); err != nil {
					t.Fatalf("%s result: %s: %v", name, text.Text, err)
				}
				return
			}
		}
		t.Fatal("tool returned no JSON text")
	}
	var watch monitor
	call("start_monitor", startMonitorInput{"123@s.whatsapp.net", "grok:test", "Reply to greetings"}, &watch)
	if err := b.a.saveObserved(fixture("wire"), true); err != nil {
		t.Fatal(err)
	}
	var jobs []replyJob
	call("claim_reply_jobs", claimJobsInput{ConsumerID: "grok:test"}, &jobs)
	if len(jobs) != 1 {
		t.Fatalf("claimed %+v", jobs)
	}
	j := jobs[0]
	var draft, sent replyJob
	call("draft_reply", draftReplyInput{j.ID, j.ClaimToken, outboundContent{Text: "Hello!"}}, &draft)
	call("send_reply", jobClaimInput{j.ID, j.ClaimToken}, &sent)
	if sent.State != "sent" {
		t.Fatalf("send result: %+v", sent)
	}
	var stopped stopMonitorResult
	call("stop_monitor", stopMonitorInput{watch.ID}, &stopped)
	jobs = nil
	call("list_reply_jobs", jobsFilter{ConsumerID: "grok:test"}, &jobs)
	if len(jobs) != 1 || jobs[0].State != "sent" || jobs[0].ClaimToken != "" {
		t.Fatal("send audit missing or claim token leaked")
	}
}

func TestMonitorLongPollAndCancellation(t *testing.T) {
	b, _ := monitoredBridge(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ready := make(chan []replyJob, 1)
	go func() {
		jobs, err := b.pollJobs(ctx, claimJobsInput{ConsumerID: "codex:test", WaitSeconds: 2})
		if err != nil {
			t.Error(err)
		}
		ready <- jobs
	}()
	if err := b.a.saveObserved(fixture("wake"), true); err != nil {
		t.Fatal(err)
	}
	select {
	case jobs := <-ready:
		if len(jobs) != 1 || jobs[0].MessageID != "wake" {
			t.Fatal("long poll missed incoming job")
		}
	case <-ctx.Done():
		t.Fatal("long poll did not return")
	}
	cancel()
	if _, err := b.pollJobs(ctx, claimJobsInput{ConsumerID: "codex:test", WaitSeconds: 25}); err == nil {
		t.Fatal("cancelled poll did not stop")
	}
}

func TestMonitorStopWaitsForAlreadyDispatchingSend(t *testing.T) {
	b, watch := monitoredBridge(t)
	j := saveDraft(t, b, incomingJob(t, b, "in-flight"))
	started, release, sendDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	b.sendFn = func(context.Context, types.JID, *waE2E.Message, whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		close(started)
		<-release
		return whatsmeow.SendResponse{Timestamp: time.Now()}, nil
	}
	go func() {
		defer close(sendDone)
		if _, err := b.sendReply(context.Background(), jobClaimInput{j.ID, j.ClaimToken}); err != nil {
			t.Error(err)
		}
	}()
	<-started
	c := serveTestSocket(t, b)
	defer c.http.CloseIdleConnections()
	stopped := make(chan error, 1)
	go func() {
		var out stopMonitorResult
		stopped <- c.call(context.Background(), "POST", "/monitors/stop", stopMonitorInput{watch.ID}, &out)
	}()
	select {
	case err := <-stopped:
		close(release)
		<-sendDone
		t.Fatalf("stop returned while dispatch still in flight: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	<-sendDone
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if err := b.a.saveObserved(fixture("after-stop"), true); err != nil {
		t.Fatal(err)
	}
	jobs, err := b.pollJobs(context.Background(), claimJobsInput{ConsumerID: "codex:test"})
	if err != nil || len(jobs) != 0 {
		t.Fatal("stopped monitor accepted another reply")
	}
}

func TestMonitorExpirationPurgesSavedDraft(t *testing.T) {
	b, _ := monitoredBridge(t)
	j := saveDraft(t, b, incomingJob(t, b, "expires"))
	if _, err := b.a.db.Exec(`UPDATE messages SET expires=? WHERE chat=? AND id=?`, time.Now().Add(-time.Second).Unix(), j.ChatID, j.MessageID); err != nil {
		t.Fatal(err)
	}
	if err := b.a.expire(); err != nil {
		t.Fatal(err)
	}
	jobs, err := b.a.listJobs(context.Background(), jobsFilter{})
	if err != nil || len(jobs) != 1 || jobs[0].Draft != "" || jobs[0].State != "skipped" {
		t.Fatalf("expired content retained: %+v %v", jobs, err)
	}
}

func TestReplyRechecksPreconditionsAtOutboxReservation(t *testing.T) {
	for _, change := range []string{"edited", "lease_expired", "revoked"} {
		t.Run(change, func(t *testing.T) {
			b, _ := monitoredBridge(t)
			j := saveDraft(t, b, incomingJob(t, b, "queued-reply"))
			entered, release := make(chan struct{}), make(chan struct{})
			b.connectedFn = func() bool { close(entered); <-release; return true }
			var sends atomic.Int32
			b.sendFn = func(context.Context, types.JID, *waE2E.Message, whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
				sends.Add(1)
				return whatsmeow.SendResponse{Timestamp: time.Now()}, nil
			}
			done := make(chan error, 1)
			go func() {
				_, err := b.sendReply(context.Background(), jobClaimInput{j.ID, j.ClaimToken})
				done <- err
			}()
			<-entered // prepareJob succeeded; dispatch has not been reserved.
			switch change {
			case "edited":
				m := fixture(j.MessageID)
				m.Text, m.Edited, m.Revision = "A different question", true, 100
				mustSave(t, b.a, m)
			case "lease_expired":
				if _, err := b.a.db.Exec(`UPDATE reply_jobs SET lease_until=0 WHERE id=?`, j.ID); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				if err := b.a.revokeMonitors(); err != nil {
					t.Fatal(err)
				}
			}
			close(release)
			if err := <-done; err == nil || sends.Load() != 0 {
				t.Fatalf("stale reply dispatched: sends=%d error=%v", sends.Load(), err)
			}
			var reservations int
			if err := b.a.db.QueryRow(`SELECT COUNT(*) FROM outbox WHERE request_id=?`, j.RequestID).Scan(&reservations); err != nil || reservations != 0 {
				t.Fatal("stale reply reserved an outbound send")
			}
			jobs, err := b.a.listJobs(context.Background(), jobsFilter{})
			want := "pending"
			if change == "revoked" {
				want = "cancelled"
			}
			if err != nil || len(jobs) != 1 || jobs[0].State != want || jobs[0].Draft != "" || jobs[0].LeaseUntil != 0 {
				t.Fatalf("invalid draft/claim retained: %+v %v", jobs, err)
			}
		})
	}
}
