package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/types"
)

// Start the normal service in a separate process with an offline transport.
// An abrupt stop tests the session, monitor and draft on disk, including WAL
// recovery and reclaiming the service lock and socket on the next start.
func TestServiceResumesLinkedDeviceAndMonitor(t *testing.T) {
	const worker = "WHATSAPP_MCP_TEST_RESTART_WORKER"
	if os.Getenv(worker) == "1" {
		if _, err := os.Stat("session.db"); err != nil {
			t.Fatal("missing temporary device fixture")
		}
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil
		transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("fixture is offline")
		}
		http.DefaultTransport = transport // whatsmeow clones this for both login paths.
		if err := run([]string{"serve", "--data-dir", "."}); err != nil {
			t.Fatal(err)
		}
		return
	}

	// A short path also fits macOS's Unix socket path length limit.
	dir, err := os.MkdirTemp("", "wa-restart-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	ctx := t.Context()
	a, err := openArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := newBridge(ctx, dir, a)
	if err != nil {
		t.Fatal(err)
	}
	own := types.NewJID("999", types.DefaultUserServer)
	linked := own
	linked.Device = 2
	device := b.client.Store
	device.ID = &linked
	device.Account = &waAdv.ADVSignedDeviceIdentity{Details: []byte("fixture"), AccountSignature: make([]byte, 64), AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64)}
	if err = device.Save(ctx); err != nil { // same store API used after successful pairing
		t.Fatal(err)
	}
	if !b.allowedAccount() {
		t.Fatal("fixture account missing")
	}
	group := "123-456@g.us"
	if err = a.saveChat(group, "Test group"); err != nil {
		t.Fatal(err)
	}
	watch, err := a.startMonitor(ctx, startMonitorInput{group, "worker:test", "Reply to greetings"})
	if err != nil {
		t.Fatal(err)
	}
	m := fixture("greeting")
	m.ChatID = group
	saveLive(t, a, m)
	if err = b.container.Close(); err != nil {
		t.Fatal(err)
	}
	if err = a.db.Close(); err != nil {
		t.Fatal(err)
	}

	start := func() (*localClient, func()) {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestServiceResumesLinkedDeviceAndMonitor$")
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), worker+"=1")
		cmd.Stdout, cmd.Stderr = t.Output(), t.Output()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		stop := sync.OnceFunc(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
		t.Cleanup(stop)
		client := newLocalClient(dir)
		t.Cleanup(client.http.CloseIdleConnections)
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
			var s status
			if err := client.call(ctx, "GET", "/status", nil, &s); err == nil && s.LastError == "WhatsApp connection failed; retrying" {
				if !s.Paired || s.Connected || s.Account != own.String() || s.ActiveMonitors != 1 || s.ReplyJobsPending != 1 {
					t.Fatal("startup did not recover the linked device and pending work")
				}
				return client, stop
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("service did not attempt automatic reconnection")
		return nil, nil
	}
	first, stop := start()
	var jobs []replyJob
	if err = first.call(ctx, "POST", "/reply-jobs/claim", claimJobsInput{ConsumerID: watch.ConsumerID}, &jobs); err != nil || len(jobs) != 1 {
		t.Fatalf("claim before restart: %v", err)
	}
	j := jobs[0]
	var draft replyJob
	if err = first.call(ctx, "POST", "/reply-jobs/draft", draftReplyInput{j.ID, j.ClaimToken, outboundContent{Text: "Oi 👋"}}, &draft); err != nil {
		t.Fatal(err)
	}
	stop()
	second, _ := start()
	var watches []monitor
	if err = second.call(ctx, "POST", "/monitors/list", monitorFilter{}, &watches); err != nil || len(watches) != 1 || watches[0].ID != watch.ID || watches[0].ChatID != group || watches[0].Instructions != watch.Instructions || !watches[0].Active {
		t.Fatalf("group watch lost after restart: %v", err)
	}
	jobs = nil
	if err = second.call(ctx, "POST", "/reply-jobs/list", jobsFilter{}, &jobs); err != nil || len(jobs) != 1 || jobs[0].ID != j.ID || jobs[0].State != "drafted" || jobs[0].Draft != draft.Draft {
		t.Fatalf("draft lost after restart: %v", err)
	}
	var pair pairing
	if err = second.call(ctx, "GET", "/pair", nil, &pair); err != nil || pair.State != "idle" || pair.Code != "" {
		t.Fatalf("paired startup opened a pairing flow: %v", err)
	}
}
