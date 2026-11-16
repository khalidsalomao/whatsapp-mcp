package main

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func waitPairFinished(t *testing.T, b *bridge) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		active := b.pairActive
		b.mu.Unlock()
		if !active {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("pairing attempt did not finish")
}

func TestRepeatedPairingFailuresRestoreMainHandler(t *testing.T) {
	dir := t.TempDir()
	a, err := openArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer a.db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := newBridge(ctx, dir, a)
	if err != nil {
		t.Fatal(err)
	}
	defer b.container.Close()
	defer b.client.Disconnect()
	transport := &offlineTransport{}
	b.client.SetPreLoginHTTPClient(&http.Client{Transport: transport})
	for i := 1; i <= 5; i++ {
		if err = b.startPair(); err != nil {
			t.Fatal(err)
		}
		waitPairFinished(t, b)
		b.mu.Lock()
		state := b.pair.State
		b.mu.Unlock()
		if state != "connection-failed" || int(transport.calls.Load()) != i {
			t.Fatalf("retry %d: attempts=%d state=%s", i, transport.calls.Load(), state)
		}
	}
	// Exercise the real client dispatcher after cleanup to ensure our permanent
	// event handler has been reattached, not just the attempt flag reset.
	own := types.NewJID("999", types.DefaultUserServer)
	b.client.Store.ID = &own
	b.client.DangerousInternals().DispatchEvent(&events.PushName{JID: types.NewJID("123", types.DefaultUserServer), NewPushName: "Recovered handler"})
	p, err := a.contacts(ctx, listInput{Query: "Recovered handler"})
	if err != nil || len(p.Items) != 1 {
		t.Fatalf("main event handler was lost: %+v %v", p, err)
	}
}

type blockedDial struct {
	calls   atomic.Int32
	release chan struct{}
}

func (d *blockedDial) RoundTrip(r *http.Request) (*http.Response, error) {
	d.calls.Add(1)
	select {
	case <-d.release:
		return nil, errors.New("simulated dial failure")
	case <-r.Context().Done():
		return nil, r.Context().Err()
	}
}

func TestConcurrentPairRequestsShareOneAttempt(t *testing.T) {
	dir := t.TempDir()
	a, err := openArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer a.db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := newBridge(ctx, dir, a)
	if err != nil {
		t.Fatal(err)
	}
	defer b.container.Close()
	defer b.client.Disconnect()
	dial := &blockedDial{release: make(chan struct{})}
	b.client.SetPreLoginHTTPClient(&http.Client{Transport: dial})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.startPair(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	close(dial.release)
	waitPairFinished(t, b)
	if dial.calls.Load() != 1 {
		t.Fatalf("started %d simultaneous dials", dial.calls.Load())
	}
	if err = b.startPair(); err != nil {
		t.Fatal(err)
	}
	waitPairFinished(t, b)
	if dial.calls.Load() != 2 {
		t.Fatal("failed attempt prevented retry")
	}
}
