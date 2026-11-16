package main

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

type offlineTransport struct{ calls atomic.Int32 }

func (r *offlineTransport) RoundTrip(*http.Request) (*http.Response, error) {
	r.calls.Add(1)
	return nil, errors.New("simulated offline network")
}

type snapshotContacts struct {
	store.ContactStore
	rows map[types.JID]types.ContactInfo
}

func (c *snapshotContacts) GetAllContacts(context.Context) (map[types.JID]types.ContactInfo, error) {
	return c.rows, nil
}

func TestRegressionSyncPreservesSavedContactNameAcrossAliases(t *testing.T) {
	dir := t.TempDir()
	a, err := openArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer a.db.Close()
	b, err := newBridge(context.Background(), dir, a)
	if err != nil {
		t.Fatal(err)
	}
	defer b.container.Close()
	own := types.NewJID("999", types.DefaultUserServer)
	b.client.Store.ID = &own
	pn, lid := types.NewJID("123", types.DefaultUserServer), types.NewJID("456", types.HiddenUserServer)
	b.client.Store.LIDs = b.container.LIDMap
	if err = b.client.Store.LIDs.PutLIDMapping(context.Background(), lid, pn); err != nil {
		t.Fatal(err)
	}
	b.client.Store.Contacts = &snapshotContacts{rows: map[types.JID]types.ContactInfo{
		pn: {Found: true, FullName: "Alice Smith"}, lid: {Found: true, PushName: "Alice"},
	}}
	for i := 0; i < 100; i++ {
		b.syncContacts()
		contacts, err := a.contacts(context.Background(), listInput{Query: "Alice Smith"})
		if err != nil {
			t.Fatal(err)
		}
		if len(contacts.Items) != 1 {
			t.Fatalf("contact sync %d lost the saved name when merging the nameless LID entry", i+1)
		}
	}
}

func TestRegressionPairingCanRetryAfterNetworkFailure(t *testing.T) {
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
	if err = b.startPair(); err != nil {
		t.Fatal(err)
	}
	waitPairFinished(t, b)
	if transport.calls.Load() != 1 {
		t.Fatalf("expected one initial connection attempt; got %d", transport.calls.Load())
	}
	if err = b.startPair(); err != nil {
		t.Fatal(err)
	}
	waitPairFinished(t, b)
	if transport.calls.Load() < 2 {
		b.mu.Lock()
		state := b.pair.State
		active := b.pairActive
		b.mu.Unlock()
		t.Fatalf("second pair request never connects: attempts=%d, state=%s, pairActive=%v", transport.calls.Load(), state, active)
	}
}

func TestRegressionChatRemovalHidesCachedContent(t *testing.T) {
	for _, kind := range []string{"clear", "delete"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			a, err := openArchive(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer a.db.Close()
			b, err := newBridge(context.Background(), dir, a)
			if err != nil {
				t.Fatal(err)
			}
			defer b.container.Close()
			own := types.NewJID("999", types.DefaultUserServer)
			b.client.Store.ID = &own
			m := fixture("removed-message")
			m.Text = "private-review-fixture"
			mustSave(t, a, m)
			jid := types.NewJID("123", types.DefaultUserServer)
			messageRange := &waSyncAction.SyncActionMessageRange{
				LastMessageTimestamp: proto.Int64(m.Timestamp),
				Messages:             []*waSyncAction.SyncActionMessage{{Key: &waCommon.MessageKey{RemoteJID: proto.String(m.ChatID), ID: proto.String(m.ID), FromMe: proto.Bool(false)}, Timestamp: proto.Int64(m.Timestamp)}},
			}
			if kind == "clear" {
				b.event(&events.ClearChat{JID: jid, Timestamp: time.Unix(m.Timestamp+10, 0), Action: &waSyncAction.ClearChatAction{MessageRange: messageRange}})
			} else {
				b.event(&events.DeleteChat{JID: jid, Timestamp: time.Unix(m.Timestamp+10, 0), Action: &waSyncAction.DeleteChatAction{MessageRange: messageRange}})
			}
			found, err := a.messages(context.Background(), listInput{ChatID: m.ChatID, Query: m.Text})
			if err != nil {
				t.Fatal(err)
			}
			if len(found.Items) > 0 {
				t.Fatalf("%s-chat event left %d removed message(s) readable and searchable", kind, len(found.Items))
			}
		})
	}
}
