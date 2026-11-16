package main

import (
	"context"
	"testing"

	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func contactSyncBridge(t *testing.T) (*bridge, types.JID, types.JID) {
	t.Helper()
	dir := t.TempDir()
	a, err := openArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.db.Close() })
	b, err := newBridge(context.Background(), dir, a)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.container.Close() })
	own := types.NewJID("999", types.DefaultUserServer)
	b.client.Store.ID = &own
	pn, lid := types.NewJID("123", types.DefaultUserServer), types.NewJID("456", types.HiddenUserServer)
	b.client.Store.LIDs = b.container.LIDMap
	if err = b.client.Store.LIDs.PutLIDMapping(context.Background(), lid, pn); err != nil {
		t.Fatal(err)
	}
	return b, pn, lid
}

func TestContactSyncDeterministicFieldPrecedence(t *testing.T) {
	b, pn, lid := contactSyncBridge(t)
	for _, test := range []struct {
		name               string
		pn, lid            types.ContactInfo
		wantName, wantPush string
	}{
		{"phone name wins ties", types.ContactInfo{FullName: "Phone name", PushName: "Phone push"}, types.ContactInfo{FullName: "LID name", PushName: "LID push"}, "Phone name", "Phone push"},
		{"saved name beats business", types.ContactInfo{BusinessName: "Business"}, types.ContactInfo{FullName: "Saved name", PushName: "Alias push"}, "Saved name", "Alias push"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b.client.Store.Contacts = &snapshotContacts{rows: map[types.JID]types.ContactInfo{pn: test.pn, lid: test.lid}}
			for i := 0; i < 20; i++ {
				b.syncContacts()
				p, err := b.a.contacts(context.Background(), listInput{Query: pn.User})
				if err != nil {
					t.Fatal(err)
				}
				if len(p.Items) != 1 || p.Items[0].Name != test.wantName || p.Items[0].PushName != test.wantPush {
					t.Fatalf("inconsistent merged contact: %+v", p)
				}
			}
		})
	}
}

func TestContactRemovalAndRenameRemainAuthoritative(t *testing.T) {
	b, pn, lid := contactSyncBridge(t)
	b.client.Store.Contacts = &snapshotContacts{rows: map[types.JID]types.ContactInfo{pn: {FullName: "Alice Smith"}, lid: {PushName: "Alice"}}}
	b.syncContacts()
	// A sparse later snapshot must not erase a previously saved name.
	b.client.Store.Contacts = &snapshotContacts{rows: map[types.JID]types.ContactInfo{lid: {PushName: "Alice"}}}
	b.syncContacts()
	p, err := b.a.contacts(context.Background(), listInput{Query: "Alice Smith"})
	if err != nil || len(p.Items) != 1 {
		t.Fatalf("sparse sync lost name: %+v %v", p, err)
	}
	b.event(&events.Contact{JID: pn, Action: nil})
	b.syncContacts()
	p, err = b.a.contacts(context.Background(), listInput{Query: "Alice Smith"})
	if err != nil || len(p.Items) != 0 {
		t.Fatalf("explicit removal was undone: %+v %v", p, err)
	}
	b.event(&events.Contact{JID: lid, Action: &waSyncAction.ContactAction{FullName: proto.String("Alice Jones")}})
	b.event(&events.PushName{JID: lid, NewPushName: "Alice New"})
	p, err = b.a.contacts(context.Background(), listInput{Query: "Alice Jones"})
	if err != nil || len(p.Items) != 1 || p.Items[0].PushName != "Alice New" {
		t.Fatalf("contact rename was lost: %+v %v", p, err)
	}
}
