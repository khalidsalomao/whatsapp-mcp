package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"google.golang.org/protobuf/proto"
)

func removalRange(ts int64, ids ...string) *waSyncAction.SyncActionMessageRange {
	r := &waSyncAction.SyncActionMessageRange{LastMessageTimestamp: proto.Int64(ts)}
	for _, id := range ids {
		r.Messages = append(r.Messages, &waSyncAction.SyncActionMessage{Key: &waCommon.MessageKey{ID: proto.String(id)}, Timestamp: proto.Int64(ts)})
	}
	return r
}

func TestChatRemovalPreservesNewerAndOtherChatMessages(t *testing.T) {
	for _, deleteChat := range []bool{false, true} {
		t.Run(map[bool]string{false: "clear", true: "delete"}[deleteChat], func(t *testing.T) {
			a := testArchive(t)
			ctx := context.Background()
			old := fixture("old")
			mustSave(t, a, old)
			newer := fixture("newer")
			newer.Timestamp++
			mustSave(t, a, newer)
			other := fixture("other")
			other.ChatID = "456@s.whatsapp.net"
			mustSave(t, a, other)
			if err := a.removeChat(old.ChatID, time.Unix(old.Timestamp+500, 0), removalRange(old.Timestamp), deleteChat); err != nil {
				t.Fatal(err)
			}
			for _, want := range []message{newer, other} {
				got, err := a.getMessage(ctx, want.ChatID, want.ID)
				if err != nil || got.Deleted || got.Text != want.Text {
					t.Fatalf("unaffected message changed: %+v %v", got, err)
				}
			}
			got, err := a.getMessage(ctx, old.ChatID, old.ID)
			if err != nil || !got.Deleted || got.Text != "" {
				t.Fatalf("old message not removed: %+v %v", got, err)
			}
			chats, err := a.chats(ctx, listInput{Query: old.ChatID})
			if err != nil || len(chats.Items) != 1 {
				t.Fatalf("newer message should keep chat visible: %+v %v", chats, err)
			}
		})
	}
}

func TestDeletedChatStaysHiddenAcrossHistoryReplayAndRestart(t *testing.T) {
	dir := t.TempDir()
	a, err := openArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	old := fixture("known")
	mustSave(t, a, old)
	if err = a.removeChat(old.ChatID, time.Unix(old.Timestamp+5, 0), removalRange(old.Timestamp, "not-yet-synced"), true); err != nil {
		t.Fatal(err)
	}
	a.db.Close()
	a, err = openArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer a.db.Close()
	for _, id := range []string{"known", "not-yet-synced", "unseen-history"} {
		m := fixture(id)
		mustSave(t, a, m)
	}
	// An edit to an explicitly removed message must not resurrect its content,
	// even when the edit envelope carries a later timestamp.
	edit := fixture("not-yet-synced")
	edit.Timestamp += 50
	edit.Edited = true
	edit.Revision = edit.Timestamp * 1000
	edit.Text = "edited secret"
	mustSave(t, a, edit)
	if err = a.saveChat(old.ChatID, "history metadata"); err != nil {
		t.Fatal(err)
	}
	p, err := a.messages(context.Background(), listInput{ChatID: old.ChatID})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range p.Items {
		if !m.Deleted || m.Text != "" {
			t.Fatalf("history resurrected removed content: %+v", m)
		}
	}
	chats, err := a.chats(context.Background(), listInput{})
	if err != nil || len(chats.Items) != 0 {
		t.Fatalf("deleted chat resurrected by metadata or edit: %+v %v", chats, err)
	}
	newer := fixture("new-after-delete")
	newer.Timestamp++
	mustSave(t, a, newer)
	chats, err = a.chats(context.Background(), listInput{})
	if err != nil || len(chats.Items) != 1 {
		t.Fatalf("new message did not restore chat: %+v %v", chats, err)
	}
}

func TestClearedChatRemainsListed(t *testing.T) {
	a := testArchive(t)
	m := fixture("old")
	mustSave(t, a, m)
	if err := a.removeChat(m.ChatID, time.Unix(m.Timestamp+1, 0), removalRange(m.Timestamp), false); err != nil {
		t.Fatal(err)
	}
	p, err := a.chats(context.Background(), listInput{})
	if err != nil || len(p.Items) != 1 {
		t.Fatalf("clear incorrectly deleted chat: %+v %v", p, err)
	}
}

func TestChatRemovalBeforeHistoryAndAliasMigration(t *testing.T) {
	a := testArchive(t)
	m := fixture("historical")
	lid := "456@lid"
	if err := a.removeChat(lid, time.Unix(m.Timestamp, 0), removalRange(m.Timestamp, "explicit"), true); err != nil {
		t.Fatal(err)
	}
	mustSave(t, a, m) // Other identity was cached before its mapping became known.
	if err := a.alias(lid, m.ChatID); err != nil {
		t.Fatal(err)
	}
	m.ChatID = lid
	mustSave(t, a, m)
	p, err := a.messages(context.Background(), listInput{ChatID: lid})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range p.Items {
		if !m.Deleted || m.Text != "" {
			t.Fatalf("alias lost removal: %+v", m)
		}
	}
	chats, err := a.chats(context.Background(), listInput{})
	if err != nil || len(chats.Items) != 0 {
		t.Fatalf("alias restored hidden chat: %+v %v", chats, err)
	}
}

func TestChatRemovalCannotBeWeakenedByOlderReplay(t *testing.T) {
	a := testArchive(t)
	m := fixture("old")
	mustSave(t, a, m)
	for _, cutoff := range []int64{m.Timestamp + 10, m.Timestamp - 10} {
		if err := a.removeChat(m.ChatID, time.Unix(cutoff, 0), removalRange(cutoff), true); err != nil {
			t.Fatal(err)
		}
	}
	m.ID = "between"
	m.Timestamp += 5
	mustSave(t, a, m)
	got, err := a.getMessage(context.Background(), m.ChatID, m.ID)
	if err != nil || !got.Deleted || got.Text != "" {
		t.Fatalf("older action weakened cutoff: %+v %v", got, err)
	}
}

func TestConcurrentRemovalAndHistoryImport(t *testing.T) {
	a := testArchive(t)
	m := fixture("concurrent")
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var err error
			if i%2 == 0 {
				err = a.save(m)
			} else {
				err = a.removeChat(m.ChatID, time.Unix(m.Timestamp, 0), removalRange(m.Timestamp), true)
			}
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	got, err := a.getMessage(context.Background(), m.ChatID, m.ID)
	if err != nil || !got.Deleted || got.Text != "" {
		t.Fatalf("concurrent import bypassed removal: %+v %v", got, err)
	}
}

func TestChatRemovalTimeFallback(t *testing.T) {
	a := testArchive(t)
	m := fixture("old")
	mustSave(t, a, m)
	if err := a.removeChat(m.ChatID, time.Unix(m.Timestamp, 0), nil, false); err != nil {
		t.Fatal(err)
	}
	got, err := a.getMessage(context.Background(), m.ChatID, m.ID)
	if err != nil || !got.Deleted {
		t.Fatalf("event-time fallback failed: %+v %v", got, err)
	}
	if err = a.removeChat(m.ChatID, time.Time{}, nil, true); err == nil {
		t.Fatal("missing removal boundary accepted")
	}
}

func TestUpgradePreservesExistingArchive(t *testing.T) {
	dir := t.TempDir()
	a, err := openArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	m := fixture("before-upgrade")
	mustSave(t, a, m)
	if err = a.setMeta("account", "999@s.whatsapp.net"); err != nil {
		t.Fatal(err)
	}
	// Reproduce the previous schema without touching any real account data.
	if _, err = a.db.Exec("DROP TABLE chat_removals; ALTER TABLE messages DROP COLUMN wire_chat; ALTER TABLE messages DROP COLUMN wire_sender; PRAGMA user_version=1"); err != nil {
		t.Fatal(err)
	}
	a.db.Close()
	a, err = openArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer a.db.Close()
	got, err := a.getMessage(context.Background(), m.ChatID, m.ID)
	if err != nil || got.Text != m.Text {
		t.Fatalf("upgrade lost existing message: %+v %v", got, err)
	}
	if a.meta("account") != "999@s.whatsapp.net" {
		t.Fatal("upgrade lost account binding")
	}
	var version int
	if err = a.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 5 {
		t.Fatalf("schema version %d: %v", version, err)
	}
	if err = a.removeChat(m.ChatID, time.Unix(m.Timestamp, 0), removalRange(m.Timestamp), false); err != nil {
		t.Fatal(err)
	}
}
