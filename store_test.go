package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func testArchive(t *testing.T) *archive {
	t.Helper()
	a, err := openArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.db.Close() })
	return a
}
func mustSave(t *testing.T, a *archive, m message) {
	t.Helper()
	if err := a.save(m); err != nil {
		t.Fatal(err)
	}
}
func fixture(id string) message {
	return message{ChatID: "123@s.whatsapp.net", ID: id, Sender: "123@s.whatsapp.net", Timestamp: 1700000000, Text: "hello", Kind: "text"}
}

func TestArchiveEditsDeletesAndReplay(t *testing.T) {
	a := testArchive(t)
	ctx := context.Background()
	m := fixture("one")
	mustSave(t, a, m)
	mustSave(t, a, m)
	edit := m
	edit.Text = "new content"
	edit.Edited = true
	edit.Revision = 20
	mustSave(t, a, edit)
	stale := edit
	stale.Text = "older edit"
	stale.Revision = 10
	mustSave(t, a, stale)
	mustSave(t, a, m)
	p, err := a.messages(ctx, listInput{})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Items) != 1 || p.Items[0].Text != "new content" || !p.Items[0].Edited {
		t.Fatalf("edit replay regressed: %+v", p)
	}
	deleted := m
	deleted.Deleted = true
	deleted.Text = ""
	deleted.Kind = "deleted"
	mustSave(t, a, deleted)
	mustSave(t, a, edit)
	mustSave(t, a, m)
	got, err := a.getMessage(ctx, m.ChatID, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Text != "" || !got.Deleted {
		t.Fatalf("deleted body resurrected: %+v", got)
	}
	before := fixture("deleted-before-original")
	before.Deleted = true
	before.Text = ""
	mustSave(t, a, before)
	mustSave(t, a, fixture("deleted-before-original"))
	got, err = a.getMessage(ctx, before.ChatID, before.ID)
	if err != nil || !got.Deleted || got.Text != "" {
		t.Fatalf("out of order deletion lost: %+v %v", got, err)
	}
}

func TestExpirationAndSearch(t *testing.T) {
	a := testArchive(t)
	m := fixture("expired")
	m.Expires = time.Now().Unix() - 1
	m.Text = "secret"
	mustSave(t, a, m)
	m = fixture("future")
	m.Text = "short lived"
	m.Expires = time.Now().Unix() + 100
	mustSave(t, a, m)
	if _, err := a.db.Exec("UPDATE messages SET expires=? WHERE id='future'", time.Now().Unix()-1); err != nil {
		t.Fatal(err)
	}
	p, err := a.messages(context.Background(), listInput{})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range p.Items {
		if !m.Deleted || m.Text != "" {
			t.Fatalf("expired message leaked: %+v", m)
		}
	}
	p, err = a.messages(context.Background(), listInput{Query: "secret"})
	if err != nil || len(p.Items) != 0 {
		t.Fatalf("expired search result: %+v %v", p, err)
	}
	for id, text := range map[string]string{"percent": "100% done", "ordinary": "1000 done", "underscore": "a_b", "backslash": `a\b`} {
		m := fixture(id)
		m.Text = text
		mustSave(t, a, m)
	}
	for _, query := range []string{"%", "_", `\`} {
		p, err = a.messages(context.Background(), listInput{Query: query})
		if err != nil || len(p.Items) != 1 {
			t.Fatalf("literal search %q: %+v %v", query, p, err)
		}
	}
	_, err = a.messages(context.Background(), listInput{After: "invalid"})
	if err == nil {
		t.Fatal("invalid date accepted")
	}
}

func TestFiltersPaginationAndEmptyResults(t *testing.T) {
	a := testArchive(t)
	ctx := context.Background()
	p, err := a.messages(ctx, listInput{})
	if err != nil || p.Items == nil || len(p.Items) != 0 {
		t.Fatalf("empty page: %+v %v", p, err)
	}
	for _, id := range []string{"a", "b", "c"} {
		mustSave(t, a, fixture(id))
	}
	p, err = a.messages(ctx, listInput{Limit: 2})
	if err != nil || len(p.Items) != 2 || p.NextOffset == nil || *p.NextOffset != 2 {
		t.Fatalf("first page: %+v %v", p, err)
	}
	q, err := a.messages(ctx, listInput{Limit: 2, Offset: 2})
	if err != nil || len(q.Items) != 1 || q.NextOffset != nil {
		t.Fatalf("second page: %+v %v", q, err)
	}
	if p.Items[0].ID == q.Items[0].ID || p.Items[1].ID == q.Items[0].ID {
		t.Fatal("pagination repeats messages")
	}
	p, err = a.messages(ctx, listInput{ChatID: "other", Query: "hello"})
	if err != nil || len(p.Items) != 0 {
		t.Fatal("chat filter ignored")
	}
	p, err = a.messages(ctx, listInput{After: time.Unix(1700000000, 0).UTC().Format(time.RFC3339), Before: time.Unix(1700000001, 0).UTC().Format(time.RFC3339)})
	if err != nil || len(p.Items) != 3 {
		t.Fatalf("date bounds: %v %v", p, err)
	}
	for _, in := range []listInput{{Limit: 201}, {Limit: -1}, {Offset: -1}, {Offset: 100001}} {
		if _, err = a.messages(ctx, in); err == nil {
			t.Fatalf("bad pagination accepted %+v", in)
		}
	}
}

func TestAddressAliasMigration(t *testing.T) {
	a := testArchive(t)
	m := fixture("same")
	m.ChatID = "99@lid"
	m.Sender = "99@lid"
	m.Text = "edited via lid"
	m.Edited = true
	m.Revision = 9
	m.Expires = time.Now().Unix() + 100
	mustSave(t, a, m)
	mustSave(t, a, fixture("same"))
	if err := a.saveTimer("99@lid", 100, 10); err != nil {
		t.Fatal(err)
	}
	if err := a.alias("99@lid", "123@s.whatsapp.net"); err != nil {
		t.Fatal(err)
	}
	p, err := a.messages(context.Background(), listInput{ChatID: "99@lid"})
	if err != nil || len(p.Items) != 1 {
		t.Fatalf("alias lookup %+v %v", p, err)
	}
	got := p.Items[0]
	if got.Text != m.Text || got.Sender != "123@s.whatsapp.net" || got.Expires != m.Expires {
		t.Fatalf("alias merge lost data: %+v", got)
	}
	if a.timer("123@s.whatsapp.net") != 100 {
		t.Fatal("timer lost during alias migration")
	}
	m.Deleted = true
	m.Text = ""
	mustSave(t, a, m)
	mustSave(t, a, fixture("same"))
	got, err = a.getMessage(context.Background(), "123@s.whatsapp.net", "same")
	if err != nil || !got.Deleted || got.Text != "" {
		t.Fatalf("alias resurrected delete: %+v %v", got, err)
	}
}

func TestRestartPreservesArchiveAndUncertainSends(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	if err := privateDir(dir); err != nil {
		t.Fatal(err)
	}
	a, err := openArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustSave(t, a, fixture("persistent"))
	_, err = a.db.Exec("INSERT INTO outbox VALUES('request-1','fingerprint','123@s.whatsapp.net','msg','sending',1)")
	if err != nil {
		t.Fatal(err)
	}
	a.db.Close()
	a, err = openArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer a.db.Close()
	p, err := a.messages(context.Background(), listInput{})
	if err != nil || len(p.Items) != 1 {
		t.Fatalf("archive lost after restart: %+v %v", p, err)
	}
	var state string
	if err = a.db.QueryRow("SELECT state FROM outbox WHERE request_id='request-1'").Scan(&state); err != nil || state != "uncertain" {
		t.Fatalf("crash reservation: %s %v", state, err)
	}
}
