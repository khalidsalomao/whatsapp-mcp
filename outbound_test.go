package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

func TestReactionTargetsAndValidation(t *testing.T) {
	b, pn, lid := contactSyncBridge(t)
	b.connectedFn = func() bool { return true }
	if !b.allowedAccount() {
		t.Fatal("fixture account missing")
	}
	e := eventFixture()
	e.Info.Chat, e.Info.Sender = lid, lid
	b.event(e)
	var sent *waE2E.Message
	b.sendFn = func(_ context.Context, dest types.JID, msg *waE2E.Message, _ whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		if dest != pn {
			t.Error("wrong chat destination")
		}
		sent = msg
		return whatsmeow.SendResponse{Timestamp: time.Now()}, nil
	}
	in := sendInput{Recipient: pn.String(), RequestID: "react-remote", ReplyTo: e.Info.ID, outboundContent: outboundContent{Reaction: "🇧🇷"}}
	if _, err := b.send(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	k := sent.GetReactionMessage().GetKey()
	if k.GetRemoteJID() != lid.String() || k.GetID() != e.Info.ID || k.GetFromMe() || k.GetParticipant() != "" {
		t.Fatalf("original DM key lost on alias mapping: %v", k)
	}
	ownMsg := fixture("mine")
	ownMsg.FromMe, ownMsg.Sender = true, b.a.meta("account")
	mustSave(t, b.a, ownMsg)
	in.ReplyTo, in.RequestID = ownMsg.ID, "react-to-own"
	if _, err := b.send(t.Context(), in); err != nil || !sent.GetReactionMessage().GetKey().GetFromMe() {
		t.Fatalf("reaction to outgoing message: %v %v", sent, err)
	}

	for _, valid := range []string{"👍🏽", "👩‍💻", "❤️", "❤", "🇧🇷", "1️⃣", "🫱🏻‍🫲🏿"} {
		if err := (outboundContent{Reaction: valid}).validate(); err != nil {
			t.Errorf("rejected emoji %q", valid)
		}
	}
	for _, invalid := range []string{"ok", "👍👋", "🏻", "👍🏽🏿", "\u200d", "A\uFE0F"} {
		if err := (outboundContent{Reaction: invalid}).validate(); err == nil {
			t.Errorf("accepted non-reaction %q", invalid)
		}
	}
	sent = nil
	other := fixture("only-in-other-chat")
	other.ChatID = "777@s.whatsapp.net"
	mustSave(t, b.a, other)
	removed := fixture("deleted")
	removed.Deleted = true
	mustSave(t, b.a, removed)
	expired := fixture("expired")
	expired.Expires = time.Now().Add(-time.Minute).Unix()
	mustSave(t, b.a, expired)
	for _, target := range []string{"", "missing", other.ID, removed.ID, expired.ID} {
		in.RequestID, in.ReplyTo = "bad-target", target
		if _, err := b.send(t.Context(), in); err == nil {
			t.Errorf("accepted unavailable target %q", target)
		}
	}
	if sent != nil {
		t.Fatal("dispatched an invalid reaction")
	}
}

func TestOutboundMediaValidation(t *testing.T) {
	b := testBridge(t)
	temp := t.TempDir()
	outside := filepath.Join(temp, "not-for-sending.txt")
	if err := os.WriteFile(outside, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(b.a.mediaDir, "link")); err != nil {
		t.Fatal(err)
	}
	writeMedia := func(name string, data []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(b.a.mediaDir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	writeMedia("oversize", nil)
	writeMedia("allowed.txt", []byte("hello"))
	writeMedia("wrong.jpg", []byte("%PDF-1.7\n"))
	if err := os.Truncate(filepath.Join(b.a.mediaDir, "oversize"), maxMediaBytes+1); err != nil {
		t.Fatal(err)
	}
	writeMedia("zero", nil)
	inputs := []outboundMedia{
		{Kind: "document", FileName: outside}, {Kind: "document", FileName: "../messages.db"},
		{Kind: "document", FileName: "link"}, {Kind: "document", FileName: "zero"}, {Kind: "document", FileName: "oversize"},
		{Kind: "image", FileName: "wrong.jpg"}, {Kind: "unknown", FileName: "allowed.txt"}, {Kind: "audio", FileName: "allowed.txt", Caption: "not supported"},
		{Kind: "image", FileName: "allowed.txt", MIMEType: "audio/mpeg"}, {Kind: "image", FileName: "allowed.txt", MIMEType: "image/*"},
		{Kind: "document", FileName: "allowed.txt", SHA256: "bad"}, {Kind: "document", FileName: "allowed.txt", SHA256: strings.Repeat("0", 64)},
		{Kind: "document", FileName: "allowed.txt", Caption: strings.Repeat("a", 1025)},
	}
	for _, m := range inputs {
		if _, err := b.a.readMedia(t.Context(), m, io.Discard); err == nil {
			t.Errorf("accepted invalid attachment: %+v", m)
		}
	}
	base := &outboundMedia{Kind: "document", FileName: "allowed.txt"}
	for _, c := range []outboundContent{{}, {Text: "x", Reaction: "👍"}, {Media: base, Reaction: "👍"}, {Media: base, Text: "x"}} {
		if err := c.validate(); err == nil {
			t.Fatalf("accepted competing content types: %+v", c)
		}
	}
	// Do not follow a replacement of the outgoing directory itself.
	if err := os.Rename(b.a.mediaDir, b.a.mediaDir+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(temp, b.a.mediaDir); err != nil {
		t.Fatal(err)
	}
	if _, err := b.a.readMedia(t.Context(), outboundMedia{Kind: "document", FileName: filepath.Base(outside)}, io.Discard); err == nil {
		t.Fatal("read through a symlinked outgoing directory")
	}
}

func TestMediaDraftRestartAndDeliveryFailures(t *testing.T) {
	b, _ := monitoredBridge(t)
	j := incomingJob(t, b, "new-file-request")
	ctx := t.Context()
	file := filepath.Join(b.a.mediaDir, "report.txt")
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(file, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("approved bytes")
	draft, err := b.draftReply(ctx, draftReplyInput{j.ID, j.ClaimToken, outboundContent{Media: &outboundMedia{Kind: "document", FileName: "report.txt"}}})
	if err != nil || draft.Media.SHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte("approved bytes"))) {
		t.Fatalf("draft did not bind file: %+v %v", draft, err)
	}
	dir := filepath.Dir(b.a.mediaDir)
	b.a.db.Close()
	b.a, err = openArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.a.db.Close()
	var uploads, sends int
	b.uploadFn = func(_ context.Context, r io.Reader, _ io.ReadWriteSeeker, _ whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
		uploads++
		if uploads == 1 {
			return whatsmeow.UploadResponse{}, errors.New("simulated CDN failure")
		}
		// The original file can change even during upload. Only the snapshot is sent.
		write("changed while uploading")
		data, err := io.ReadAll(r)
		if err != nil || string(data) != "approved bytes" {
			t.Error("upload changed after validation")
		}
		return syntheticUpload(data), nil
	}
	b.sendFn = func(context.Context, types.JID, *waE2E.Message, whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		sends++
		return whatsmeow.SendResponse{}, context.DeadlineExceeded
	}
	claim := jobClaimInput{j.ID, j.ClaimToken}
	write("replaced before sending")
	if _, err = b.sendReply(ctx, claim); err == nil || uploads != 0 || sends != 0 {
		t.Fatal("changed file was uploaded or sent")
	}
	if _, err = b.send(ctx, sendInput{Recipient: j.ChatID, RequestID: j.RequestID, outboundContent: outboundContent{Text: "bypass"}}); err == nil {
		t.Fatal("direct call used a monitored job's reserved identity")
	}
	write("approved bytes")
	if out, err := b.sendReply(ctx, claim); err == nil || out.State != "drafted" || uploads != 1 || sends != 0 {
		t.Fatalf("pre-dispatch failure: %+v %v", out, err)
	}
	for range 2 {
		out, err := b.sendReply(ctx, claim)
		if err != nil || out.State != "uncertain" || uploads != 2 || sends != 1 {
			t.Fatalf("uncertain media was retried: %+v %v", out, err)
		}
	}
}

func TestMonitorChangeDuringMediaUpload(t *testing.T) {
	for _, change := range []string{"edited", "deleted", "lease_expired", "revoked_on_failure"} {
		t.Run(change, func(t *testing.T) {
			b, _ := monitoredBridge(t)
			j := incomingJob(t, b, "attachment-source")
			ctx := t.Context()
			if err := os.WriteFile(filepath.Join(b.a.mediaDir, "a.txt"), []byte("document"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := b.draftReply(ctx, draftReplyInput{j.ID, j.ClaimToken, outboundContent{Media: &outboundMedia{Kind: "document", FileName: "a.txt"}}}); err != nil {
				t.Fatal(err)
			}
			want := "pending"
			b.uploadFn = func(context.Context, io.Reader, io.ReadWriteSeeker, whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
				switch change {
				case "lease_expired":
					_, err := b.a.db.Exec("UPDATE reply_jobs SET lease_until=0 WHERE id=?", j.ID)
					if err != nil {
						t.Fatal(err)
					}
				case "revoked_on_failure":
					if err := b.a.revokeMonitors(); err != nil {
						t.Fatal(err)
					}
					want = "cancelled"
					return whatsmeow.UploadResponse{}, errors.New("upload interrupted")
				default:
					m := fixture(j.MessageID)
					m.Edited, m.Deleted, m.Text, m.Revision = change == "edited", change == "deleted", "changed", 100
					mustSave(t, b.a, m)
					if m.Deleted {
						want = "skipped"
					}
				}
				return syntheticUpload([]byte("document")), nil
			}
			b.sendFn = func(context.Context, types.JID, *waE2E.Message, whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
				t.Fatal("sent invalidated attachment")
				return whatsmeow.SendResponse{}, nil
			}
			if _, err := b.sendReply(ctx, jobClaimInput{j.ID, j.ClaimToken}); err == nil {
				t.Fatal("accepted stale reply")
			}
			jobs, err := b.a.listJobs(ctx, jobsFilter{})
			if err != nil || len(jobs) != 1 || jobs[0].State != want || jobs[0].savedReply != (savedReply{}) {
				t.Fatalf("retained stale state: %+v %v", jobs, err)
			}
		})
	}
}

func TestUpgradeV4DraftAndTextSendIdentity(t *testing.T) {
	b, _ := monitoredBridge(t)
	j := incomingJob(t, b, "legacy")
	raw := `{"draft_reaction":"👍"}`
	dir := filepath.Dir(b.a.mediaDir)
	if _, err := b.a.db.Exec("UPDATE reply_jobs SET state='drafted',draft=? WHERE id=?", raw, j.ID); err != nil {
		t.Fatal(err)
	}
	legacy := `{"recipient_id":"123@s.whatsapp.net","text":"hello","request_id":"old-request","reply_to_message_id":"legacy"}`
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(legacy)))
	if _, err := b.a.db.Exec("INSERT INTO outbox VALUES(?,?,?,?,?,?)", "old-request", hash, j.ChatID, "previous-send", "acknowledged", time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.a.db.Exec("ALTER TABLE messages DROP COLUMN wire_chat; ALTER TABLE messages DROP COLUMN wire_sender; PRAGMA user_version=4"); err != nil {
		t.Fatal(err)
	}
	b.a.db.Close()
	var err error
	b.a, err = openArchive(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.a.db.Close()
	var sent int
	b.sendFn = func(_ context.Context, _ types.JID, m *waE2E.Message, _ whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		sent++
		if m.GetExtendedTextMessage().GetText() != raw || m.ReactionMessage != nil {
			t.Fatal("old text was reinterpreted as an action")
		}
		return whatsmeow.SendResponse{Timestamp: time.Now()}, nil
	}
	prev, err := b.send(t.Context(), sendInput{Recipient: j.ChatID, RequestID: "old-request", ReplyTo: "legacy", outboundContent: outboundContent{Text: "hello"}})
	if err != nil || prev.MessageID != "previous-send" || prev.State != "acknowledged" || sent != 0 {
		t.Fatalf("v4 idempotency lost: %+v %v", prev, err)
	}
	out, err := b.sendReply(t.Context(), jobClaimInput{j.ID, j.ClaimToken})
	if err != nil || out.Draft != raw || out.State != "sent" || sent != 1 {
		t.Fatalf("old draft: %+v %v", out, err)
	}
}

func TestDirectMediaRetryAfterUploadAndMissingFile(t *testing.T) {
	b := testBridge(t)
	ctx := t.Context()
	target := fixture("quote-this")
	mustSave(t, b.a, target)
	data := []byte("approved document")
	path := filepath.Join(b.a.mediaDir, "fixture.txt")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	in := sendInput{Recipient: target.ChatID, RequestID: "direct-media", ReplyTo: target.ID, outboundContent: outboundContent{
		Media: &outboundMedia{Kind: "document", FileName: "fixture.txt"},
	}}
	var uploads, sends int
	b.uploadFn = func(_ context.Context, r io.Reader, _ io.ReadWriteSeeker, typ whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
		uploads++
		bytes, err := io.ReadAll(r)
		up := syntheticUpload(bytes)
		if uploads == 1 {
			up.URL, up.DirectPath = "", ""
		}
		return up, err
	}
	b.sendFn = func(_ context.Context, _ types.JID, msg *waE2E.Message, _ whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		sends++
		doc := msg.GetDocumentMessage()
		if doc.GetFileName() != "fixture.txt" || doc.GetContextInfo().GetStanzaID() != target.ID {
			t.Fatal("document without caption lost filename or quote")
		}
		return whatsmeow.SendResponse{Timestamp: time.Now()}, nil
	}
	if _, err := b.send(ctx, in); err == nil || sends != 0 {
		t.Fatal("dispatched attachment without a locator")
	}
	sent, err := b.send(ctx, in)
	if err != nil || sent.State != "acknowledged" {
		t.Fatalf("safe retry: %+v %v", sent, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	retry, err := b.send(ctx, in)
	if err != nil || retry.MessageID != sent.MessageID || uploads != 2 || sends != 1 {
		t.Fatalf("repeat did not use outbox: %+v %v", retry, err)
	}
	in.outboundContent = outboundContent{Reaction: "👍"}
	if _, err := b.send(ctx, in); err == nil || sends != 1 {
		t.Fatal("reused a media request ID for another action")
	}
}
