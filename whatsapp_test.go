package main

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func eventFixture() *events.Message {
	return &events.Message{Info: types.MessageInfo{MessageSource: types.MessageSource{Chat: types.NewJID("123", types.DefaultUserServer), Sender: types.NewJID("123", types.DefaultUserServer)}, ID: "original", Timestamp: time.Unix(1700000000, 0)}, Message: &waE2E.Message{Conversation: proto.String("text")}}
}

func TestDecodeRealWhatsAppShapes(t *testing.T) {
	t.Run("text", func(t *testing.T) {
		m, ok := decodeMessage(eventFixture())
		if !ok || m.Text != "text" || m.Kind != "text" {
			t.Fatalf("%+v", m)
		}
	})
	t.Run("caption", func(t *testing.T) {
		e := eventFixture()
		e.Message = &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Caption: proto.String("caption")}}
		m, ok := decodeMessage(e)
		if !ok || m.Text != "caption" || m.Kind != "image" {
			t.Fatalf("%+v", m)
		}
	})
	t.Run("view once", func(t *testing.T) {
		e := eventFixture()
		e.IsViewOnce = true
		m, ok := decodeMessage(e)
		if !ok || m.Text != "" || !m.Deleted {
			t.Fatalf("view once leaked: %+v", m)
		}
	})
	t.Run("ephemeral", func(t *testing.T) {
		e := eventFixture()
		e.IsEphemeral = true
		e.Message = &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("temporary"), ContextInfo: &waE2E.ContextInfo{Expiration: proto.Uint32(60)}}}
		m, ok := decodeMessage(e)
		if !ok || m.Expires != 1700000060 {
			t.Fatalf("%+v", m)
		}
	})
	t.Run("unknown expiry", func(t *testing.T) {
		e := eventFixture()
		e.IsEphemeral = true
		m, ok := decodeMessage(e)
		if !ok || m.Text != "" || !m.Deleted {
			t.Fatalf("unknown ephemeral retained: %+v", m)
		}
	})
	t.Run("history expiry", func(t *testing.T) {
		e := eventFixture()
		e.SourceWebMsg = &waWeb.WebMessageInfo{EphemeralDuration: proto.Uint32(60), EphemeralStartTimestamp: proto.Uint64(1700000010)}
		m, ok := decodeMessage(e)
		if !ok || m.Expires != 1700000070 {
			t.Fatalf("%+v", m)
		}
	})
	t.Run("edit", func(t *testing.T) {
		e := eventFixture()
		e.Message = &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(), Key: &waCommon.MessageKey{ID: proto.String("target")}, EditedMessage: &waE2E.Message{Conversation: proto.String("new")}, TimestampMS: proto.Int64(1700000000500)}}
		m, ok := decodeMessage(e)
		if !ok || m.ID != "target" || m.Text != "new" || !m.Edited || m.Revision != 1700000000500 {
			t.Fatalf("%+v", m)
		}
	})
	t.Run("revoke", func(t *testing.T) {
		e := eventFixture()
		e.Message = &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_REVOKE.Enum(), Key: &waCommon.MessageKey{ID: proto.String("target")}}}
		m, ok := decodeMessage(e)
		if !ok || m.ID != "target" || m.Text != "" || !m.Deleted {
			t.Fatalf("%+v", m)
		}
	})
	t.Run("noncontent protocol", func(t *testing.T) {
		e := eventFixture()
		e.Message = &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: waE2E.ProtocolMessage_HISTORY_SYNC_NOTIFICATION.Enum()}}
		if _, ok := decodeMessage(e); ok {
			t.Fatal("indexed control message")
		}
	})
}
