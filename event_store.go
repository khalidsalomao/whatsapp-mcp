package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"time"
)

const eventSchema = `
CREATE TABLE IF NOT EXISTS event_subscriptions (
 id TEXT PRIMARY KEY, owner TEXT NOT NULL, argument_chat TEXT NOT NULL,
 chat TEXT NOT NULL, url TEXT NOT NULL, secret TEXT NOT NULL,
 old_secret TEXT NOT NULL DEFAULT '', old_until INTEGER NOT NULL DEFAULT 0,
 expires INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS event_deliveries (
 subscription TEXT NOT NULL REFERENCES event_subscriptions(id) ON DELETE CASCADE,
 event_id TEXT NOT NULL, chat TEXT NOT NULL, message_id TEXT NOT NULL,
 payload BLOB NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
 next_attempt INTEGER NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
 PRIMARY KEY(subscription,event_id));
CREATE INDEX IF NOT EXISTS event_delivery_due ON event_deliveries(state,next_attempt);
`

const messageEventName = "message.created"

func stableID(prefix string, parts ...string) string {
	b, _ := json.Marshal(parts)
	sum := sha256.Sum256(b)
	return prefix + hex.EncodeToString(sum[:])
}

type messageEventData struct {
	ChatID    string `json:"chat_id"`
	MessageID string `json:"message_id"`
	SenderID  string `json:"sender_id"`
	Timestamp int64  `json:"timestamp_unix"`
	Kind      string `json:"kind"`
}

type messageEvent struct {
	ID        string           `json:"eventId"`
	Name      string           `json:"name"`
	Timestamp string           `json:"timestamp"`
	Data      messageEventData `json:"data"`
	Cursor    *string          `json:"cursor"`
}

func enqueueMessageEvent(tx *sql.Tx, m message) error {
	var owner string
	err := tx.QueryRow("SELECT value FROM meta WHERE key='account'").Scan(&owner)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	id := stableID("evt_", owner, m.ChatID, m.ID)
	event := messageEvent{ID: id, Name: messageEventName, Timestamp: time.Unix(m.Timestamp, 0).UTC().Format(time.RFC3339),
		Data: messageEventData{ChatID: m.ChatID, MessageID: m.ID, SenderID: m.Sender, Timestamp: m.Timestamp, Kind: m.Kind}}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	now := time.Now().UnixMilli()
	_, err = tx.Exec(`INSERT OR IGNORE INTO event_deliveries
 (subscription,event_id,chat,message_id,payload,next_attempt)
 SELECT id,?,?,?,?,? FROM event_subscriptions WHERE owner=? AND chat=? AND expires>?`,
		id, m.ChatID, m.ID, payload, now, owner, m.ChatID, now)
	if err != nil {
		return err
	}
	return enqueueReplyJobs(tx, owner, id, m)
}
