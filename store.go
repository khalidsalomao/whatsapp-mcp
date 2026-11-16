package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type archive struct {
	db       *sql.DB
	mediaDir string
	// Serialize multi-statement writes with alias migration and chat removal.
	writeMu sync.Mutex
}

type message struct {
	ChatID     string `json:"chat_id"`
	ID         string `json:"message_id"`
	Sender     string `json:"sender_id"`
	FromMe     bool   `json:"from_me"`
	Timestamp  int64  `json:"timestamp_unix"`
	Text       string `json:"text"`
	Kind       string `json:"kind"`
	Deleted    bool   `json:"deleted"`
	Edited     bool   `json:"edited"`
	Expires    int64  `json:"expires_at_unix,omitempty"`
	Revision   int64  `json:"-"`
	wireChat   string
	wireSender string
}

type listInput struct {
	ChatID string `json:"chat_id,omitempty" jsonschema:"Exact chat ID returned by list_chats or search_contacts"`
	Query  string `json:"query,omitempty" jsonschema:"Literal case-insensitive substring"`
	Sender string `json:"sender_id,omitempty"`
	After  string `json:"after,omitempty" jsonschema:"Inclusive RFC3339 timestamp"`
	Before string `json:"before,omitempty" jsonschema:"Exclusive RFC3339 timestamp"`
	Limit  int    `json:"limit,omitempty" jsonschema:"Page size 1 to 200; default 50"`
	Offset int    `json:"offset,omitempty" jsonschema:"Pagination offset; default 0"`
}

type page[T any] struct {
	Items      []T  `json:"items"`
	NextOffset *int `json:"next_offset"`
}

type chat struct {
	ID          string `json:"chat_id"`
	Name        string `json:"name"`
	LastMessage int64  `json:"last_message_unix"`
}
type contact struct {
	ID       string `json:"recipient_id"`
	Name     string `json:"name"`
	PushName string `json:"push_name"`
}

func sqliteDSN(path string) string {
	u := url.URL{Scheme: "file", Path: path}
	return u.String() + "?_foreign_keys=on&_busy_timeout=5000&_journal_mode=WAL&_secure_delete=on"
}

func openArchive(dir string) (*archive, error) {
	db, err := sql.Open("sqlite3", sqliteDSN(filepath.Join(dir, "messages.db")))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	mediaDir := filepath.Join(dir, "outgoing")
	if err = os.MkdirAll(mediaDir, 0700); err == nil {
		err = initializeArchive(db)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return &archive{db: db, mediaDir: mediaDir}, nil
}

func initializeArchive(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var previous int
	if err = tx.QueryRow("PRAGMA user_version").Scan(&previous); err != nil {
		return err
	}
	if previous > 5 {
		return fmt.Errorf("archive schema %d requires a newer service", previous)
	}
	_, err = tx.Exec(`
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS contacts (jid TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '', push_name TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS chats (jid TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS aliases (jid TEXT PRIMARY KEY, canonical TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS chat_timers (jid TEXT PRIMARY KEY, duration INTEGER NOT NULL, updated INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS chat_removals (
 chat TEXT PRIMARY KEY, cleared_through INTEGER NOT NULL, deleted_through INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS messages (
 chat TEXT NOT NULL, id TEXT NOT NULL, sender TEXT NOT NULL, from_me INTEGER NOT NULL,
 ts INTEGER NOT NULL, body TEXT NOT NULL, kind TEXT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0,
 edited INTEGER NOT NULL DEFAULT 0, expires INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(chat,id));
CREATE INDEX IF NOT EXISTS messages_chat_time ON messages(chat,ts DESC,id);
CREATE INDEX IF NOT EXISTS messages_time ON messages(ts DESC,id);
CREATE INDEX IF NOT EXISTS messages_expiry ON messages(expires) WHERE expires>0;
CREATE TABLE IF NOT EXISTS outbox (
 request_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, chat TEXT NOT NULL, message_id TEXT NOT NULL,
 state TEXT NOT NULL, created INTEGER NOT NULL);
UPDATE outbox SET state='uncertain' WHERE state='sending';
` + eventSchema + monitorSchema)
	if err != nil {
		return err
	}
	if previous < 5 {
		// A v4 draft was arbitrary user text, including text that looks like JSON.
		_, err = tx.Exec(`ALTER TABLE messages ADD COLUMN wire_chat TEXT NOT NULL DEFAULT '';
ALTER TABLE messages ADD COLUMN wire_sender TEXT NOT NULL DEFAULT '';
UPDATE reply_jobs SET draft=json_object('draft',draft) WHERE draft<>'';
PRAGMA user_version=5;`)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (a *archive) meta(key string) string {
	var value string
	_ = a.db.QueryRow("SELECT value FROM meta WHERE key=?", key).Scan(&value)
	return value
}
func (a *archive) setMeta(key, value string) error {
	_, err := a.db.Exec("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, value)
	return err
}

func (a *archive) canonical(id string) string {
	var out string
	if a.db.QueryRow("SELECT canonical FROM aliases WHERE jid=?", id).Scan(&out) == nil {
		return out
	}
	return id
}

// A newly learned LID/phone mapping also migrates earlier cached messages.
func (a *archive) alias(from, to string) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if from == to || from == "" || to == "" {
		return nil
	}
	tx, err := a.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{"INSERT INTO aliases VALUES(?,?) ON CONFLICT(jid) DO UPDATE SET canonical=excluded.canonical", []any{from, to}},
		{"INSERT OR IGNORE INTO chats SELECT ?,name FROM chats WHERE jid=?", []any{to, from}},
		{`INSERT INTO contacts SELECT ?,name,push_name FROM contacts WHERE jid=?
 ON CONFLICT(jid) DO UPDATE SET name=CASE WHEN contacts.name='' THEN excluded.name ELSE contacts.name END,
 push_name=CASE WHEN contacts.push_name='' THEN excluded.push_name ELSE contacts.push_name END`, []any{to, from}},
		{`INSERT INTO chat_removals SELECT ?,cleared_through,deleted_through FROM chat_removals WHERE chat=?
 ON CONFLICT(chat) DO UPDATE SET cleared_through=MAX(chat_removals.cleared_through,excluded.cleared_through),
 deleted_through=MAX(chat_removals.deleted_through,excluded.deleted_through)`, []any{to, from}},
		{`INSERT INTO chat_timers SELECT ?,duration,updated FROM chat_timers WHERE jid=?
 ON CONFLICT(jid) DO UPDATE SET duration=excluded.duration,updated=excluded.updated WHERE excluded.updated>chat_timers.updated`, []any{to, from}},
		// Preserve tombstones and newer edits when both address forms already exist.
		{`INSERT INTO messages SELECT ?,id,sender,from_me,ts,body,kind,deleted,edited,expires,revision,COALESCE(NULLIF(wire_chat,''),chat),wire_sender FROM messages WHERE chat=?
 ON CONFLICT(chat,id) DO UPDATE SET body=CASE WHEN messages.deleted OR excluded.deleted THEN '' WHEN excluded.revision>messages.revision THEN excluded.body ELSE messages.body END,
 kind=CASE WHEN messages.deleted THEN messages.kind WHEN excluded.deleted OR excluded.revision>messages.revision THEN excluded.kind ELSE messages.kind END,
 deleted=MAX(messages.deleted,excluded.deleted), edited=MAX(messages.edited,excluded.edited),
 revision=MAX(messages.revision,excluded.revision),
 wire_chat=COALESCE(NULLIF(messages.wire_chat,''),excluded.wire_chat),
 wire_sender=COALESCE(NULLIF(messages.wire_sender,''),excluded.wire_sender),
 expires=CASE WHEN messages.expires=0 THEN excluded.expires WHEN excluded.expires=0 THEN messages.expires ELSE MIN(messages.expires,excluded.expires) END`, []any{to, from}},
		{"UPDATE messages SET wire_sender=COALESCE(NULLIF(wire_sender,''),sender),sender=? WHERE sender=?", []any{to, from}},
		{"UPDATE event_subscriptions SET chat=? WHERE chat=?", []any{to, from}},
		{"UPDATE event_deliveries SET chat=? WHERE chat=?", []any{to, from}},
		// If both address forms have responders, stop both rather than choose
		// one user's standing instructions during address reconciliation.
		{`UPDATE monitors SET active=0 WHERE chat IN (?,?) AND active=1 AND
 EXISTS(SELECT 1 FROM monitors x WHERE x.chat=? AND x.active=1) AND
 EXISTS(SELECT 1 FROM monitors y WHERE y.chat=? AND y.active=1)`, []any{from, to, from, to}},
		{"UPDATE monitors SET chat=? WHERE chat=?", []any{to, from}},
		{"UPDATE reply_jobs SET chat=? WHERE chat=?", []any{to, from}},
		{`UPDATE reply_jobs SET state='cancelled',draft='',claim_token='',lease_until=0
 WHERE state IN ('pending','claimed','drafted') AND monitor_id IN (SELECT id FROM monitors WHERE active=0)`, nil},
		{`UPDATE messages SET body='',kind='deleted',deleted=1 WHERE chat=? AND ts<=
 (SELECT cleared_through FROM chat_removals WHERE chat=?)`, []any{to, to}},
		{"DELETE FROM messages WHERE chat=?", []any{from}},
		{"DELETE FROM chats WHERE jid=?", []any{from}},
		{"DELETE FROM contacts WHERE jid=?", []any{from}},
		{"DELETE FROM chat_timers WHERE jid=?", []any{from}},
		{"DELETE FROM chat_removals WHERE chat=?", []any{from}},
	} {
		if _, err = tx.Exec(q.sql, q.args...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (a *archive) saveTimer(id string, duration uint32, updated int64) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	_, err := a.db.Exec(`INSERT INTO chat_timers VALUES(?,?,?) ON CONFLICT(jid) DO UPDATE SET duration=excluded.duration,updated=excluded.updated WHERE excluded.updated>=chat_timers.updated`, a.canonical(id), duration, updated)
	return err
}

func (a *archive) timer(id string) uint32 {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	var duration uint32
	_ = a.db.QueryRow("SELECT duration FROM chat_timers WHERE jid=?", a.canonical(id)).Scan(&duration)
	return duration
}

func (a *archive) saveChat(id, name string) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	return a.saveChatLocked(id, name)
}

func (a *archive) saveChatLocked(id, name string) error {
	id = a.canonical(id)
	_, err := a.db.Exec(`INSERT INTO chats VALUES(?,?) ON CONFLICT(jid) DO UPDATE SET name=CASE WHEN excluded.name!='' THEN excluded.name ELSE chats.name END`, id, name)
	return err
}
func (a *archive) saveContact(c contact) error {
	return a.writeContact(c, false)
}

// A sync snapshot may contain only a push name for an alias. Only explicit
// contact updates (saveContact) are allowed to clear the saved contact name.
func (a *archive) mergeContact(c contact) error {
	return a.writeContact(c, true)
}

func (a *archive) writeContact(c contact, preserveName bool) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	c.ID = a.canonical(c.ID)
	_, err := a.db.Exec(`INSERT INTO contacts VALUES(?,?,?) ON CONFLICT(jid) DO UPDATE SET
 name=CASE WHEN ? AND excluded.name='' THEN contacts.name ELSE excluded.name END,
 push_name=CASE WHEN excluded.push_name!='' THEN excluded.push_name ELSE contacts.push_name END`, c.ID, c.Name, c.PushName, preserveName)
	return err
}

func (a *archive) save(m message) error {
	return a.saveObserved(m, false)
}

// The message and its event reservations commit together. History imports use
// live=false; a duplicate, edit, tombstone or outgoing message never triggers.
func (a *archive) saveObserved(m message, live bool) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	chat, sender := a.canonical(m.ChatID), a.canonical(m.Sender)
	if chat != m.ChatID && m.wireChat == "" {
		m.wireChat = m.ChatID
	}
	if sender != m.Sender && m.wireSender == "" {
		m.wireSender = m.Sender
	}
	m.ChatID, m.Sender = chat, sender
	tx, err := a.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var existed int
	if err = tx.QueryRow("SELECT COUNT(*) FROM messages WHERE chat=? AND id=?", m.ChatID, m.ID).Scan(&existed); err != nil {
		return err
	}
	var cutoff int64
	err = tx.QueryRow("SELECT cleared_through FROM chat_removals WHERE chat=?", m.ChatID).Scan(&cutoff)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if cutoff > 0 && m.Timestamp <= cutoff {
		m.Text = ""
		m.Kind = "deleted"
		m.Deleted = true
	}
	if m.Expires > 0 && m.Expires <= time.Now().Unix() {
		m.Text = ""
		m.Kind = "expired"
		m.Deleted = true
	}
	if _, err = tx.Exec("INSERT OR IGNORE INTO chats VALUES(?,'')", m.ChatID); err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
 ON CONFLICT(chat,id) DO UPDATE SET
 body=CASE WHEN messages.deleted OR excluded.deleted THEN '' WHEN excluded.revision>messages.revision THEN excluded.body ELSE messages.body END,
 kind=CASE WHEN messages.deleted THEN messages.kind WHEN excluded.deleted OR excluded.revision>messages.revision THEN excluded.kind ELSE messages.kind END,
 deleted=MAX(messages.deleted,excluded.deleted), edited=MAX(messages.edited,excluded.edited),
 revision=MAX(messages.revision,excluded.revision),
 ts=CASE WHEN messages.edited AND NOT excluded.edited THEN excluded.ts ELSE messages.ts END,
 wire_chat=COALESCE(NULLIF(messages.wire_chat,''),excluded.wire_chat),
 wire_sender=COALESCE(NULLIF(messages.wire_sender,''),excluded.wire_sender),
 expires=CASE WHEN messages.expires=0 THEN excluded.expires WHEN excluded.expires=0 THEN messages.expires ELSE MIN(messages.expires,excluded.expires) END`,
		m.ChatID, m.ID, m.Sender, m.FromMe, m.Timestamp, m.Text, m.Kind, m.Deleted, m.Edited, m.Expires, m.Revision, m.wireChat, m.wireSender)
	if err != nil {
		return err
	}
	if live && existed == 0 && !m.FromMe && !m.Deleted && !m.Edited {
		if err = enqueueMessageEvent(tx, m); err != nil {
			return err
		}
	}
	if m.Deleted {
		if err = cleanJobs(tx, time.Now().Unix()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (a *archive) expire() error {
	_, err := a.db.Exec("UPDATE messages SET body='',kind='expired',deleted=1 WHERE expires>0 AND expires<=? AND deleted=0", time.Now().Unix())
	if err != nil {
		return err
	}
	return cleanJobs(a.db, time.Now().Unix())
}

func validateList(in *listInput) error {
	if in.Limit == 0 {
		in.Limit = 50
	}
	if in.Limit < 1 || in.Limit > 200 || in.Offset < 0 || in.Offset > 100000 {
		return errors.New("limit must be 1..200 and offset 0..100000")
	}
	if len(in.Query) > 500 {
		return errors.New("query is too long")
	}
	return nil
}
func like(s string) string {
	return "%" + strings.NewReplacer(`\`, `\\`, "%", `\%`, "_", `\_`).Replace(s) + "%"
}
func finishPage[T any](items []T, in listInput) page[T] {
	p := page[T]{Items: items}
	if len(items) > in.Limit {
		n := in.Offset + in.Limit
		p.NextOffset = &n
		p.Items = items[:in.Limit]
	}
	return p
}

func (a *archive) messages(ctx context.Context, in listInput) (page[message], error) {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if err := validateList(&in); err != nil {
		return page[message]{}, err
	}
	if err := a.expire(); err != nil {
		return page[message]{}, err
	}
	q := "SELECT chat,id,sender,from_me,ts,body,kind,deleted,edited,expires FROM messages WHERE 1=1"
	args := []any{}
	if in.ChatID != "" {
		q += " AND chat=?"
		args = append(args, a.canonical(in.ChatID))
	}
	if in.Sender != "" {
		q += " AND sender=?"
		args = append(args, a.canonical(in.Sender))
	}
	if in.Query != "" {
		q += ` AND deleted=0 AND body LIKE ? ESCAPE '\'`
		args = append(args, like(in.Query))
	}
	for _, filter := range []struct{ value, op string }{{in.After, ">="}, {in.Before, "<"}} {
		if filter.value != "" {
			t, err := time.Parse(time.RFC3339, filter.value)
			if err != nil {
				return page[message]{}, errors.New("dates must use RFC3339")
			}
			q += " AND ts" + filter.op + "?"
			args = append(args, t.Unix())
		}
	}
	q += " ORDER BY ts DESC,chat,id DESC LIMIT ? OFFSET ?"
	args = append(args, in.Limit+1, in.Offset)
	rows, err := a.db.QueryContext(ctx, q, args...)
	if err != nil {
		return page[message]{}, err
	}
	defer rows.Close()
	items := []message{}
	for rows.Next() {
		var m message
		if err = rows.Scan(&m.ChatID, &m.ID, &m.Sender, &m.FromMe, &m.Timestamp, &m.Text, &m.Kind, &m.Deleted, &m.Edited, &m.Expires); err != nil {
			return page[message]{}, err
		}
		items = append(items, m)
	}
	return finishPage(items, in), rows.Err()
}

func (a *archive) contacts(ctx context.Context, in listInput) (page[contact], error) {
	if err := validateList(&in); err != nil {
		return page[contact]{}, err
	}
	rows, err := a.db.QueryContext(ctx, `SELECT jid,name,push_name FROM contacts WHERE name LIKE ? ESCAPE '\' OR push_name LIKE ? ESCAPE '\' OR jid LIKE ? ESCAPE '\' ORDER BY name,jid LIMIT ? OFFSET ?`, like(in.Query), like(in.Query), like(in.Query), in.Limit+1, in.Offset)
	if err != nil {
		return page[contact]{}, err
	}
	defer rows.Close()
	items := []contact{}
	for rows.Next() {
		var c contact
		if err = rows.Scan(&c.ID, &c.Name, &c.PushName); err != nil {
			return page[contact]{}, err
		}
		items = append(items, c)
	}
	return finishPage(items, in), rows.Err()
}
func (a *archive) chats(ctx context.Context, in listInput) (page[chat], error) {
	if err := validateList(&in); err != nil {
		return page[chat]{}, err
	}
	rows, err := a.db.QueryContext(ctx, `SELECT c.jid,COALESCE(NULLIF(c.name,''),NULLIF(p.name,''),p.push_name,''),COALESCE(MAX(m.ts),0)
 FROM chats c LEFT JOIN contacts p ON p.jid=c.jid LEFT JOIN messages m ON m.chat=c.jid
 WHERE (c.jid LIKE ? ESCAPE '\' OR c.name LIKE ? ESCAPE '\' OR p.name LIKE ? ESCAPE '\' OR p.push_name LIKE ? ESCAPE '\')
 AND NOT EXISTS (SELECT 1 FROM chat_removals r WHERE r.chat=c.jid AND r.deleted_through>0
 AND NOT EXISTS (SELECT 1 FROM messages newer WHERE newer.chat=c.jid AND newer.ts>r.deleted_through))
 GROUP BY c.jid ORDER BY 3 DESC,c.jid LIMIT ? OFFSET ?`, like(in.Query), like(in.Query), like(in.Query), like(in.Query), in.Limit+1, in.Offset)
	if err != nil {
		return page[chat]{}, err
	}
	defer rows.Close()
	items := []chat{}
	for rows.Next() {
		var c chat
		if err = rows.Scan(&c.ID, &c.Name, &c.LastMessage); err != nil {
			return page[chat]{}, err
		}
		items = append(items, c)
	}
	return finishPage(items, in), rows.Err()
}

func (a *archive) getMessage(ctx context.Context, chatID, id string) (message, error) {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	if err := a.expire(); err != nil {
		return message{}, err
	}
	var m message
	err := a.db.QueryRowContext(ctx, `SELECT chat,id,sender,from_me,ts,body,kind,deleted,edited,expires,
 COALESCE(NULLIF(wire_chat,''),chat),COALESCE(NULLIF(wire_sender,''),sender) FROM messages WHERE chat=? AND id=?`, a.canonical(chatID), id).Scan(&m.ChatID, &m.ID, &m.Sender, &m.FromMe, &m.Timestamp, &m.Text, &m.Kind, &m.Deleted, &m.Edited, &m.Expires, &m.wireChat, &m.wireSender)
	if errors.Is(err, sql.ErrNoRows) {
		return m, fmt.Errorf("reply message was not found in this chat")
	}
	return m, err
}
