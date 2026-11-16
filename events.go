package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type eventArguments struct {
	ChatID string `json:"chat_id"`
}
type eventDestination struct {
	Mode   string `json:"mode"`
	URL    string `json:"url"`
	Secret string `json:"secret,omitempty"`
}
type eventParams struct {
	mcp.ParamsBase
	Name      string           `json:"name,omitempty"`
	Arguments json.RawMessage  `json:"arguments,omitempty"`
	Delivery  eventDestination `json:"delivery"`
	Cursor    *string          `json:"cursor,omitempty"`
	TTL       json.RawMessage  `json:"ttlMs,omitempty"`
}
type eventSubscription struct {
	ID, Owner, ArgumentChat, Chat, URL, Secret, OldSecret string
	OldUntil, Expires                                     int64
}
type eventReply struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  *jsonrpc.Error  `json:"error,omitempty"`
}
type subscribeResult struct {
	ID            string  `json:"id"`
	RefreshBefore string  `json:"refreshBefore"`
	Cursor        *string `json:"cursor"`
	Truncated     bool    `json:"truncated"`
}

type eventHub struct {
	a      *archive
	active func() bool
	http   *http.Client
	// Serialize delivery with refresh/unsubscribe. An unsubscribe response is
	// returned only after any already-started delivery has finished.
	mu       sync.Mutex
	verified map[string]time.Time
	wakeup   chan struct{}
}

func newEventHub(a *archive, active func() bool) *eventHub {
	return &eventHub{a: a, active: active, http: webhookClient(), verified: map[string]time.Time{}, wakeup: make(chan struct{}, 1)}
}
func (h *eventHub) wake() {
	select {
	case h.wakeup <- struct{}{}:
	default:
	}
}

func eventFailure(code int64, message, reason string) *jsonrpc.Error {
	e := &jsonrpc.Error{Code: code, Message: message}
	if reason != "" {
		e.Data, _ = json.Marshal(map[string]string{"reason": reason})
	}
	return e
}

func strictJSON(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("expected one JSON value")
	}
	return nil
}

func eventCatalog() any {
	return map[string]any{"events": []any{map[string]any{
		"name":        messageEventName,
		"description": "A newly observed incoming WhatsApp message in one exact chat. Excludes history imports, edits, deletions and your own messages. Payload contains identifiers; use get_message to fetch current content. No replay cursor.",
		"delivery":    []string{"webhook"},
		"inputSchema": map[string]any{"type": "object", "properties": map[string]any{"chat_id": map[string]any{"type": "string", "description": "Exact chat ID returned by list_chats or search_contacts"}}, "required": []string{"chat_id"}, "additionalProperties": false},
		"payloadSchema": map[string]any{"type": "object", "properties": map[string]any{
			"chat_id": map[string]string{"type": "string"}, "message_id": map[string]string{"type": "string"},
			"sender_id": map[string]string{"type": "string"}, "timestamp_unix": map[string]string{"type": "integer"}, "kind": map[string]string{"type": "string"},
		}, "required": []string{"chat_id", "message_id", "sender_id", "timestamp_unix", "kind"}, "additionalProperties": false},
	}}}
}

func (h *eventHub) call(ctx context.Context, method string, in *eventParams) eventReply {
	var value any
	var rpcErr *jsonrpc.Error
	switch method {
	case "list":
		if in.Cursor != nil && *in.Cursor != "" {
			rpcErr = eventFailure(-32602, "unknown catalog cursor", "")
		} else {
			value = eventCatalog()
		}
	case "subscribe", "unsubscribe":
		value, rpcErr = h.change(ctx, method, in)
	default:
		rpcErr = eventFailure(-32601, "unknown event method", "")
	}
	if rpcErr != nil {
		return eventReply{Error: rpcErr}
	}
	data, err := json.Marshal(value)
	if err != nil {
		return eventReply{Error: eventFailure(-32603, "event response could not be encoded", "")}
	}
	return eventReply{Result: data}
}

func (h *eventHub) change(ctx context.Context, method string, in *eventParams) (any, *jsonrpc.Error) {
	var args eventArguments
	if in.Name != messageEventName || in.Delivery.Mode != "webhook" || strictJSON(in.Arguments, &args) != nil || args.ChatID == "" || in.Cursor != nil {
		return nil, eventFailure(-32602, "use message.created, one exact chat_id, webhook delivery, and a null cursor", "")
	}
	if _, err := validRecipient(args.ChatID); err != nil {
		return nil, eventFailure(-32602, "invalid chat_id", "")
	}
	if err := validateCallback(in.Delivery.URL); err != nil {
		return nil, eventFailure(-32602, err.Error(), "")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	owner := h.a.meta("account")
	if owner == "" {
		return nil, eventFailure(-32000, "pair WhatsApp before managing subscriptions", "")
	}
	id := stableID("sub_", owner, in.Delivery.URL, in.Name, args.ChatID)
	if method == "unsubscribe" {
		_, err := h.a.db.ExecContext(ctx, "DELETE FROM event_subscriptions WHERE id=? AND owner=?", id, owner)
		if err != nil {
			return nil, eventFailure(-32603, "subscription could not be removed", "")
		}
		return map[string]any{}, nil
	}
	if !h.active() {
		return nil, eventFailure(-32000, "WhatsApp must be connected to subscribe", "")
	}
	if _, err := webhookKey(in.Delivery.Secret); err != nil {
		return nil, eventFailure(-32602, err.Error(), "")
	}
	ttl := int64((24 * time.Hour) / time.Millisecond)
	if len(in.TTL) > 0 && string(in.TTL) != "null" {
		var requested int64
		if json.Unmarshal(in.TTL, &requested) != nil || requested <= 0 {
			return nil, eventFailure(-32602, "ttlMs must be a positive integer or null", "")
		}
		if requested < ttl {
			ttl = requested
		}
	}
	chat := h.a.canonical(args.ChatID)
	var known int
	if err := h.a.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM (SELECT jid FROM chats WHERE jid=? UNION SELECT jid FROM contacts WHERE jid=?)", chat, chat).Scan(&known); err != nil {
		return nil, eventFailure(-32603, "chat lookup failed", "")
	}
	if known == 0 {
		return nil, eventFailure(-32602, "chat_id is not in the local index; look it up first", "")
	}
	s := eventSubscription{ID: id, Owner: owner, ArgumentChat: args.ChatID, Chat: chat, URL: in.Delivery.URL, Secret: in.Delivery.Secret}
	if err := h.verify(ctx, s); err != nil {
		return nil, err
	}
	if !h.active() || h.a.meta("account") != owner {
		return nil, eventFailure(-32000, "WhatsApp access changed during subscription", "")
	}
	now := time.Now().UnixMilli()
	s.Expires = now + ttl
	h.a.writeMu.Lock()
	defer h.a.writeMu.Unlock()
	chat = h.a.canonical(args.ChatID)
	// Expired subscriptions are new subscriptions, with no backlog replay.
	if _, err := h.a.db.ExecContext(ctx, "DELETE FROM event_subscriptions WHERE id=? AND expires<=?", id, now); err != nil {
		return nil, eventFailure(-32603, "subscription could not be refreshed", "")
	}
	_, err := h.a.db.ExecContext(ctx, `INSERT INTO event_subscriptions(id,owner,argument_chat,chat,url,secret,expires) VALUES(?,?,?,?,?,?,?)
 ON CONFLICT(id) DO UPDATE SET chat=excluded.chat,
 old_secret=CASE WHEN event_subscriptions.secret!=excluded.secret THEN event_subscriptions.secret ELSE event_subscriptions.old_secret END,
 old_until=CASE WHEN event_subscriptions.secret!=excluded.secret THEN ? ELSE event_subscriptions.old_until END,
 secret=excluded.secret,expires=excluded.expires`, id, owner, args.ChatID, chat, s.URL, s.Secret, s.Expires, now+int64((5*time.Minute)/time.Millisecond))
	if err != nil {
		return nil, eventFailure(-32603, "subscription could not be saved", "")
	}
	return subscribeResult{ID: id, RefreshBefore: time.UnixMilli(s.Expires).UTC().Format(time.RFC3339Nano)}, nil
}

func (h *eventHub) verify(ctx context.Context, s eventSubscription) *jsonrpc.Error {
	cacheKey := stableID("", s.Owner, s.URL, s.Secret)
	now := time.Now()
	for key, expiry := range h.verified {
		if !expiry.After(now) {
			delete(h.verified, key)
		}
	}
	if expiry, ok := h.verified[cacheKey]; ok && expiry.After(now) {
		return nil
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return eventFailure(-32603, "verification could not be prepared", "")
	}
	challenge := hex.EncodeToString(nonce[:])
	body, _ := json.Marshal(map[string]string{"type": "verification", "challenge": challenge})
	code, data, err := h.post(ctx, s, "verify_"+challenge, body)
	if err != nil {
		return eventFailure(-32015, "callback verification request failed", "challenge_failed")
	}
	var reply struct {
		Challenge string `json:"challenge"`
	}
	if code < 200 || code >= 300 || json.Unmarshal(data, &reply) != nil || subtle.ConstantTimeCompare([]byte(challenge), []byte(reply.Challenge)) != 1 {
		return eventFailure(-32015, "callback did not echo the verification challenge", "challenge_failed")
	}
	h.verified[cacheKey] = now.Add(5 * time.Minute)
	return nil
}

func (h *eventHub) revoke() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	clear(h.verified)
	_, err := h.a.db.Exec("DELETE FROM event_subscriptions")
	return err
}

func (h *eventHub) run(ctx context.Context, onError func(string)) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer h.http.CloseIdleConnections()
	for {
		for i := 0; i < 100; i++ {
			more, err := h.deliverOne(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				onError("Event delivery storage failed")
				break
			}
			if !more || ctx.Err() != nil {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-h.wakeup:
		}
	}
}

func (h *eventHub) deliverOne(ctx context.Context) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now().UnixMilli()
	if _, err := h.a.db.ExecContext(ctx, "DELETE FROM event_subscriptions WHERE expires<=?", now); err != nil {
		return false, err
	}
	if _, err := h.a.db.ExecContext(ctx, "UPDATE event_subscriptions SET old_secret='',old_until=0 WHERE old_secret!='' AND old_until<=?", now); err != nil {
		return false, err
	}
	if !h.active() {
		return false, nil
	}
	var s eventSubscription
	var id, chat, messageID string
	var body []byte
	var attempts int
	err := h.a.db.QueryRowContext(ctx, `SELECT s.id,s.owner,s.url,s.secret,s.old_secret,s.old_until,s.expires,
 d.event_id,d.chat,d.message_id,d.payload,d.attempts FROM event_deliveries d
 JOIN event_subscriptions s ON s.id=d.subscription
 WHERE d.state='pending' AND d.next_attempt<=? AND s.owner=? ORDER BY d.rowid LIMIT 1`, now, h.a.meta("account")).Scan(
		&s.ID, &s.Owner, &s.URL, &s.Secret, &s.OldSecret, &s.OldUntil, &s.Expires, &id, &chat, &messageID, &body, &attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	m, err := h.a.getMessage(ctx, chat, messageID)
	if err != nil {
		return false, err
	}
	if m.Deleted || m.FromMe {
		_, err = h.a.db.ExecContext(ctx, "DELETE FROM event_deliveries WHERE subscription=? AND event_id=?", s.ID, id)
		return true, err
	}
	if attempts >= 8 {
		_, err = h.a.db.ExecContext(ctx, "UPDATE event_deliveries SET state='failed' WHERE subscription=? AND event_id=?", s.ID, id)
		return true, err
	}
	// Reserve the retry before making the request. A crash after receipt can
	// cause a duplicate, always with the same event ID and identical body.
	backoff := int64((5*time.Second)/time.Millisecond) << attempts
	_, err = h.a.db.ExecContext(ctx, "UPDATE event_deliveries SET attempts=attempts+1,next_attempt=? WHERE subscription=? AND event_id=?", now+backoff, s.ID, id)
	if err != nil {
		return false, err
	}
	if !h.active() || s.Expires <= time.Now().UnixMilli() {
		return false, nil
	}
	code, _, _ := h.post(ctx, s, id, body)
	switch {
	case code == http.StatusGone:
		_, err = h.a.db.ExecContext(ctx, "DELETE FROM event_subscriptions WHERE id=?", s.ID)
	case code >= 200 && code < 300:
		_, err = h.a.db.ExecContext(ctx, "DELETE FROM event_deliveries WHERE subscription=? AND event_id=?", s.ID, id)
	case code == 413 || (code >= 300 && code < 500 && code != 408 && code != 429) || attempts+1 >= 8:
		_, err = h.a.db.ExecContext(ctx, "UPDATE event_deliveries SET state='failed' WHERE subscription=? AND event_id=?", s.ID, id)
	}
	return true, err
}
