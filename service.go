package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

type status struct {
	Connected          bool   `json:"connected"`
	Paired             bool   `json:"paired"`
	Account            string `json:"account,omitempty"`
	AccountMismatch    bool   `json:"account_mismatch"`
	MessageCount       int    `json:"message_count"`
	OldestMessage      int64  `json:"oldest_message_unix"`
	NewestMessage      int64  `json:"newest_message_unix"`
	LastHistorySync    string `json:"last_history_sync,omitempty"`
	LastMessageEvent   string `json:"last_message_event,omitempty"`
	LastError          string `json:"last_error,omitempty"`
	HistoryCoverage    string `json:"history_coverage"`
	EventSubscriptions int    `json:"event_subscriptions"`
	EventsPending      int    `json:"events_pending"`
	EventsFailed       int    `json:"events_failed"`
	ActiveMonitors     int    `json:"active_monitors"`
	ReplyJobsPending   int    `json:"reply_jobs_pending"`
	ReplyJobsUncertain int    `json:"reply_jobs_uncertain"`
}

type sendInput struct {
	Recipient string `json:"recipient_id" jsonschema:"Exact recipient_id or chat_id returned by search_contacts or list_chats. Names are rejected."`
	outboundContent
	RequestID string `json:"request_id" jsonschema:"Unique UUID for this user-authorized send; reuse exactly the same ID and arguments on retries. Never generate a new ID to retry an uncertain send."`
	ReplyTo   string `json:"reply_to_message_id,omitempty" jsonschema:"A message ID from this same chat. Required for reactions; optional to quote a message with text or media."`
}
type sendResult struct {
	RequestID string `json:"request_id"`
	MessageID string `json:"message_id"`
	Recipient string `json:"recipient_id"`
	State     string `json:"state"`
	Note      string `json:"note"`
}

var requestPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{8,128}$`)
var userPattern = regexp.MustCompile(`^[0-9]+(?:-[0-9]+)?$`)

func validRecipient(s string) (types.JID, error) {
	j, err := types.ParseJID(s)
	if err != nil || j.Device != 0 || j.RawAgent != 0 || j.Integrator != 0 || j.String() != s || !userPattern.MatchString(j.User) || (j.Server != types.DefaultUserServer && j.Server != types.HiddenUserServer && j.Server != types.GroupServer) {
		return j, errors.New("use an exact contact or group ID from a lookup; recipient names and broadcasts are not accepted")
	}
	return j, nil
}

func (b *bridge) send(ctx context.Context, in sendInput) (sendResult, error) {
	return b.sendWithGuard(ctx, in, nil)
}

func (b *bridge) sendWithGuard(ctx context.Context, in sendInput, job *replyJob) (sendResult, error) {
	var out sendResult
	if !requestPattern.MatchString(in.RequestID) {
		return out, errors.New("request_id must contain 8..128 letters, digits, underscores or hyphens")
	}
	if err := in.validate(); err != nil {
		return out, err
	}
	if _, err := validRecipient(in.Recipient); err != nil {
		return out, err
	}
	// Fingerprint the original input so alias changes don't invalidate an existing request.
	payload, _ := json.Marshal(in)
	sum := sha256.Sum256(payload)
	fingerprint := hex.EncodeToString(sum[:])
	b.sendMu.Lock()
	defer b.sendMu.Unlock()
	if job == nil && strings.HasPrefix(in.RequestID, "reply_") {
		var belongsToJob bool
		err := b.a.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM reply_jobs WHERE request_id=?)", in.RequestID).Scan(&belongsToJob)
		if err != nil {
			return out, err
		}
		if belongsToJob {
			return out, errors.New("this request_id belongs to a monitored job; use send_reply with its claim")
		}
	}
	var savedFingerprint string
	err := b.a.db.QueryRowContext(ctx, "SELECT fingerprint,chat,message_id,state FROM outbox WHERE request_id=?", in.RequestID).Scan(&savedFingerprint, &out.Recipient, &out.MessageID, &out.State)
	out.RequestID = in.RequestID
	if err == nil {
		if savedFingerprint != fingerprint {
			return sendResult{}, errors.New("request_id was already used with different arguments")
		}
		out.Note = "Existing request; no second send attempted. Acknowledged means accepted by WhatsApp, not read by the recipient. For uncertain status, inspect the chat before deciding what to do."
		return out, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return out, errors.New("could not read outbound request ledger")
	}
	b.mu.Lock()
	blocked := b.accountMismatch || b.eventsRevoked
	b.mu.Unlock()
	if blocked {
		return out, errors.New("account changed or logged out; sending is disabled")
	}
	if !b.connectedFn() {
		return out, errors.New("WhatsApp is disconnected; no send attempted")
	}
	in.Recipient = b.a.canonical(in.Recipient)
	j, err := validRecipient(in.Recipient)
	if err != nil {
		return out, err
	}
	var known int
	if err = b.a.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM (SELECT jid FROM contacts WHERE jid=? UNION SELECT jid FROM chats WHERE jid=?)", in.Recipient, in.Recipient).Scan(&known); err != nil || known == 0 {
		return out, errors.New("recipient is not in the local contact/chat index; look it up first")
	}
	target, err := b.a.target(ctx, in)
	if err != nil {
		return out, err
	}
	var media *uploadedMedia
	if in.Media != nil {
		media, err = b.uploadMedia(ctx, in, job)
		if err != nil {
			return out, err
		}
		// Uploads may take time. Re-read the account and quote before dispatch.
		if err = b.monitorAccess(); err != nil || !b.connectedFn() {
			return out, errors.New("account changed or disconnected during upload; no message sent")
		}
		target, err = b.a.target(ctx, in)
		if err != nil {
			return out, err
		}
	}
	duration := b.a.timer(in.Recipient)
	msg, err := b.outboundMessage(in.outboundContent, target, duration, media)
	if err != nil {
		return out, err
	}
	id := whatsmeow.GenerateMessageID()
	if job != nil {
		err = b.a.guardReply(ctx, *job, in, fingerprint, id)
	} else {
		_, err = b.a.db.ExecContext(ctx, "INSERT INTO outbox VALUES(?,?,?,?,?,?)", in.RequestID, fingerprint, in.Recipient, id, "sending", time.Now().Unix())
	}
	if err != nil {
		var stale *replyPreconditionError
		if errors.As(err, &stale) {
			return out, err
		}
		return out, errors.New("could not persist outbound request; no send attempted")
	}
	out = sendResult{RequestID: in.RequestID, MessageID: id, Recipient: in.Recipient, State: "uncertain"}
	// A persisted reservation survives caller cancellation and service crashes.
	response, sendErr := b.sendFn(ctx, j, msg, whatsmeow.SendRequestExtra{ID: id, Timeout: 30 * time.Second})
	if sendErr == nil {
		out.State = "acknowledged"
		out.Note = "Accepted by WhatsApp; delivery/read status is not implied."
	} else {
		out.Note = "Send outcome is uncertain. Do not resend with a new request_id. Inspect the chat first."
	}
	if _, err = b.a.db.Exec("UPDATE outbox SET state=? WHERE request_id=?", out.State, in.RequestID); err != nil {
		out.State = "uncertain"
		out.Note = "Could not persist acknowledgement; do not send again automatically."
		b.recordError("Outbound acknowledgement could not be persisted")
	}
	if sendErr == nil {
		stamp := response.Timestamp.Unix()
		if stamp <= 0 {
			stamp = time.Now().Unix()
		}
		expires := int64(0)
		if duration > 0 {
			expires = stamp + int64(duration)
		}
		if in.Reaction != "" && target.Expires > 0 && (expires == 0 || target.Expires < expires) {
			expires = target.Expires
		}
		body, kind, _ := content(msg)
		if err = b.a.save(message{ChatID: in.Recipient, ID: id, Sender: b.a.meta("account"), FromMe: true, Timestamp: stamp, Text: body, Kind: kind, Expires: expires}); err != nil {
			b.recordError("Sent message could not be indexed; outbound request remains recorded")
		}
	}
	return out, nil
}

func (b *bridge) status() (status, error) {
	b.mu.Lock()
	s := status{LastError: b.lastError, AccountMismatch: b.accountMismatch}
	b.mu.Unlock()
	s.Connected = b.connectedFn()
	s.Paired = b.client != nil && b.client.Store.ID != nil
	s.Account = b.a.meta("account")
	s.LastHistorySync = b.a.meta("last_history_sync")
	s.LastMessageEvent = b.a.meta("last_message_event")
	s.HistoryCoverage = "Partial local cache: only history supplied to this linked device and messages observed while connected. Oldest/newest timestamps do not prove continuous coverage."
	err := b.a.db.QueryRow("SELECT COUNT(*),COALESCE(MIN(ts),0),COALESCE(MAX(ts),0) FROM messages").Scan(&s.MessageCount, &s.OldestMessage, &s.NewestMessage)
	if err == nil {
		err = b.a.db.QueryRow(`SELECT
 (SELECT COUNT(*) FROM event_subscriptions WHERE expires>?),
 (SELECT COUNT(*) FROM event_deliveries WHERE state='pending'),
 (SELECT COUNT(*) FROM event_deliveries WHERE state='failed')`, time.Now().UnixMilli()).Scan(&s.EventSubscriptions, &s.EventsPending, &s.EventsFailed)
	}
	if err == nil {
		err = b.a.db.QueryRow(`SELECT
 (SELECT COUNT(*) FROM monitors WHERE active=1),
 (SELECT COUNT(*) FROM reply_jobs WHERE state IN ('pending','claimed','drafted','sending')),
 (SELECT COUNT(*) FROM reply_jobs WHERE state='uncertain')`).Scan(&s.ActiveMonitors, &s.ReplyJobsPending, &s.ReplyJobsUncertain)
	}
	return s, err
}

func decodeRequest(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return errors.New("invalid request JSON")
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("expected one JSON object")
	}
	return nil
}

func (b *bridge) handler() http.Handler {
	mux := http.NewServeMux()
	respond := func(w http.ResponseWriter, value any, err error) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(value)
	}
	b.addMonitorRoutes(mux, respond)
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) { v, e := b.status(); respond(w, v, e) })
	mux.HandleFunc("POST /message", func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		blocked := b.accountMismatch
		b.mu.Unlock()
		if blocked {
			respond(w, nil, errors.New("account mismatch"))
			return
		}
		var in getMessageInput
		if err := decodeRequest(w, r, &in); err != nil {
			respond(w, nil, err)
			return
		}
		if in.ChatID == "" || in.MessageID == "" {
			respond(w, nil, errors.New("chat_id and message_id are required"))
			return
		}
		v, err := b.a.getMessage(r.Context(), in.ChatID, in.MessageID)
		respond(w, v, err)
	})
	for _, method := range []string{"list", "subscribe", "unsubscribe"} {
		mux.HandleFunc("POST /events/"+method, func(w http.ResponseWriter, r *http.Request) {
			var in eventParams
			if err := decodeRequest(w, r, &in); err != nil {
				respond(w, nil, err)
				return
			}
			if b.eventHub == nil {
				respond(w, nil, errors.New("event service unavailable"))
				return
			}
			respond(w, b.eventHub.call(r.Context(), method, &in), nil)
		})
	}
	for _, op := range []string{"contacts", "chats", "messages"} {
		mux.HandleFunc("POST /"+op, func(w http.ResponseWriter, r *http.Request) {
			b.mu.Lock()
			blocked := b.accountMismatch
			b.mu.Unlock()
			if blocked {
				respond(w, nil, errors.New("account mismatch"))
				return
			}
			var in listInput
			if err := decodeRequest(w, r, &in); err != nil {
				respond(w, nil, err)
				return
			}
			switch op {
			case "contacts":
				v, e := b.a.contacts(r.Context(), in)
				respond(w, v, e)
			case "chats":
				v, e := b.a.chats(r.Context(), in)
				respond(w, v, e)
			case "messages":
				v, e := b.a.messages(r.Context(), in)
				respond(w, v, e)
			}
		})
	}
	mux.HandleFunc("POST /send", func(w http.ResponseWriter, r *http.Request) {
		var in sendInput
		if e := decodeRequest(w, r, &in); e != nil {
			respond(w, nil, e)
			return
		}
		v, e := b.send(r.Context(), in)
		respond(w, v, e)
	})
	mux.HandleFunc("POST /pair", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]string{"state": "starting"}, b.startPair())
	})
	mux.HandleFunc("GET /pair", func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		p := b.pair
		b.mu.Unlock()
		respond(w, p, nil)
	})
	mux.HandleFunc("POST /logout", func(w http.ResponseWriter, r *http.Request) {
		b.sendMu.Lock()
		defer b.sendMu.Unlock()
		if b.client.Store.ID == nil {
			respond(w, nil, errors.New("not paired"))
			return
		}
		if err := b.client.Logout(r.Context()); err != nil {
			respond(w, nil, errors.New("logout failed; revoke this device from your phone"))
			return
		}
		b.mu.Lock()
		b.eventsRevoked = true
		b.mu.Unlock()
		if err := b.a.revokeMonitors(); err != nil {
			respond(w, nil, errors.New("logged out, but monitor cleanup failed"))
			return
		}
		if b.eventHub != nil {
			if err := b.eventHub.revoke(); err != nil {
				respond(w, nil, errors.New("logged out, but subscription cleanup failed"))
				return
			}
		}
		respond(w, map[string]string{"state": "logged_out", "note": "Restart the service before pairing again. Local message cache is retained."}, nil)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Socket is filesystem-private; also reject browser-originated requests.
		if r.Host != "local" || r.Header.Get("Origin") != "" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func privateDir(dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return errors.New("data directory must be a real directory")
	}
	if err = os.Chmod(dir, 0700); err != nil {
		return err
	}
	for _, name := range []string{"messages.db", "session.db", "service.lock", "bridge.sock"} {
		fi, e := os.Lstat(filepath.Join(dir, name))
		if e == nil && fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing symlink: %s", name)
		}
	}
	return nil
}

func runService(ctx context.Context, dir string) error {
	if err := privateDir(dir); err != nil {
		return err
	}
	syscall.Umask(0077)
	lock, err := os.OpenFile(filepath.Join(dir, "service.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errors.New("another service already owns this data directory")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	path := filepath.Join(dir, "bridge.sock")
	if len(path) > 100 {
		return errors.New("data directory path is too long for a Unix socket")
	}
	if fi, e := os.Lstat(path); e == nil {
		if fi.Mode()&os.ModeSocket == 0 {
			return errors.New("bridge.sock exists and is not a socket")
		}
		if err = os.Remove(path); err != nil {
			return err
		}
	}
	a, err := openArchive(dir)
	if err != nil {
		return err
	}
	defer a.db.Close()
	b, err := newBridge(ctx, dir, a)
	if err != nil {
		return err
	}
	defer b.container.Close()
	defer b.client.Disconnect()
	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err = os.Chmod(path, 0600); err != nil {
		return err
	}
	server := &http.Server{Handler: b.handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: mediaRequestTimeout + 5*time.Second, IdleTimeout: 60 * time.Second}
	go b.reconnectLoop()
	eventCtx, eventCancel := context.WithCancel(ctx)
	eventDone := make(chan struct{})
	go func() { defer close(eventDone); b.eventHub.run(eventCtx, b.recordError) }()
	defer func() { eventCancel(); <-eventDone }()
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := a.expire(); err != nil {
					b.recordError("Message expiration failed")
				}
			}
		}
	}()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
