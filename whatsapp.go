package main

import (
	"context"
	"io"
	"log"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type pairing struct {
	State string `json:"state"`
	Code  string `json:"code,omitempty"`
}

type bridge struct {
	a               *archive
	client          *whatsmeow.Client
	container       *sqlstore.Container
	ctx             context.Context
	mu              sync.Mutex
	pair            pairing
	pairActive      bool
	accountMismatch bool
	eventsRevoked   bool
	lastError       string
	sendMu          sync.Mutex
	monitorMu       sync.Mutex // Serialize stopping a watch with dispatching its reply.
	syncMu          sync.Mutex
	sendFn          func(context.Context, types.JID, *waE2E.Message, whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error)
	uploadFn        func(context.Context, io.Reader, io.ReadWriteSeeker, whatsmeow.MediaType) (whatsmeow.UploadResponse, error)
	connectedFn     func() bool
	eventHub        *eventHub
}

func newBridge(ctx context.Context, dir string, a *archive) (*bridge, error) {
	c, err := sqlstore.New(ctx, "sqlite3", sqliteDSN(filepath.Join(dir, "session.db")), waLog.Noop)
	if err != nil {
		return nil, err
	}
	device, err := c.GetFirstDevice(ctx)
	if err != nil {
		c.Close()
		return nil, err
	}
	cli := whatsmeow.NewClient(device, waLog.Noop)
	b := &bridge{a: a, client: cli, container: c, ctx: ctx, pair: pairing{State: "idle"}}
	b.sendFn = func(ctx context.Context, j types.JID, m *waE2E.Message, e whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		return cli.SendMessage(ctx, j, m, e)
	}
	b.uploadFn = cli.UploadReader
	b.connectedFn = func() bool { return cli.IsConnected() && cli.IsLoggedIn() }
	b.eventHub = newEventHub(a, func() bool {
		b.mu.Lock()
		blocked := b.accountMismatch || b.eventsRevoked
		b.mu.Unlock()
		return !blocked && b.connectedFn() && cli.Store.ID != nil && cli.Store.ID.ToNonAD().String() == a.meta("account")
	})
	cli.AddEventHandler(b.event)
	return b, nil
}

func (b *bridge) recordError(reason string) {
	b.mu.Lock()
	b.lastError = reason
	b.mu.Unlock()
	log.Print(reason) // Only application-defined messages; never raw protocol errors/payloads.
}

func (b *bridge) reconnectLoop() {
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		b.mu.Lock()
		active := b.pairActive
		blocked := b.accountMismatch
		b.mu.Unlock()
		if !active && !blocked && b.client.Store.ID != nil && !b.client.IsConnected() {
			if err := b.client.ConnectContext(b.ctx); err != nil {
				b.recordError("WhatsApp connection failed; retrying")
			}
		}
		select {
		case <-b.ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (b *bridge) normalize(j types.JID) string {
	j = j.ToNonAD()
	if j.Server == types.HiddenUserServer {
		pn, err := b.client.Store.LIDs.GetPNForLID(b.ctx, j)
		if err == nil && !pn.IsEmpty() {
			if err = b.a.alias(j.String(), pn.ToNonAD().String()); err != nil {
				b.recordError("Address mapping could not be saved")
			}
			return pn.ToNonAD().String()
		}
	}
	return j.String()
}

func (b *bridge) allowedAccount() bool {
	if b.client.Store.ID == nil {
		return false
	}
	id := b.client.Store.ID.ToNonAD().String()
	old := b.a.meta("account")
	if old != "" && old != id {
		b.mu.Lock()
		b.accountMismatch = true
		b.mu.Unlock()
		b.recordError("Account mismatch: use a separate data directory for another account")
		return false
	}
	if old == "" {
		if err := b.a.setMeta("account", id); err != nil {
			b.recordError("Could not bind local archive to account")
			return false
		}
	}
	return true
}

func (b *bridge) syncContacts() {
	b.syncMu.Lock()
	defer b.syncMu.Unlock()
	if !b.allowedAccount() {
		return
	}
	contacts, err := b.client.Store.Contacts.GetAllContacts(b.ctx)
	if err != nil {
		b.recordError("Contact sync failed")
		return
	}
	// Merge all aliases before writing. Map iteration must not decide which
	// name survives. Prefer saved names over business names, then phone IDs
	// over LIDs for equal-quality fields.
	ids := make([]types.JID, 0, len(contacts))
	for jid := range contacts {
		ids = append(ids, jid)
	}
	sort.Slice(ids, func(i, j int) bool {
		pi, pj := ids[i].Server == types.DefaultUserServer, ids[j].Server == types.DefaultUserServer
		if pi != pj {
			return pi
		}
		return ids[i].String() < ids[j].String()
	})
	type candidate struct {
		contact
		rank int
	}
	merged := make(map[string]candidate)
	for _, jid := range ids {
		c := contacts[jid]
		id := b.normalize(jid)
		entry := merged[id]
		entry.ID = id
		name := c.FullName
		rank := 3
		if name == "" {
			name = c.FirstName
			rank = 2
		}
		if name == "" {
			name = c.BusinessName
			rank = 1
		}
		if name != "" && rank > entry.rank {
			entry.Name = name
			entry.rank = rank
		}
		if entry.PushName == "" {
			entry.PushName = c.PushName
		}
		merged[id] = entry
	}
	for _, entry := range merged {
		if err = b.a.mergeContact(entry.contact); err != nil {
			b.recordError("Contact could not be saved")
			return
		}
	}
}

func (b *bridge) syncGroups() {
	if !b.allowedAccount() {
		return
	}
	ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
	defer cancel()
	groups, err := b.client.GetJoinedGroups(ctx)
	if err != nil {
		b.recordError("Group list sync failed; cached chats remain available")
		return
	}
	for _, g := range groups {
		id := b.normalize(g.JID)
		if err = b.a.saveChat(id, g.Name); err != nil {
			b.recordError("Group could not be saved")
			return
		}
		if err = b.a.saveTimer(id, g.DisappearingTimer, time.Now().Unix()); err != nil {
			b.recordError("Group timer could not be saved")
			return
		}
	}
}

func (b *bridge) event(evt any) {
	switch e := evt.(type) {
	case *events.Connected:
		if !b.allowedAccount() {
			go b.client.Disconnect()
			return
		}
		b.mu.Lock()
		b.lastError = ""
		b.eventsRevoked = false
		b.pair = pairing{State: "success"}
		b.mu.Unlock()
		log.Print("WhatsApp connected")
		if err := b.a.saveContact(contact{ID: b.a.meta("account"), Name: "You (linked account)"}); err != nil {
			b.recordError("Own account could not be indexed")
		}
		go b.syncContacts()
		go b.syncGroups()
	case *events.LoggedOut:
		b.mu.Lock()
		b.eventsRevoked = true
		b.mu.Unlock()
		if err := b.a.revokeMonitors(); err != nil {
			b.recordError("Monitor cleanup failed after logout")
		}
		if b.eventHub != nil {
			if err := b.eventHub.revoke(); err != nil {
				b.recordError("Event subscription cleanup failed")
			}
		}
		b.recordError("WhatsApp session was revoked; restart service and pair again")
	case *events.Disconnected:
		log.Print("WhatsApp disconnected")
	case *events.AppStateSyncComplete:
		go b.syncContacts()
	case *events.GroupInfo:
		if !b.allowedAccount() {
			return
		}
		if e.Name != nil {
			if err := b.a.saveChat(b.normalize(e.JID), e.Name.Name); err != nil {
				b.recordError("Group name update failed")
			}
		}
		if e.Ephemeral != nil {
			if err := b.a.saveTimer(b.normalize(e.JID), e.Ephemeral.DisappearingTimer, e.Timestamp.Unix()); err != nil {
				b.recordError("Group timer update failed")
			}
		}
	case *events.Contact:
		if b.allowedAccount() {
			b.syncMu.Lock()
			name := ""
			if e.Action != nil {
				name = e.Action.GetFullName()
				if name == "" {
					name = e.Action.GetFirstName()
				}
			}
			if err := b.a.saveContact(contact{ID: b.normalize(e.JID), Name: name}); err != nil {
				b.recordError("Contact update failed")
			}
			b.syncMu.Unlock()
		}
	case *events.PushName:
		if b.allowedAccount() {
			b.syncMu.Lock()
			id := b.normalize(e.JID)
			err := b.a.mergeContact(contact{ID: id, PushName: e.NewPushName})
			if err != nil {
				b.recordError("Contact name update failed")
			}
			b.syncMu.Unlock()
		}
	case *events.Message:
		if !b.allowedAccount() {
			return
		}
		b.ingest(e)
	case *events.DeleteForMe:
		if !b.allowedAccount() {
			return
		}
		m := message{ChatID: b.normalize(e.ChatJID), ID: e.MessageID, Sender: b.normalize(e.SenderJID), Timestamp: e.Timestamp.Unix(), Deleted: true, Kind: "deleted", Revision: e.Timestamp.UnixMilli()}
		if err := b.a.save(m); err != nil {
			b.recordError("Message deletion could not be saved")
		}
	case *events.ClearChat:
		if b.allowedAccount() {
			if err := b.a.removeChat(b.normalize(e.JID), e.Timestamp, e.Action.GetMessageRange(), false); err != nil {
				b.recordError("Chat clear could not be saved")
			}
		}
	case *events.DeleteChat:
		if b.allowedAccount() {
			if err := b.a.removeChat(b.normalize(e.JID), e.Timestamp, e.Action.GetMessageRange(), true); err != nil {
				b.recordError("Chat deletion could not be saved")
			}
		}
	case *events.HistorySync:
		if !b.allowedAccount() {
			return
		}
		for _, conv := range e.Data.GetConversations() {
			jid, err := types.ParseJID(conv.GetID())
			if err != nil {
				continue
			}
			if err = b.a.saveChat(b.normalize(jid), conv.GetName()); err != nil {
				b.recordError("Chat history could not be saved")
			}
			if err = b.a.saveTimer(b.normalize(jid), conv.GetEphemeralExpiration(), conv.GetEphemeralSettingTimestamp()); err != nil {
				b.recordError("Chat timer could not be saved")
			}
			for _, h := range conv.GetMessages() {
				m, err := b.client.ParseWebMessage(jid, h.GetMessage())
				if err != nil {
					continue
				}
				// ParseWebMessage can unwrap edits; normalize again from the raw envelope.
				m.UnwrapRaw()
				b.ingestObserved(m, false)
			}
		}
		if err := b.a.setMeta("last_history_sync", time.Now().UTC().Format(time.RFC3339)); err != nil {
			b.recordError("History sync status could not be saved")
		}
		_ = b.a.setMeta("history_progress", strconv.Itoa(int(e.Data.GetProgress())))
		go b.syncContacts()
	}
}

func content(m *waE2E.Message) (string, string, *waE2E.ContextInfo) {
	switch {
	case m == nil:
		return "", "unsupported", nil
	case m.Conversation != nil:
		return m.GetConversation(), "text", nil
	case m.ExtendedTextMessage != nil:
		return m.GetExtendedTextMessage().GetText(), "text", m.GetExtendedTextMessage().GetContextInfo()
	case m.ImageMessage != nil:
		return m.GetImageMessage().GetCaption(), "image", m.GetImageMessage().GetContextInfo()
	case m.VideoMessage != nil:
		return m.GetVideoMessage().GetCaption(), "video", m.GetVideoMessage().GetContextInfo()
	case m.DocumentWithCaptionMessage != nil:
		return content(m.GetDocumentWithCaptionMessage().GetMessage())
	case m.DocumentMessage != nil:
		return m.GetDocumentMessage().GetCaption(), "document", m.GetDocumentMessage().GetContextInfo()
	case m.AudioMessage != nil:
		return "", "audio", m.GetAudioMessage().GetContextInfo()
	case m.ReactionMessage != nil:
		return m.GetReactionMessage().GetText(), "reaction", nil
	case m.StickerMessage != nil:
		return "", "sticker", m.GetStickerMessage().GetContextInfo()
	default:
		return "", "unsupported", nil
	}
}

func decodeMessage(e *events.Message) (message, bool) {
	m := message{ChatID: e.Info.Chat.ToNonAD().String(), ID: e.Info.ID, Sender: e.Info.Sender.ToNonAD().String(), FromMe: e.Info.IsFromMe, Timestamp: e.Info.Timestamp.Unix()}
	if m.ID == "" || e.Message == nil {
		return m, false
	}
	p := e.Message.GetProtocolMessage()
	body := e.Message
	if p != nil {
		switch p.GetType() {
		case waE2E.ProtocolMessage_REVOKE:
			m.ID = p.GetKey().GetID()
			m.Deleted = true
			m.Kind = "deleted"
			m.Revision = e.Info.Timestamp.UnixMilli()
			return m, m.ID != ""
		case waE2E.ProtocolMessage_MESSAGE_EDIT:
			m.ID = p.GetKey().GetID()
			m.Edited = true
			m.Revision = p.GetTimestampMS()
			if m.Revision == 0 {
				m.Revision = e.Info.Timestamp.UnixMilli()
			}
			body = p.GetEditedMessage()
		default:
			return m, false
		}
	}
	if e.IsEdit {
		m.Edited = true
		if m.Revision == 0 {
			m.Revision = e.Info.Timestamp.UnixMilli()
		}
	}
	var ci *waE2E.ContextInfo
	m.Text, m.Kind, ci = content(body)
	if e.IsViewOnce {
		m.Text = ""
		m.Kind = "view_once"
		m.Deleted = true
	}
	duration := ci.GetExpiration()
	if duration == 0 {
		duration = e.SourceWebMsg.GetEphemeralDuration()
	}
	if duration > 0 {
		start := m.Timestamp
		if v := e.SourceWebMsg.GetEphemeralStartTimestamp(); v > 0 {
			start = int64(v)
		}
		m.Expires = start + int64(duration)
	} else if e.IsEphemeral {
		m.Text = ""
		m.Kind = "ephemeral_unavailable"
		m.Deleted = true
	}
	if expiry := e.SourceWebMsg.GetEphemeralExpirationTimestamp(); expiry > 0 {
		m.Expires = int64(expiry)
	}
	return m, m.ID != ""
}

func (b *bridge) ingest(e *events.Message) {
	b.ingestObserved(e, true)
}

func (b *bridge) ingestObserved(e *events.Message, live bool) {
	if p := e.Message.GetProtocolMessage(); p != nil && p.GetType() == waE2E.ProtocolMessage_EPHEMERAL_SETTING {
		if err := b.a.saveTimer(b.normalize(e.Info.Chat), p.GetEphemeralExpiration(), p.GetEphemeralSettingTimestamp()); err != nil {
			b.recordError("Chat timer update failed")
		}
		return
	}
	m, ok := decodeMessage(e)
	if !ok {
		return
	}
	// The archive indexes canonical addresses, but reaction keys and quotes must
	// preserve the original WhatsApp addresses, including group participant LIDs.
	if !m.Edited && !m.Deleted {
		m.wireChat, m.wireSender = m.ChatID, m.Sender
	}
	m.ChatID = b.normalize(e.Info.Chat)
	m.Sender = b.normalize(e.Info.Sender)
	if err := b.a.saveObserved(m, live); err != nil {
		b.recordError("Message could not be saved")
		return
	}
	_ = b.a.setMeta("last_message_event", time.Now().UTC().Format(time.RFC3339))
	if b.eventHub != nil {
		b.eventHub.wake()
	}
}
