package main

import (
	"errors"
	"time"

	"go.mau.fi/whatsmeow/proto/waSyncAction"
)

// Keep a cutoff as well as individual tombstones: a deletion may arrive before
// the corresponding history. Rows cannot simply be removed and later reimported.
func (a *archive) removeChat(chatID string, when time.Time, r *waSyncAction.SyncActionMessageRange, deleteChat bool) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	chatID = a.canonical(chatID)
	cutoff := max(r.GetLastMessageTimestamp(), r.GetLastSystemMessageTimestamp())
	for _, m := range r.GetMessages() {
		cutoff = max(cutoff, m.GetTimestamp())
	}
	// An explicit range takes precedence over event time, which can be much
	// later than the last message observed on the device performing the action.
	if cutoff <= 0 && !when.IsZero() {
		cutoff = when.Unix()
	}
	if cutoff <= 0 {
		return errors.New("chat removal has no usable time boundary")
	}
	deletedThrough := int64(0)
	if deleteChat {
		deletedThrough = cutoff
	}
	tx, err := a.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`INSERT INTO chat_removals VALUES(?,?,?) ON CONFLICT(chat) DO UPDATE SET
 cleared_through=MAX(chat_removals.cleared_through,excluded.cleared_through),
 deleted_through=MAX(chat_removals.deleted_through,excluded.deleted_through)`, chatID, cutoff, deletedThrough)
	if err != nil {
		return err
	}
	_, err = tx.Exec("UPDATE messages SET body='',kind='deleted',deleted=1 WHERE chat=? AND ts<=?", chatID, cutoff)
	if err != nil {
		return err
	}
	for _, entry := range r.GetMessages() {
		key := entry.GetKey()
		if key.GetID() == "" {
			continue
		}
		stamp := entry.GetTimestamp()
		if stamp <= 0 {
			stamp = cutoff
		}
		_, err = tx.Exec(`INSERT INTO messages(chat,id,sender,from_me,ts,body,kind,deleted)
 VALUES(?,?,?,?,?,'','deleted',1) ON CONFLICT(chat,id) DO UPDATE SET body='',kind='deleted',deleted=1`,
			chatID, key.GetID(), key.GetParticipant(), key.GetFromMe(), stamp)
		if err != nil {
			return err
		}
	}
	if err = cleanJobs(tx, time.Now().Unix()); err != nil {
		return err
	}
	return tx.Commit()
}
