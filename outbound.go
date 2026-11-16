package main

import (
	"context"
	_ "embed"
	"errors"
	"strings"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// All fully-qualified, minimally-qualified and unqualified sequences from
// Unicode Emoji 18.0 emoji-test.txt, excluding standalone components.
// Each space-separated entry is one reaction, including compound/ZWJ emoji.
//
//go:embed emoji_sequences.txt
var emojiSequences string

var reactionEmoji = func() map[string]bool {
	sequences := make(map[string]bool)
	for emoji := range strings.FieldsSeq(emojiSequences) {
		sequences[emoji] = true
	}
	return sequences
}()

// The direct tool and durable reply drafts accept the same three alternatives.
type outboundContent struct {
	Text     string         `json:"text,omitempty" jsonschema:"Message text (1 to 16000 bytes). Supply exactly one of text, reaction or media."`
	Reaction string         `json:"reaction,omitempty" jsonschema:"One emoji to apply to the target message. For a direct send, reply_to_message_id is required. A monitored reaction targets the incoming message."`
	Media    *outboundMedia `json:"media,omitempty" jsonschema:"One attachment from the service's outgoing directory."`
}

func (c outboundContent) validate() error {
	switch {
	case c.Media != nil:
		if c.Text != "" || c.Reaction != "" {
			return errors.New("supply exactly one of text, reaction or media; put an attachment caption in media.caption")
		}
		return c.Media.validate()
	case c.Reaction != "":
		if c.Text != "" || !reactionEmoji[c.Reaction] {
			return errors.New("supply one emoji in reaction, without a text message")
		}
	case strings.TrimSpace(c.Text) == "" || len(c.Text) > 16000:
		return errors.New("supply text (1..16000 bytes), one reaction, or one media attachment")
	}
	return nil
}

// Resolve the target from the local archive. A caller cannot override the sender,
// FromMe bit or group participant that WhatsApp uses in the reaction key.
func (a *archive) target(ctx context.Context, in sendInput) (*message, error) {
	if in.ReplyTo == "" {
		if in.Reaction != "" {
			return nil, errors.New("a reaction requires reply_to_message_id")
		}
		return nil, nil
	}
	target, err := a.getMessage(ctx, in.Recipient, in.ReplyTo)
	if err != nil {
		return nil, err
	}
	if target.Deleted {
		return nil, errors.New("target message is deleted, expired or unavailable")
	}
	return &target, nil
}

func (b *bridge) outboundMessage(c outboundContent, target *message, duration uint32, media *uploadedMedia) (*waE2E.Message, error) {
	if c.Reaction != "" {
		wireChat, err := validRecipient(target.wireChat)
		if err != nil {
			return nil, errors.New("invalid original chat address for reaction")
		}
		var sender types.JID
		if !target.FromMe {
			sender, err = validRecipient(target.wireSender)
			if err != nil || sender.Server == types.GroupServer {
				return nil, errors.New("target message has no valid sender for a reaction")
			}
		}
		return b.client.BuildReaction(wireChat, sender, target.ID, c.Reaction), nil
	}

	var ci *waE2E.ContextInfo
	if target != nil {
		ci = &waE2E.ContextInfo{
			StanzaID: proto.String(target.ID), Participant: proto.String(target.wireSender),
			QuotedMessage: &waE2E.Message{Conversation: proto.String(target.Text)},
		}
	}
	if duration > 0 {
		if ci == nil {
			ci = &waE2E.ContextInfo{}
		}
		ci.Expiration = proto.Uint32(duration)
	}
	if media != nil {
		return media.message(ci), nil
	}
	if ci != nil {
		return &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String(c.Text), ContextInfo: ci}}, nil
	}
	return &waE2E.Message{Conversation: proto.String(c.Text)}, nil
}
