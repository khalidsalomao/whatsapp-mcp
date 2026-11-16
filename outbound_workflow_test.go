package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

type testSubscription struct {
	mcp.ResultBase
	subscribeResult
}

type testDispatch struct {
	chat types.JID
	id   string
	msg  *waE2E.Message
}

type testUpload struct {
	kind whatsmeow.MediaType
	data []byte
}

func syntheticUpload(data []byte) whatsmeow.UploadResponse {
	plainHash := sha256.Sum256(data)
	encryptedHash := sha256.Sum256(append([]byte("test ciphertext "), data...))
	return whatsmeow.UploadResponse{
		URL: "https://cdn.example/file", DirectPath: "/file", MediaKey: bytes.Repeat([]byte{42}, 32),
		FileSHA256: plainHash[:], FileEncSHA256: encryptedHash[:], FileLength: uint64(len(data)), StreamingSidecar: []byte("sidecar"),
	}
}

func mustCallMCP(t *testing.T, ctx context.Context, session *mcp.ClientSession, name string, in, out any) {
	t.Helper()
	resp, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: in})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if resp.IsError || len(resp.Content) == 0 {
		t.Fatalf("%s: %v", name, resp.Content)
	}
	body, ok := resp.Content[0].(*mcp.TextContent)
	if !ok || json.Unmarshal([]byte(body.Text), out) != nil {
		t.Fatalf("%s: invalid result %v", name, resp.Content)
	}
}

// Exercise the stdio command, socket service, authenticated HTTP endpoint and
// signed callbacks. Only WhatsApp upload/send and the host's HTTPS receiver are fakes.
func TestOutboundMCPWorkflows(t *testing.T) {
	const worker = "WHATSAPP_MCP_TEST_STDIO_DIR"
	if dir := os.Getenv(worker); dir != "" {
		if err := run([]string{"mcp", "--data-dir", dir}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	for _, mode := range []string{"direct", "poll", "event"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			b, _, sender := contactSyncBridge(t)
			b.connectedFn = func() bool { return true }
			if !b.allowedAccount() {
				t.Fatal("account not bound")
			}
			group := types.NewJID("123-456", types.GroupServer)
			if err := b.a.saveChat(group.String(), "Fixture group"); err != nil {
				t.Fatal(err)
			}
			if err := b.a.saveTimer(group.String(), 3600, time.Now().Unix()); err != nil {
				t.Fatal(err)
			}
			sends := make(chan testDispatch, 10)
			uploads := make(chan testUpload, 10)
			b.sendFn = func(_ context.Context, chat types.JID, msg *waE2E.Message, extra whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
				sends <- testDispatch{chat: chat, id: extra.ID, msg: msg}
				return whatsmeow.SendResponse{Timestamp: time.Now()}, nil
			}
			b.uploadFn = func(_ context.Context, r io.Reader, _ io.ReadWriteSeeker, typ whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
				data, err := io.ReadAll(r)
				uploads <- testUpload{typ, data}
				return syntheticUpload(data), err
			}
			receiver := &fakeEventReceiver{}
			b.eventHub.http = receiver.client(t)
			dir := serveTestSocketDir(t, b)
			var transport mcp.Transport
			if mode == "event" {
				const token = "fixture-bearer"
				local := newLocalClient(dir)
				defer local.http.CloseIdleConnections()
				api := httptest.NewServer(authenticatedMCP(local, token))
				defer api.Close()
				httpClient := &http.Client{Transport: eventRoundTripper(func(r *http.Request) (*http.Response, error) {
					req := r.Clone(r.Context())
					req.Header.Set("Authorization", "Bearer "+token)
					return api.Client().Transport.RoundTrip(req)
				})}
				transport = &mcp.StreamableClientTransport{Endpoint: api.URL + "/mcp", HTTPClient: httpClient, DisableStandaloneSSE: true}
			} else {
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOutboundMCPWorkflows$")
				cmd.Env = append(os.Environ(), worker+"="+dir)
				cmd.Stderr = t.Output()
				transport = &mcp.CommandTransport{Command: cmd}
			}
			client := mcp.NewClient(&mcp.Implementation{Name: "outbound-test", Version: "1"}, nil)
			if err := mcp.AddSendingCustomMethod[*eventParams, *testSubscription](client, "events/subscribe"); err != nil {
				t.Fatal(err)
			}
			session, err := client.Connect(ctx, transport, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			watch := monitor{}
			if mode != "direct" {
				mustCallMCP(t, ctx, session, "start_monitor", startMonitorInput{group.String(), "agent:test", "Send the requested fixture as a reply or react."}, &watch)
			}
			subParams := eventInput(group.String())
			if mode == "event" {
				sub, err := mcp.CallCustomMethod[*eventParams, *testSubscription](ctx, session, "events/subscribe", subParams)
				if err != nil || sub.ID == "" {
					t.Fatalf("events/subscribe: %v, %v", sub, err)
				}
			}
			for _, tc := range []struct{ kind, name, mime string }{
				{"reaction", "", ""}, {"image", "image.png", "image/png"}, {"audio", "audio.mp3", "audio/mpeg"},
				{"video", "video.mp4", "video/mp4"}, {"document", "report.txt", "text/plain; charset=utf-8"},
			} {
				t.Run(tc.kind, func(t *testing.T) {
					source := eventFixture()
					source.Info.ID, source.Info.Chat, source.Info.Sender = "source-"+tc.kind, group, sender
					source.Info.IsGroup, source.Info.Timestamp = true, time.Now()
					b.event(source)
					c := outboundContent{Reaction: "👩🏽‍💻"}
					var data []byte
					caption := ""
					if tc.kind != "reaction" {
						data = []byte("Synthetic document for an offline WhatsApp test.\n")
						if tc.kind != "document" {
							var err error
							data, err = os.ReadFile(filepath.Join("testdata", "outbound", tc.name))
							if err != nil {
								t.Fatal(err)
							}
						}
						if err := os.WriteFile(filepath.Join(b.a.mediaDir, tc.name), data, 0600); err != nil {
							t.Fatal(err)
						}
						if tc.kind != "audio" {
							caption = "Legenda de teste"
						}
						c = outboundContent{Media: &outboundMedia{Kind: tc.kind, FileName: tc.name, Caption: caption}}
					}
					var messageID string
					if mode == "direct" {
						in := sendInput{Recipient: group.String(), outboundContent: c, RequestID: "request-" + tc.kind}
						if c.Reaction != "" {
							in.ReplyTo = source.Info.ID
						}
						var out sendResult
						for range 2 {
							mustCallMCP(t, ctx, session, "send_message", in, &out)
							if out.State != "acknowledged" || out.Recipient != group.String() {
								t.Fatalf("direct send: %+v", out)
							}
						}
						messageID = out.MessageID
					} else {
						wait := 1
						if mode == "event" {
							wait = 0
							if !deliverForTest(t, b.eventHub) || len(receiver.events) == 0 {
								t.Fatal("host received no callback")
							}
							callback := receiver.events[len(receiver.events)-1]
							expectSignature(t, callback, subParams.Delivery.Secret)
							var event messageEvent
							if json.Unmarshal(callback.Body, &event) != nil || event.Data.MessageID != source.Info.ID {
								t.Fatal("wrong message in callback")
							}
						}
						var jobs []replyJob
						mustCallMCP(t, ctx, session, "claim_reply_jobs", claimJobsInput{ConsumerID: watch.ConsumerID, WaitSeconds: wait}, &jobs)
						if len(jobs) != 1 || jobs[0].MessageID != source.Info.ID {
							t.Fatalf("claim: %+v", jobs)
						}
						j := jobs[0]
						var draft, out replyJob
						mustCallMCP(t, ctx, session, "draft_reply", draftReplyInput{j.ID, j.ClaimToken, c}, &draft)
						if draft.State != "drafted" || draft.Reaction != c.Reaction || (c.Media != nil && (draft.Media == nil || draft.Media.SHA256 == "")) {
							t.Fatalf("draft: %+v", draft)
						}
						if len(uploads) != 0 || len(sends) != 0 {
							t.Fatal("drafting contacted WhatsApp")
						}
						for range 2 {
							mustCallMCP(t, ctx, session, "send_reply", jobClaimInput{j.ID, j.ClaimToken}, &out)
							if out.State != "sent" || out.RequestID != j.RequestID {
								t.Fatalf("send reply: %+v", out)
							}
						}
						messageID = out.SentMessageID
					}
					if len(sends) != 1 {
						t.Fatalf("got %d dispatches after retry", len(sends))
					}
					sent := <-sends
					if sent.id == "" || sent.id != messageID || sent.chat != group {
						t.Fatalf("wrong dispatch: %+v", sent)
					}
					var cached message
					mustCallMCP(t, ctx, session, "get_message", getMessageInput{group.String(), sent.id}, &cached)
					if cached.Kind != tc.kind || !cached.FromMe || cached.Expires == 0 {
						t.Fatalf("wrong cached message: %+v", cached)
					}
					if tc.kind == "reaction" {
						r := sent.msg.GetReactionMessage()
						k := r.GetKey()
						if r.GetText() != c.Reaction || r.GetSenderTimestampMS() == 0 || k.GetID() != source.Info.ID || k.GetFromMe() ||
							k.GetRemoteJID() != group.String() || k.GetParticipant() != sender.String() || cached.Text != c.Reaction || len(uploads) != 0 {
							t.Fatalf("wrong reaction: %+v", r)
						}
						return
					}
					if len(uploads) != 1 {
						t.Fatalf("got %d uploads after retry", len(uploads))
					}
					u := <-uploads
					if u.kind != mediaTypes[tc.kind] || !bytes.Equal(u.data, data) {
						t.Fatal("wrong upload type or bytes")
					}
					body, kind, ci := content(sent.msg)
					quoteID, participant := "", ""
					if mode != "direct" {
						quoteID, participant = source.Info.ID, sender.String()
					}
					if kind != tc.kind || body != caption || cached.Text != caption || ci.GetExpiration() != 3600 ||
						ci.GetStanzaID() != quoteID || ci.GetParticipant() != participant {
						t.Fatalf("wrong attachment, caption or quote: %v", sent.msg)
					}
					var attachment interface {
						GetURL() string
						GetDirectPath() string
						GetFileLength() uint64
						GetFileSHA256() []byte
						GetFileEncSHA256() []byte
						GetMediaKey() []byte
						GetMimetype() string
					}
					switch tc.kind {
					case "image":
						attachment = sent.msg.GetImageMessage()
					case "audio":
						attachment = sent.msg.GetAudioMessage()
					case "video":
						v := sent.msg.GetVideoMessage()
						if string(v.GetStreamingSidecar()) != "sidecar" {
							t.Fatal("missing video streaming metadata")
						}
						attachment = v
					case "document":
						d := sent.msg.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage()
						if d.GetFileName() != tc.name {
							t.Fatal("lost document filename")
						}
						attachment = d
					}
					expected := syntheticUpload(data)
					if attachment.GetMimetype() != tc.mime || attachment.GetURL() != "https://cdn.example/file" || attachment.GetDirectPath() != "/file" ||
						attachment.GetFileLength() != uint64(len(data)) || !bytes.Equal(attachment.GetFileSHA256(), expected.FileSHA256) ||
						!bytes.Equal(attachment.GetFileEncSHA256(), expected.FileEncSHA256) || !bytes.Equal(attachment.GetMediaKey(), expected.MediaKey) {
						t.Fatalf("wrong WhatsApp upload fields: %+v", attachment)
					}
				})
			}
		})
	}
}
