# Local WhatsApp MCP for Codex and other agents

A macOS-local WhatsApp linked-device service and MCP adapter, built in Go with
[whatsmeow](https://github.com/tulir/whatsmeow) and the
[official Go MCP SDK](https://github.com/modelcontextprotocol/go-sdk).
Version 0.4.0 supports reading and searching messages, sending text and media,
reacting with emoji, and replying in monitored conversations through durable
jobs and native MCP Events subscriptions.

## Development and review

This deliverable is the MCP server software. An agent runtime, a bot deployment,
and a recurring task are not required to build or test it. There is no active
Codex/Grok integration from this implementation. Client configuration is supplied
as a portable example for later use.

Build and run the synthetic tests without pairing or installing anything:

```sh
make check
make build
```

One service owns the WhatsApp connection and SQLite databases. Independent MCP
stdio clients share it over a private Unix socket. The `start` command starts the
service and its authenticated loopback MCP endpoint. It can also run a configured
Cloudflare Tunnel for a remote client. A host supplies its own model inference.

## Run locally

After building, run the compiled executable from the project directory:

```sh
./bin/whatsapp-mcp setup
./bin/whatsapp-mcp start
```

`setup` offers WhatsApp pairing and tunnel configuration independently. For
pairing, it opens the QR code on macOS and prints the image path for manual
opening. Scan it from WhatsApp under **Linked Devices → Link a Device**. Run
`setup --pair` to do just this step. An existing saved session is reused.

`start` prints `http://127.0.0.1:8765/mcp` and uses the same private MCP token
across restarts. The service also processes MCP Events subscriptions and
deliveries. Press `Ctrl+C` to stop the components started by this command after
giving active requests time to finish. Already running WhatsApp services have
their own lifecycle.

`./bin/whatsapp-mcp status` shows the WhatsApp connection and individual
checks under `local_mcp` and `tunnel` in its JSON output. When using a custom
`--data-dir`, `--listen` or `--token-file`, pass the same option to `status`.

### Connect Grok Bot at a fixed HTTPS URL

1. Install `cloudflared`. On macOS, run `brew install cloudflared`. In your
   Cloudflare account, create a dedicated named tunnel and a published route on
   a domain managed by Cloudflare. Set the service URL to
   `http://127.0.0.1:8765` and the **HTTP Host Header** to `127.0.0.1:8765`.
   Use the same address as `start --listen` if you customize it. Quick Tunnel
   hostnames change on restart.
   [Cloudflare setup](https://developers.cloudflare.com/tunnel/get-started/)
   · [Origin settings](https://developers.cloudflare.com/tunnel/reference/origin-parameters/#httphostheader)
2. Run `./bin/whatsapp-mcp setup`. Enter the published HTTPS URL and only the
   token value that follows `--token` in the Cloudflare dashboard command.
   Setup adds `/mcp` to a hostname-only URL and stores the settings privately
   in the selected data directory. It can copy the MCP header to the clipboard
   on macOS with your permission. For scripts, use
   `setup --tunnel-url URL --tunnel-token-file PATH` with a private file
   containing the Cloudflare connector token.
3. Run `./bin/whatsapp-mcp start` and wait for **Cloudflare HTTPS endpoint**.
   In **Connect apps → Add a custom connector**, use **Remote HTTPS**, the
   printed URL, and the `Authorization` header with value `Bearer <MCP token>`.
   Use the token from `mcp-http.token` or the value copied by setup. Leave
   **Client ID** empty. Ask the Bot to call `get_status`.
   [Grok Bot connector setup](https://docs.x.ai/grok-bot/computer-and-apps#connect-an-app)

Restart `start` and call `get_status` again from the same connector. The hostname
and MCP token are reused. The hostname is public and the MCP bearer token
authorizes its tools, including WhatsApp sends. Keep both tokens private. If the
published route fails verification, check the Cloudflare settings and run
`start` again. `start --local` runs without the tunnel for that session, and
`setup --remove-tunnel` removes the saved configuration for future starts.

The HTTPS connector lets Grok Bot call tools on demand. For event-driven use,
an agent host needs to subscribe to MCP Events and handle verified callbacks.
A Grok routine uses its own webhook trigger.

## Tools

| Tool | Function |
| --- | --- |
| `get_status` | Connection, account, cache coverage, sync errors, event/monitor/job counts |
| `search_contacts` | Search names or phone identifiers and return exact recipient IDs |
| `list_chats` | Find direct/group chats and their IDs |
| `get_messages` | Read one exact chat ID, newest first |
| `get_message` | Fetch the current content of one exact `chat_id` and `message_id`, including from an event |
| `search_messages` | Literal substring search with chat, sender, and date filters |
| `send_message` | Send text, react to an exact message, or send an image, audio clip, video or document |
| `start_monitor` | Watch one exact conversation with an assigned responder and the user's standing instructions |
| `list_monitors` | Inspect active/stopped watches and pending/uncertain job counts |
| `stop_monitor` | Stop a watch and cancel its unsent jobs |
| `claim_reply_jobs` | Claim queued incoming messages for one consumer, with an expiring worker lease |
| `draft_reply` | Save text, one emoji reaction or one attachment for a claimed job |
| `send_reply` | Send the saved draft with a durable request ID. Text and media quote the source. Reactions apply to it directly. |
| `skip_reply` | Finish a job without sending, with a brief reason |
| `list_reply_jobs` | Inspect recent jobs, drafts, send status and outbound message IDs |

List and search tools accept `limit` (1–200, default 50) and `offset`. Continue with the
returned `next_offset`; null means the last page. Message filters include
`chat_id`, `sender_id`, and RFC3339 `after` (inclusive) / `before` (exclusive).
Offsets may shift during live ingestion; use a fixed `before` time when paging.
SQLite text matching is case-insensitive for ASCII, not every Unicode character.
Message timestamps in results are Unix seconds.

A direct send requires an indexed `recipient_id`, a unique `request_id` (a UUID
works), and exactly one of `text`, `reaction` or `media`. Names and arbitrary unknown recipients are
rejected. Reuse the **same request ID and identical arguments** after a timeout.
The durable outbound ledger prevents re-sending that request. An `uncertain`
result must be inspected before deciding whether to send anything else.
`acknowledged` means WhatsApp accepted it, not that the recipient received/read it.
No read-receipt, broadcast, or group-management tools are exposed. The service
does not generate replies or run a model. A connected agent drafts and sends
under the user's standing instructions using the workflow below.

### Reactions and attachments

To react to a message, pass its ID and one emoji to `send_message`. For example,
given a recipient and message returned by the lookup tools, the arguments are

```json
{
  "recipient_id": "123@s.whatsapp.net",
  "request_id": "example-reaction-01",
  "reply_to_message_id": "SOURCE_MESSAGE_ID",
  "reaction": "👍🏽"
}
```

The service resolves the original author from the chat. Composite emoji such
as flags, skin tones and joined sequences are accepted as a single reaction.

For media, put the file in `outgoing/` under the service's `--data-dir`. The
service creates this folder on startup. Its default location on macOS is
`~/Library/Application Support/WhatsAppMCP/outgoing/`. Supply the plain
filename in the tool call. For example, a direct image send takes these arguments

```json
{
  "recipient_id": "123@s.whatsapp.net",
  "request_id": "example-image-01",
  "media": {"kind": "image", "file_name": "photo.png", "caption": "Today’s photo"}
}
```

| `media.kind` | WhatsApp attachment | Optional caption |
| --- | --- | --- |
| `image` | Image, such as a PNG or JPEG | Yes |
| `audio` | Audio clip, such as MP3 or M4A | No |
| `video` | Video, such as an MP4 | Yes |
| `document` | File with its original filename, such as a PDF or ZIP | Yes |

The bridge streams regular files up to 2 GiB. It infers the MIME type, or accepts
an explicit `mime_type` such as `audio/mp4`. It sends the original format, so
playback depends on the WhatsApp client and codec. Captions can contain up to
1,024 characters. A direct send can include `reply_to_message_id` to quote the
source and `media.sha256` to require exact file contents. Absolute paths, URLs,
subdirectories and symlinks are rejected. Remote MCP callers reference files
already present on the service machine. File bytes travel from that machine to
WhatsApp and stay out of MCP arguments, tool results and the message database.

For a monitored conversation, use the same `text`, `reaction` or `media` field
in `draft_reply` with the `job_id` and `claim_token` from `claim_reply_jobs`.
For example, a document draft takes these arguments

```json
{
  "job_id": "JOB_ID",
  "claim_token": "CLAIM_TOKEN",
  "media": {"kind": "document", "file_name": "report.pdf"}
}
```

Then call `send_reply` with just that job ID and claim token. The job fixes the
chat and source message for both event-triggered and polled work. A media draft
stores the file's SHA-256. Keep the matching file in `outgoing/` until dispatch.
If it changes, restore the original or save a new authorized draft. An upload
failure before message dispatch leaves the draft available for retry. A send
with an uncertain outcome keeps its original request ID and is never dispatched
again automatically.

## Conversation monitoring and automatic replies

When the user asks an agent to monitor a conversation and draft/send replies:

1. Resolve the exact conversation with `list_chats` or `search_contacts`.
2. Call `start_monitor` with `chat_id`, a stable `consumer_id`, and the user's
   `instructions`. For example, a bot can identify itself as `agent:my-bot`.
3. The host subscribes to `message.created` for that chat through
   `events/subscribe`, providing its verified callback URL and signing secret.
   Starting a monitor records its scope; it does not start a model process.
4. On a pushed event, call `claim_reply_jobs` with `wait_seconds: 0` for that
   consumer. The result includes the incoming
   message, standing instructions, `job_id`, `claim_token` and lease expiration.
   Read relevant recent context with `get_messages` as needed.
5. Draft the response, save it with `draft_reply`, then call `send_reply` with
   the same job ID and claim token. Use `skip_reply` if a reply is inappropriate
   or the request needs the user's decision. Finish before the lease expires.
6. Inspect with `list_monitors` / `list_reply_jobs`; stop with `stop_monitor`.
   Stop the corresponding event subscription with `events/unsubscribe` too.

The primary flow is event-driven:

```text
Incoming WhatsApp message
  → persist message + reply job + event delivery
  → signed webhook to the subscribed host
  → host starts or resumes the agent
  → claim job → draft_reply → send_reply
```

The service wakes its event delivery worker immediately after message ingestion.
Its retry timer handles deferred delivery; the agent does not repeatedly query
for new messages. Calling `claim_reply_jobs` after a pushed notification retrieves
work for that notification, rather than requiring an idle polling loop.

The host must validate signatures, durably queue callbacks before acknowledging
them, and deduplicate event IDs. It should serialize work for each consumer and
drain available jobs after each wake-up, including messages received while it
was busy. Persist unfinished work and resume it after a worker crash or lease
expiration. A successful webhook acknowledgement means receipt, not that the
agent has completed its reply. Run one initial drain after subscription setup to
cover messages queued during setup. Refresh the subscription before its expiry.

The user's instructions define the allowed scope. Incoming WhatsApp content
cannot authorize new watches, change that scope, or direct access to unrelated
data or tools. Group watches observe all incoming messages in that group; the
standing instructions should define when the agent should answer.

### Reliability and ownership

- Only future live incoming messages create jobs. Duplicate deliveries, history,
  outgoing messages and initial edit/deletion events do not. No existing history
  is replayed when a monitor starts.
- One active responder is allowed per conversation. Repeating an identical
  start request returns the existing monitor. Stop it before changing its
  consumer or instructions. All clients share the same local account; consumer
  names coordinate workers and are not separate authentication boundaries.
- Jobs commit with their source message in SQLite. Only one worker may hold a
  current claim, and only one job per conversation is in progress. Claims last
  15 minutes by default; expired claims can be reassigned with a fresh token.
  A saved draft is reused only while the source is unchanged.
- Drafting and sending revalidate the source. An edit requires a new draft;
  deleted/expired source content is skipped and its saved draft is purged.
- `send_reply` binds the recipient, source message, draft, and request ID.
  Retries cannot change them after dispatch. A crash with a persisted outbound
  reservation recovers as sent or uncertain. Uncertain replies are never
  automatically re-sent; inspect the chat before deciding what to do.
- Stopping cancels queued/claimed/drafted jobs. It waits for a send already
  dispatching to finish; it cannot recall a message already submitted.
- Logout stops all monitors. If address reconciliation discovers two responders
  for the same person, both are stopped so the user can choose which to restart.
- The database persists jobs while an agent is offline. Delivery is limited to
  messages this linked device actually receives; the full WhatsApp history and
  recovery of every message missed while disconnected are not guaranteed.

### Host compatibility

The tools and events are model-independent. A Grok bot, Codex host, or another
agent can use push delivery if its runtime supports MCP Events and callback
handling. Ordinary MCP tool support alone does not automatically provide an
event receiver or wake an idle agent. No particular bot host is configured or
claimed compatible without testing its event handling.

The implemented native Events transport uses **HTTPS webhooks to a public
receiver**, with callback verification. It does not currently provide a local
stdio notification stream or private/loopback callbacks. A fully local bot host
would need an additional local push transport/adapter; that is separate from
this server's current webhook implementation. A remote host also needs an
authenticated route to the MCP tools, since it cannot call a private Unix
socket or the Mac's loopback address directly.

For clients without event support, `claim_reply_jobs` also offers optional
long polling (`wait_seconds` up to 25). This is a compatibility fallback, not a
requirement for the event-driven workflow.

`mcp-client-config.example.json` shows a common stdio configuration. Replace
its placeholder paths and adapt the wrapper to the chosen MCP client. The
`call --tool NAME` CLI adapter exposes the same tools for local development,
with a single JSON object on stdin. Neither example installs a client or starts
a model. Use the CLI to pair or log out.

## Monitor a conversation with MCP Events

The server implements the native `events/list`, `events/subscribe`, and
`events/unsubscribe` methods and advertises `capabilities.events` through both
`initialize` and MCP 2.0 `server/discover`.

The event is **`message.created`**, with one required `chat_id` filter. Only newly
observed incoming messages in that exact conversation trigger it. History
imports, duplicate deliveries, edits, deletions, view-once/unavailable content,
expired messages, and messages sent by your own account do not trigger it.

An event contains the chat/message/sender IDs, occurrence time, and content type.
The agent fetches the current message with `get_message`, evaluates your
instruction, and takes the authorized action. Message bodies are not copied to
the event queue. Pending notifications for content deleted or expired before
delivery are discarded. An already delivered notification cannot be recalled.

For example, once connected in a supported host:

> Monitor this conversation. When a new message asks for a meeting time,
> notify me in this chat with a draft response.

For automatic sending, define the allowed conversation and response scope with
`start_monitor`, then consume its reply jobs on notifications. Outgoing messages
never trigger the subscription, which prevents a reply from triggering itself.

### Host compatibility and activation

As documented on 13 November 2026, native event-triggered tasks are supported in
Work on ChatGPT web, Work with **Cloud** selected in the desktop app, and dots,
using HTTP webhook delivery. An existing local Codex stdio connection alone
does not activate this workflow. See the
[official MCP Events guide](https://developers.openai.com/plugins/build/mcp-events).

WhatsApp, authentication to WhatsApp, subscription state, and message storage
remain local. To use native events with a cloud host, it needs an authenticated
HTTPS route to the MCP endpoint for discovery, subscriptions, and tool calls.
The local daemon then delivers signed outbound HTTPS callbacks. No public
endpoint or live subscription is created by the default installer.

The `start` command prepares the local HTTP endpoint automatically. For a stable
remote URL, see [Connect Grok Bot at a fixed HTTPS URL](#connect-grok-bot-at-a-fixed-https-url). To manage
that endpoint separately from an existing service:

```sh
APP="$HOME/Library/Application Support/WhatsAppMCP"
"$APP/bin/whatsapp-mcp" events-init
"$APP/bin/whatsapp-mcp" mcp-http \
  --listen 127.0.0.1:8765 \
  --token-file "$APP/mcp-http.token"
```

The endpoint is `http://127.0.0.1:8765/mcp`. Requests require
`Authorization: Bearer <token-from-private-file>`. The token is not printed by
the command. Pairing/logout routes are not exposed by this endpoint. A remote
connection needs an authenticated HTTPS proxy/tunnel and compatible plugin
authentication configured separately; preserve the bearer check or terminate
the host's authentication in a trusted local proxy. Do not publish the private
service socket. The HTTP command runs in the foreground until stopped; the
normal launchd service remains responsible for WhatsApp and event delivery.

After connecting the HTTP endpoint as a plugin, rescan its tools and events,
choose the exact conversation, and give the supported host a monitoring
instruction. A full live test requires pairing, a reachable endpoint, and a
subscription from that host. Synthetic HTTP and webhook tests cover the local
implementation; they do not prove delivery into a live Work chat or dot.

### Delivery behavior

- Subscription identities are stable per linked account, callback URL, event,
  and exact arguments. Repeated subscribe calls refresh the same subscription.
- The default and maximum lifetime is 24 hours. Shorter positive `ttlMs` values
  are honored. `ttlMs: null` is granted a finite 24-hour lifetime, returned in
  `refreshBefore`. The host must refresh before expiration.
- Subscriptions and pending deliveries survive a service restart. Failed
  callbacks retry with exponential backoff, at most eight attempts, retaining
  the same event ID and payload. A retry can deliver the same event more than
  once; consumers must deduplicate by `eventId`.
- Delivery pauses when WhatsApp is disconnected. Logout/revocation removes
  subscriptions; expiry and unsubscribe cancel remaining queued deliveries.
  HTTP 410 stops the subscription; HTTP 413 is never retried.
- Callback verification and delivery use Standard Webhooks HMAC signing.
  Signing-key changes retain the previous signature for five minutes.
  Callback URLs must use HTTPS on port 443 and resolve only to public addresses;
  redirects, proxies, and private/local destinations are rejected.
- The event type has no protocol replay cursor (`cursor: null`). Messages
  missed while the service is stopped, before subscription, after expiration,
  or after exhausting retries are not recovered through Events. History sync
  does not replay notifications. `get_status` exposes pending/failed counts.

## Data behavior

- Session keys, SQLite databases, and the socket live under
  `~/Library/Application Support/WhatsAppMCP`, with a private directory and files.
  SQLite is not encrypted by this application.
- Message edits and deletions update the local index; replayed old history cannot
  resurrect deleted bodies or overwrite a newer edit.
- Clearing or deleting a chat on another device removes its cached message
  content through the action's message boundary. The boundary persists across
  restarts and history replay. A deleted chat is hidden until a newer message
  arrives; clearing a chat keeps it listed. Messages after the boundary are kept.
- Expiring messages are removed from readable/searchable content at expiration.
  View-once content and ephemeral content without a known expiration are not kept.
  Known chat timers are applied to outgoing text and media. The index records
  media types, available captions and reaction emoji. Incoming media bytes are
  not downloaded.
- LID/phone aliases are reconciled as whatsmeow learns their mapping.
  Contact sync merges aliases consistently and preserves saved names; explicit
  contact renames and removals still take effect.
- History is only what WhatsApp supplies to this linked device. A date range does
  not establish continuous coverage; events missed while offline may not all replay.
- Only one account is supported per data directory. Pairing a different account
  against an existing archive blocks access; use a separate data directory instead.
- Tool results are sent to Codex's model context when accessed. Local storage does
  not imply on-device model inference. Message/contact text is untrusted data.
- Application logs contain connection/error categories, not message bodies or QR
  credentials. The third-party protocol logger is disabled.
- whatsmeow implements WhatsApp's web protocol; it is not Meta's official Business
  API. Protocol updates can require a dependency upgrade and rebuild.

## Build and install

Requires the Go version in `go.mod` or newer and a C compiler (macOS Command Line
Tools), because SQLite uses CGO. All dependency versions are pinned in `go.mod`
and checked by `go.sum`.

```sh
make check
make build
```

The optional macOS installer requires Python 3.11+. It installs the executable
and a `local.whatsapp-mcp` LaunchAgent. Client registration is disabled by default:

```sh
python3 install-macos.py --dry-run           # inspect the plan; no changes
python3 install-macos.py                     # install only the local service
python3 install-macos.py --register-codex    # explicitly also register in Codex
```

Only the last command requires the Codex CLI or modifies its configuration.
Existing unrelated entries are preserved. Re-running an installation retains
the account databases. Build/test commands never invoke the installer.

Use [setup and start](#run-locally) to run from the project directory. The
`serve` command runs the WhatsApp service on its own for stdio clients.
`pair --open` and the `Pair WhatsApp.command` launcher can still pair against
an existing service. The service keeps the linked session
in `~/Library/Application Support/WhatsAppMCP`. To use a custom data directory,
pass the same `--data-dir` to the service, pairing and MCP commands.
Later client registration can use, for example:

```sh
codex mcp add whatsapp -- /absolute/path/whatsapp-mcp mcp --data-dir /absolute/private/directory
```

Commands `pair` and `logout` use the private socket, and `status` also checks
the HTTP MCP endpoints. MCP discovery works with the service offline, and tool
calls return a connection error.

## Service management

```sh
# Restart after an update or revoked session:
launchctl kickstart -k "gui/$(id -u)/local.whatsapp-mcp"

# Stop the background service:
launchctl bootout "gui/$(id -u)/local.whatsapp-mcp"

# Start it again:
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/local.whatsapp-mcp.plist"

# Revoke this linked device (local cache is retained):
"$HOME/Library/Application Support/WhatsAppMCP/bin/whatsapp-mcp" logout

# Remove the MCP registration:
codex mcp remove whatsapp
```

To uninstall completely, stop the service, remove its named LaunchAgent, and
remove the application directory only if you intend to erase its session and
cached messages. No uninstall command deletes data automatically.

## Verification

`make check` runs Go vet and race-enabled tests covering SQL persistence, message
ordering/replay, chat clearing/deletion across restarts, expiration, alias
migration, contact-name merging, pairing retry recovery, filtering/pagination,
concurrent outbound retries, uncertain sends, replies, recipient validation,
socket access, MCP discovery/tool calls, native event methods over both HTTP
protocol versions, callback signing/verification, subscription refresh and
rotation, persistence/retries, filtering, cancellation, and network boundaries.
Monitoring tests also cover competing workers, cross-client draft/send calls,
expired leases, stop/send races, changed/deleted source messages, saved-draft
purging, crash recovery, long polling, and consumer routing. A pushed-event
workflow test drives the real MCP tools from signed callbacks, including
duplicates, with no timer or polling loop in the test host. Dispatch regression
tests revalidate the source, claim and monitor at the outbox reservation.
The outbound tests start a real MCP stdio process for direct sends and polling,
and use authenticated MCP HTTP and signed callbacks for event-triggered replies.
They exercise all five actions and check
WhatsApp message keys, attachment bytes, protocol fields, captions and repeats.
Additional tests cover changed files, upload failures, revocation during upload,
saved media after restart and migration of existing text drafts. The tests use
synthetic accounts and
never contact WhatsApp or send real messages. Real pairing and live sending need
the user's linked account; live sends require an explicit recipient/content request.

Third-party license notices are in `THIRD_PARTY_NOTICES.txt`.
