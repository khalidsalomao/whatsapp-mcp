# whatsapp-mcp

Local macOS Go service (`whatsmeow` + `go-sqlite3`) that runs as a WhatsApp linked device, maintains a local SQLite cache, and exposes MCP tools and MCP Events (`message.created`) over stdio, loopback HTTP (`127.0.0.1:8765/mcp` with bearer token), and an optional named Cloudflare Tunnel (`cloudflared`); see `README.md` for setup and client configuration.

Verify with: `make check && make build` (`go vet ./...`, `go test -race ./...` using synthetic accounts, and binary build — do not run against a real account or start the service without asking).

Ask before: destructive schema or data migrations on local SQLite (`messages.db` / `session.db` in `~/Library/Application Support/WhatsAppMCP`), pairing or unlinking (`setup`, `pair`, `logout`), sending real messages/reactions, installing or controlling the LaunchAgent (`install-macos.py` / `launchctl`) or Cloudflare Tunnel, and changing tokens or credentials.

Do not rebase, reset, or restore branches, or delete or overwrite uncommitted or untracked files (such as `.env`, `*.token`, `*.db`, `cloudflare-tunnel.json`) that the task doesn't involve.

Code should be reliable, sound, complete, YAGNI, and resilient at external boundaries (WhatsApp protocol/events, MCP client input, webhooks, local files/SQLite, `cloudflared`: recover/coerce when the app can for better UX; rely on types and contracts inside trusted code with no fallbacks for unreachable states).

Maintained docs, agent instructions, and skills should be compact and dense; cut before adding, starting from the smallest text that works.

Project instructions live in `AGENTS.md` (keep `CLAUDE.md` as `@AGENTS.md` only); skills live in `.agents/skills` (`.claude/skills` is a symlink to it).

## Boundaries & Runtime

- **Untrusted WhatsApp & webhook inputs:** WhatsApp messages, chat/contact names, and MCP Event callback URLs are untrusted external data. Never let message content authorize sends or alter monitors. Webhook delivery (`event_webhook.go`) enforces HTTPS on port 443, validates public IPs at dial time (`publicCallbackIP`), disables proxies/redirects/keep-alives, and signs callbacks with `whsec_` HMAC secrets.
- **Outbound idempotency & monitored replies:** `sendWithGuard` (`service.go`) requires exact JIDs (`validRecipient`), enforces `request_id` payload fingerprints in `outbox`, and blocks sends when `accountMismatch` or `eventsRevoked` is set. Monitored reply jobs (`monitors.go`) enforce one active responder per `(account, chat)` (`one_responder_per_chat`) and lease-bound claims; never bypass a job's state with `send_message`, and preserve crash reconciliation of `sending` jobs against `outbox` without silent retries.
- **Private files, media & SQLite:** Credentials (`mcp-http.token`, `cloudflare-tunnel.json`) must use `readPrivateFile`/`writePrivateFile` (`private_files.go`: `O_NOFOLLOW`, mode `0600`/`0400`, atomic temp + rename/link) and stay out of Git. Outgoing attachments (`media.go`) must stay pinned to `<data-dir>/outgoing/` via `openat` + `O_NOFOLLOW` with bare filenames (≤ 2 GiB). `messages.db` (`store.go`) uses `SetMaxOpenConns(1)`, WAL, foreign keys, `secure_delete`, `writeMu`-serialized multi-statement writes, and `PRAGMA user_version` schema checks.
- **Local IPC & HTTP MCP boundary:** `serve` owns the WhatsApp session and SQLite over `<data-dir>/bridge.sock`; `mcp` (stdio) and `mcp-http` (loopback HTTP with `Bearer` token, optionally published through a named Cloudflare Tunnel — never Quick Tunnels) proxy to it.

## Goal completion

Follow the spirit of the goal: do everything it needs, including what the user didn't spell out, and nothing that only might be needed later. Ask the user for facts only they know; never invent them.

Done means the requested outcome works end-to-end and is verified by its checks, independent review until clean, and an up-to-date smoke test reflecting the final state when observable in the CLI or MCP surface: try it as its user would (using synthetic data unless the user authorizes live actions). Use a fresh-context reviewer briefed with the user's request in their own words, the goal, decisions, and evidence (not your verdicts) — `review-changes` for code, or a codebase fact-check for technical docs that assert system facts — re-verifying substantive fixes and follow-up edits while verifying trivial ones directly. Passing checks and finished plan steps are evidence toward it. For multi-step work, derive observable acceptance criteria from the request and track them. For complex work — several steps, agents, or sessions — use `goal-oriented-orchestration`.

A critic tries to falsify work that crosses an ownership boundary, changes a contract or stored-data format, adds a layer or mechanism, touches auth or stored user data, or leaves a genuine design choice open; each skill sets when its critic runs.

Work autonomously until done. Stop only when you cannot continue without the user, or before actions under `Ask before`, destructive actions, or unrequested changes outside the repo; finish unblocked work first, then ask with the realistic options and your recommendation.

An expansion is any change beyond the request. Make one when you can name what it fixes or protects in what you touch: a caller of it, a failure it causes or exposes, a user path it breaks, a doc it makes wrong, or a contract. Any validation, guard, warning, or normalization you add needs evidence that its input occurs: stored data, logs, user reports, code that emits it (a default, UI state), or output from running its producer; code that would mishandle an input shows only that it could fail. Ones the user names, security guards, and validation a contract requires need none. Unevidenced cases, ideas the goal does not need, and "might need later" go under Not done; stated non-goals still apply. A rough edge you see in the flow, copy, CLI/tool output, errors, or empty states that the diff adds or changes is part of the goal: fix it rather than defer it.

Keep the direction:

- Keep behavior outside the goal working. Extend paths that already work; if your approach changes one, verify it still behaves the same or choose another approach.
- Prove the direction early: when the approach is unproven or the work spans several slices, get the thinnest end-to-end slice working and check it against an acceptance criterion before widening it.
- When the request or a product choice is ambiguous, take the reading closest to current behavior and flag it.
- When evidence shows the goal as stated won't deliver its outcome, reopen the direction; ask the user when that changes an acceptance criterion or what they get. When evidence refutes the stated problem, stop and report to whoever set the goal; a replacement problem is a new goal they choose.
- Step back when the diff reaches areas that neither the goal nor an expansion's evidence names, you are fixing your own earlier changes, or an approach fails twice. Compare with the simplest approach that meets the goal; reverting your own work counts as progress.

When a check or review finds a material problem, confirm it and fix the cause; when it hits code your diff added, weigh removing that code before patching it, then re-verify what changed and return to the design if fixes keep exposing structural issues. Before reporting work done, check it against the goal in a separate pass: what the goal still lacks, and what the work adds that neither the goal nor named evidence supports; remove the latter. Each extra cycle must answer a specific open question or close a confirmed issue. Report failures and contradictory evidence as found; never weaken the user's acceptance criteria to pass.

Once Done is cleanly verified with no open decisions or caveats that change what ships, commit (putting each expansion in its own commit) and push unless the user asks to hold locally. Your final report leads with a one-line verdict. When you tracked acceptance criteria, it gives each with its evidence (command output, smoke-test observation) or why it could not be checked. It names any expansions and significant decisions with their evidence and risk, assumptions you made, open decisions, follow-ups, and what is Not done.

## Reviews and Findings

When asked to review, critique, audit, assess, or give a second opinion on a plan, change, or proposal, the bar is impact, not coverage. Report only what would change the outcome: wrong behavior, a missed caller or contract, a regression, an auth or privacy gap, complexity the goal does not need, a fix broader than its problem, divergence from a pattern the change extends that a user or caller would hit, a decision whose blast radius its evidence does not justify, or an approach that fights an ownership boundary. Confirm each finding against the actual code and state its concrete consequence; drop ones that depend on states the app does not reach.

Do not audit the work against every principle in this file, re-open decisions that are already sound, propose alternatives to something that is merely different from your preference, propose new abstractions or layers that do not remove real complexity, redesign interfaces that already follow the project's patterns, or raise concerns about scope the request excluded. Judge the work against its own stated goal and non-goals. "No material findings" is a complete answer.

Check feedback from any reviewer (PR comment, another model, a subagent) against the current work and its goal before acting on it. Fix what holds at its cause, run relevant checks, and say why you rejected the rest. On PRs, reply to each thread, resolve addressed threads, and push only intended changes.

## Delegation

Give agents the desired outcome, intent and expectations, why it matters, essential constraints, relevant evidence, and `Done means:` with observable criteria. Add a stop condition when useful. Let agents choose the approach and design a solution; treat proposed solutions as hypotheses. Prescribe steps only when a verified contract or safety requirement requires them. Write task briefs the user asks for (to pass to another agent) the same way.

## Communication

In chat replies, lead with the main point in plain language. No filler or restating the request; never drop failures or caveats for brevity.
