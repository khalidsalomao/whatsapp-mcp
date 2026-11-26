---
name: goal-oriented-orchestration
description: Drive complex work to verified completion — direction, investigation, implementation, review, and revision — directly or by coordinating agents, sessions, and workflows. Use when asked to execute or finish non-trivial work to completion or to orchestrate agents, when work spans several steps, agents, or sessions, or when work triggers a pre-code critic.
---

# Goal-oriented orchestration

You own the outcome and the direction through verified done. `AGENTS.md` → Goal completion sets done, scope, expansion evidence, direction checks, and stop conditions; this skill runs that loop. Implement directly or delegate, task by task: delegate work that is independent, needs a fresh reviewer, or would outrun your context. Keep a goal file when work spans multiple slices, coordinates multiple agents or sessions, or risks context limits; otherwise run the loop inline.

## Goal file

When the work needs one (above), keep one file per goal at `.agents/scratchpad/<goal-slug>.md` from the worktree root (`.agents/scratchpad/` is git-ignored; keep secrets out). Write only your own goal's file. When resuming — after compaction, in a new session, or on CI and review events — read the file whose branch or PR matches the current work, and check it against the branch, commits, and PR.

Treat it as a compact live execution board: the goal's outcome, acceptance criteria, non-goals, what must keep working, decisions, expansions, and ruled-out approaches with evidence, execution state (done / in progress / next / blocked), active and finished delegations (with agent/session refs), branch, and PR link. Update it at real transitions and rewrite stale entries to keep it compact.

When the goal is done (after its PR merges, when it has one) or dropped, delete the goal file and the scratch files it used; the PR or final report keeps the record. When work pauses, keep the file and mark it paused with its next action.

## Direction

Settle the approach before implementation. First check the goal against its current base branch (default `main`); when merged work already covers part of it, report that and re-scope to what remains. Map the callers, sibling paths, and user flows the goal touches, and name each place the evidence shows the problem recurs: cover those within the goal; one beyond it is an expansion only when it meets `AGENTS.md` → Goal completion, otherwise it goes under Not done. When the design is open, compare independent approaches. When the choice meets the critic conditions in `AGENTS.md` → Goal completion, have an independent agent try to falsify it before building (or resuming after reopening direction) — briefing set-aside alternatives when a design choice was open, and testing whether a simpler approach, an existing repo path, or the dependency's documented APIs would meet the goal — and re-run the critic if confirmed findings materially change the approach, boundaries, or other critic-triggering choices. Acceptance criteria describe outcomes; a criterion that dictates the mechanism is part of the direction and faces the same challenge.

When the step-back signals in `AGENTS.md` fire, or agent evidence contradicts the recorded direction, reopen it before more investigation.

## Delegation

Follow `AGENTS.md` → Delegation, adding a bounded task within the whole goal, what must keep working, and how to check each done criterion. Investigation briefs ask what occurs and what fails, with evidence; an open request for other possible cases returns inputs nobody has observed. A check of new behavior counts only if it fails without the change; for a measured number, that means a gain beyond run-to-run noise against a baseline measured under the same conditions. Mark each stated finding as reproduced (how, where) or inferred from code; for an inferred one, the first done criterion is reproducing it.

In each brief, allow expansions local to the task. Expansions that reach other areas, add layers, or change working behavior come back with evidence for you to decide.

Agents use the session model unless the user names another. A cheaper model fits only mechanical tasks with a check that fails on wrong work.

Size agents and checks to the change's risk; a small fix needs only the floor in `AGENTS.md` → Goal completion.

Sequence dependent work; parallelize independent work with clear ownership of shared files. Choose the form by how the work returns:

- **Agents** report to you within the session.
- **Workflows** script fan-out and adversarial verification when the user opted in.
- **Separate sessions** fit an independent goal with its own branch or PR. Each works in its own worktree and keeps its own goal file naming its parent session; verify its branch or PR before relying on its report.

Ask agents to return their conclusion, evidence, ruled-out approaches, expansions made or proposed, limitations, and whether their done criteria were met and checked.

## Context

Reuse an agent's conversation for related follow-up; send changed facts, new evidence, and the next goal without repeating history. When reuse is unavailable, hand off compactly (using the goal file when one exists, keeping independent reviewers isolated from other agents' conclusions).

Give independent agents and reviewers fresh conversations with the goal, constraints, artifacts, and evidence needed to judge the work, without other agents' conclusions presented as facts. For post-fix verification, align with `review-changes`: verify trivial, self-evident fixes directly; reuse the original reviewer only to confirm a localized fix to its own finding; run a targeted fresh re-review (plus invalidated checks and smoke test) for substantive fixes introducing new logic, state, contracts, or CLI/MCP behavior.

## Loop

Confirm all delegated work is resolved (completed, integrated, or explicitly cancelled/superseded) before final review and before done. Before done — and after substantive follow-up edits — review the combined change with `review-changes` (which governs fixes, reviewer reuse vs fresh re-review, and when to stop) and ensure checks and smoke tests reflect the final state. Verify each agent's expansions and completion claims against current artifacts and behavior, confirming the combined outcome still fits the spirit of the goal, YAGNI, project conventions, CLI/MCP UX quality, and any open bottom-line decisions. Return to `## Direction` whenever fixes expose structural or design friction rather than local bugs.

Done is the verified outcome in `AGENTS.md` → Goal completion; follow its commit and push rule, open a PR when the workflow uses PRs or the user asks, and leave merging to the user.

## Reporting

Keep progress updates to the bottom line. Report decisions, evidence, and blockers; state uncertainty precisely.
