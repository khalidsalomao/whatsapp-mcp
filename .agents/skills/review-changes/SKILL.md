---
name: review-changes
description: Review implemented changes against their goal and fix what holds up — gaps, regressions, excess, duplication, and inefficiency. Use when asked to review or simplify changes, "/review-changes", or after completing a feature or fix. Do not use to review a plan before code exists.
---

# Review Changes

Leave the change correct, complete, and as simple as its goal allows. Judge it as a whole — goal, affected feature, user-visible behavior — against the bar in `AGENTS.md` → Reviews and Findings; the code values (reliable, sound, complete, YAGNI, resilient at external boundaries) define what counts as a gap or excess.

## Scope

Review the branch diff against its merge base plus uncommitted work, unless the user names another scope. Establish the goal, non-goals, intended behavior, and significant decisions with their evidence from the request, commits, conversation, and goal file, and trace the blast radius: callers, contracts, data the change touches, and code it makes run for the first time.

## Review

Check the direction, then look both ways:

- **Direction:** whether the approach serves the spirit of the goal and fits the architecture, project conventions, and YAGNI without hacks, workarounds around an owner, or overcorrection; which decisions stay open; and whether the result is a good user experience (flow, copy, CLI/tool output, errors, empty states), judged from a smoke test when the change is observable.
- **Gaps:** anything the bar names, failures at external boundaries the app could recover from but does not, places within the goal where the evidence shows the problem recurs that the change misses, and parts of the goal that do not work end-to-end.
- **Excess:** what neither the goal nor evidence needs — fixes broader than their problem, speculative options or layers, handling for states that cannot occur, duplicates of existing helpers or patterns, redundant state or logic, and unnecessary work (N+1 queries, over-fetching, unbounded fan-out, repeated calls, no-op state updates, hot-path additions, leaks).

Scale reviewers to risk: one for a small contained change; for larger ones, independent reviewers with change-specific focuses, free to follow evidence beyond them, plus a critic as falsifier on the diff when the change meets the critic conditions in `AGENTS.md` → Goal completion. Brief each with the user's request for the work in their own words, the bar, goal, non-goals, intended behavior, significant decisions with their evidence, and touched surface. Reviewers may propose a simpler approach when the diff shows excess, and challenge a decision or a non-goal the user did not set when evidence shows it defeats the request.

## Fix

Wait for every reviewer, then validate each finding against the current code, callers, contracts, and runtime behavior. Fix confirmed material findings at the cause; removing code counts as a fix. Decline the rest with a reason. Run proportionate validation and inspect the resulting diff. Verify trivial, self-evident fixes directly; for substantive fixes (logic, state, contracts, multi-file behavior, CLI/MCP surface), run a targeted fresh re-review of the revised diff and re-run any invalidated checks or smoke test until a pass leaves no unverified material findings. Step back to rethink the design if another pass still hits structural bugs or you are patching your own earlier fixes.

## Report

List fixes and removals, declined findings with reasons, validation run (including the smoke test, or why none ran), open decisions with a recommendation, follow-ups the goal leaves (capability gaps, consistency, UX, bugs; no chores), and anything unresolved — or confirm the change is clean.
