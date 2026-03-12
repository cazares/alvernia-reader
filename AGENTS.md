# AGENTS.md

This file defines user-facing custom directives for this repository.

## Directive Format

- Use slash commands at the start of a message.
- Format: `/directive [optional-arguments]`
- Example: `/test tests/test_step2_split.py`

## Custom Directives

| Directive | What it does |
| --- | --- |
| `/status` | Show current branch, working tree summary, and most recent commits. |
| `/plan` | Produce a concrete execution plan, then begin implementation. |
| `/implement <task>` | Implement the request immediately, run relevant checks, and commit. |
| `/test [path-or-pattern]` | Run targeted tests. If omitted, run the most relevant suite for current changes. |
| `/commit <message>` | Create a commit with the provided message for current tracked changes. |
| `/pr` | Push current branch and open or update a PR against `main` with a concise summary and validation notes. |
| `/handoff` | Generate a compact handoff summary with branch, commits, changed files, test status, and next steps. |
| `/review` | Perform findings-first code review with severity, file references, and residual risks. |
| `/cleanup` | Remove temporary artifacts created by the agent and leave unrelated user files untouched. |
| `/checkpoint <message>` | Save progress now: run quick validation, then commit with the message. |

## Safety Rules

- Never merge or deploy to `main`/`master` without explicit user approval.
- Do not delete or revert unrelated local changes.
- Leave untracked user files untouched unless explicitly asked to modify them.
- Prefer focused tests first, then broader validation when needed.

## Branching + PR Defaults

- New work should use a branch named `dev-<short-topic>`.
- Commit early and often with focused messages.
- If user asks to deploy, open PR first and wait for explicit merge permission.
