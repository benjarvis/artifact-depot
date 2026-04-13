---
name: commit
description: Create a git commit with a well-formatted message describing only code changes.
disable-model-invocation: true
allowed-tools: Bash(git *)
---

Create a git commit for the currently staged and unstaged changes.

## Commit message rules

- Plain ASCII only. No emojis, no unicode symbols.
- Describe only the code changes applied. Focus on what was changed and why.
- Do NOT mention: tests executed, tools used, review feedback, code review, linting, formatting runs, or any process/workflow details.
- Keep the subject line under 72 characters.
- Use imperative mood (e.g., "Fix race in drive coordinator" not "Fixed race in drive coordinator").
- If additional detail is needed, add a blank line after the subject and then body paragraphs, each wrapped at 72 characters.

## Process

1. Run `git status` and `git diff` to review all changes.
2. Run `git log --oneline -5` to see recent commit style for reference.
3. Stage relevant files (prefer explicit file names over `git add -A`).
4. Draft a commit message following the rules above.
5. Create the commit.
6. Run `git status` to verify success.
