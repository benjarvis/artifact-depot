---
name: rebase
description: Rebase the current branch onto main.
disable-model-invocation: true
allowed-tools: Bash(git *), Bash(make *)
---

Rebase the current branch onto main.

## Process

1. Run `git rebase main`.
2. If there are conflicts, resolve them and continue the rebase.
3. Verify the result with `git log --oneline -5`.
4. **MANDATORY**: Run `make test` after the rebase completes successfully. The rebase may have introduced incompatibilities between main's changes and the branch's changes. Tests MUST pass before the branch is considered ready. Do NOT skip this step — we cannot break main.
