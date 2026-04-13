---
name: push
description: Rebase on main, merge to main, and push to the remote. Handles worktree constraints when main is checked out elsewhere.
disable-model-invocation: true
allowed-tools: Bash(git *), Bash(make *)
---

Merge the current branch into main and push to the remote.

## Context

The `main` branch is typically checked out in `/artifact-depot`. When working from a different worktree (e.g., under `/worktrees/`), you cannot `git checkout main` because git prohibits checking out a branch that is active in another worktree. In that case, use the fast-forward ref update approach described below.

If you are running directly in `/artifact-depot` (i.e., main is your current worktree), use the normal merge flow.

## Process

1. Ensure your branch is rebased onto main:
   ```
   git rebase main
   ```

2. **MANDATORY**: Run `make test` after rebasing. The rebase may have introduced incompatibilities between main's changes and the branch's changes. ALL tests MUST pass before pushing. Do NOT skip this step — we cannot break main. If tests fail, fix the issues before proceeding.

3. Verify that a fast-forward merge is possible:
   ```
   git merge-base --is-ancestor main HEAD && echo "fast-forward possible"
   ```
   If this fails, your branch is not rebased onto main. Rebase first.

4. Merge into main:

   **If you are in a worktree (not `/artifact-depot`):**
   ```
   git update-ref refs/heads/main HEAD
   git push origin main
   ```
   Do NOT `cd /artifact-depot` or run git commands there — multiple agents may be working concurrently in that worktree.

   **If you are in `/artifact-depot` (main is your branch):**
   ```
   git merge <branch-name>
   git push origin main
   ```

5. Verify the push succeeded by checking the log:
   ```
   git log --oneline -3 main
   ```

## Rules

- ONLY fast-forward merges are allowed. If the merge is not a fast-forward, rebase your branch first.
- When in a worktree, NEVER `cd /artifact-depot` or run git commands in that directory — other agents may be using it.
- Always verify the fast-forward is possible before updating the ref.
