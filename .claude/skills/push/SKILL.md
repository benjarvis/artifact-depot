---
name: push
description: Push the current branch to the origin fork and raise a pull request against the upstream project.
disable-model-invocation: true
allowed-tools: Bash(git *), Bash(make *), Bash(gh *)
---

Push the current branch to the origin fork and raise a pull request against the upstream project.

## Context

All changes go through a pull request. The repository has two remotes:

- `origin` is the user's fork (for example `benjarvis/artifact-depot`). Branches are pushed here.
- `upstream` is the canonical project (`artifact-depot/artifact-depot`). Pull requests are raised here, with `main` as the base.

Never push directly to `main` on either remote.

## PR description rules

- Plain ASCII only. No em-dashes, no smart quotes, no emojis, no unicode symbols. Use `-` and `--` where you would otherwise reach for an en- or em-dash.
- Describe only the code changes applied. Focus on what was changed and why.
- Do NOT mention: tests executed, test plans, tools used, review feedback, code review, linting, formatting runs, or any process/workflow details.
- Do NOT include a "Test plan" or similar checklist section.
- Start with a single `## Summary` section containing a short bullet list or prose description. Additional sections are only allowed if the user explicitly asks for them.
- Wrap body lines at 72 characters.

## PR title rules

- Plain ASCII only, under 72 characters, imperative mood.
- Match the style of the primary commit on the branch when it is a single-commit PR.

## Process

1. Ensure the branch is rebased onto `upstream/main` (or `origin/main` if `upstream` does not exist):
   ```
   git fetch upstream main 2>/dev/null || git fetch origin main
   git rebase upstream/main 2>/dev/null || git rebase origin/main
   ```

2. **MANDATORY**: Run `make test` after rebasing. The rebase may have introduced incompatibilities between main's changes and the branch's changes. All tests MUST pass before pushing. If tests fail, fix the issues before proceeding.

3. Run the following commands in parallel to understand what will go into the PR:
   - `git status` to confirm the working tree is clean.
   - `git log --oneline upstream/main..HEAD` (fall back to `origin/main..HEAD`) to see every commit that will be part of the PR.
   - `git diff upstream/main...HEAD` (fall back to `origin/main...HEAD`) to see the full diff.
   - `git remote -v` to confirm the origin and upstream URLs.

4. Push the branch to the user's fork:
   ```
   git push -u origin HEAD
   ```
   If the branch already exists on `origin`, this updates it and any open PR picks up the new commits automatically.

5. Check whether a PR already exists for this branch against the upstream:
   ```
   gh pr list --repo <upstream-owner>/<upstream-repo> --head <fork-owner>:<branch> --state open
   ```
   If a PR is already open, stop here and report its URL. The push in step 4 has already updated it.

6. If no PR exists, draft a title and body according to the rules above. Analyze ALL commits on the branch (not just the latest), and summarize the combined change set.

7. Create the PR, passing the body via a HEREDOC to preserve formatting:
   ```
   gh pr create \
     --repo <upstream-owner>/<upstream-repo> \
     --base main \
     --head <fork-owner>:<branch> \
     --title "<title>" \
     --body "$(cat <<'EOF'
   ## Summary

   - <bullet describing a change>
   - <bullet describing another change>
   EOF
   )"
   ```

8. Report the PR URL back to the user.

## Deriving owners and branch name

- `<branch>` is the current branch: `git branch --show-current`.
- `<fork-owner>` is the owner segment of the `origin` URL. Extract with:
  ```
  git remote get-url origin | sed -E 's#^.*[:/]([^/]+)/[^/]+(\.git)?$#\1#'
  ```
- `<upstream-owner>/<upstream-repo>` is the path of the `upstream` URL (without the `.git` suffix). Extract with:
  ```
  git remote get-url upstream | sed -E 's#^.*[:/]([^/]+/[^/]+)(\.git)?$#\1#'
  ```

## Rules

- NEVER push directly to `main` on either remote.
- NEVER force-push to a branch you did not create.
- NEVER use `--no-verify` or otherwise bypass hooks.
- If `gh` is not authenticated (`gh auth status` fails), ask the user to run `gh auth login` rather than attempting workarounds.
- If the `upstream` remote is missing, stop and ask the user how to proceed rather than silently targeting `origin`.
