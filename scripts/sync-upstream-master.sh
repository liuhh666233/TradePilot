#!/usr/bin/env bash

set -euo pipefail

TARGET_BRANCH="${TARGET_BRANCH:-master}"
UPSTREAM_REMOTE="${UPSTREAM_REMOTE:-upstream}"
UPSTREAM_URL="${UPSTREAM_URL:-git@github.com:liuhh666233/TradePilot.git}"
PUSH_ORIGIN="${PUSH_ORIGIN:-0}"
MIRROR_ORIGIN="${MIRROR_ORIGIN:-0}"

require_clean_worktree() {
  if [[ -n "$(git status --porcelain)" ]]; then
    printf 'Refusing to sync: worktree is dirty. Commit or stash changes first.\n' >&2
    exit 1
  fi
}

ensure_upstream_remote() {
  if git remote get-url "$UPSTREAM_REMOTE" >/dev/null 2>&1; then
    return
  fi

  git remote add "$UPSTREAM_REMOTE" "$UPSTREAM_URL"
}

sync_checked_out_branch() {
  require_clean_worktree
  git merge --ff-only "$UPSTREAM_REMOTE/$TARGET_BRANCH"
}

sync_detached_branch_ref() {
  git show-ref --verify --quiet "refs/heads/$TARGET_BRANCH" || {
    git branch "$TARGET_BRANCH" "$UPSTREAM_REMOTE/$TARGET_BRANCH"
    return
  }

  git branch -f "$TARGET_BRANCH" "$UPSTREAM_REMOTE/$TARGET_BRANCH"
}

main() {
  if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    printf 'This script must run inside a git repository.\n' >&2
    exit 1
  fi

  ensure_upstream_remote
  git fetch "$UPSTREAM_REMOTE" "$TARGET_BRANCH"

  current_branch="$(git branch --show-current)"
  if [[ "$current_branch" == "$TARGET_BRANCH" ]]; then
    sync_checked_out_branch
  else
    sync_detached_branch_ref
  fi

  if [[ "$PUSH_ORIGIN" == "1" && "$MIRROR_ORIGIN" == "1" ]]; then
    printf 'Choose only one of PUSH_ORIGIN=1 or MIRROR_ORIGIN=1.\n' >&2
    exit 1
  fi

  if [[ "$PUSH_ORIGIN" == "1" ]]; then
    git push origin "$TARGET_BRANCH"
  fi

  if [[ "$MIRROR_ORIGIN" == "1" ]]; then
    git push --force-with-lease origin "$TARGET_BRANCH:$TARGET_BRANCH"
  fi

  git rev-parse --short "$TARGET_BRANCH"
}

main "$@"
