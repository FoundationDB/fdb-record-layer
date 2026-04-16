#!/usr/bin/env python3
"""
Summarize post-approval changes in the last N merged PRs.

For each merged PR, finds the last reviewer approval, identifies any commits
pushed after that approval, and shows commit messages plus a diffstat.

Requires the `gh` CLI to be installed and authenticated.
"""

import argparse
import json
import subprocess
import sys

# ANSI color codes
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"


def gh(*args: str) -> str:
    """Run a gh CLI command and return its stdout."""
    result = subprocess.run(
        ["gh", *args],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def gh_api(endpoint: str) -> list | dict:
    """Call the GitHub API via gh and return parsed JSON. Handles pagination."""
    raw = gh("api", endpoint, "--paginate")
    if not raw:
        return []
    # --paginate can produce concatenated JSON arrays; merge them
    # e.g. "[{...}][{...}]" -> we need to handle that
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Concatenated arrays from pagination: "[...][...]"
        merged = []
        decoder = json.JSONDecoder()
        pos = 0
        while pos < len(raw):
            raw_stripped = raw[pos:].lstrip()
            if not raw_stripped:
                break
            obj, end = decoder.raw_decode(raw_stripped)
            if isinstance(obj, list):
                merged.extend(obj)
            else:
                merged.append(obj)
            pos += len(raw) - len(raw_stripped) - pos + end
        return merged


def get_repo() -> str:
    """Detect the current repo from gh."""
    raw = gh("repo", "view", "--json", "nameWithOwner", "-q", ".nameWithOwner")
    return raw


def get_merged_prs(repo: str, limit: int) -> list[dict]:
    """Get the last N merged PRs."""
    raw = gh(
        "pr", "list",
        "--repo", repo,
        "--state", "merged",
        "--limit", str(limit),
        "--json", "number,title,mergedAt",
    )
    return json.loads(raw) if raw else []


def get_reviews(repo: str, pr_number: int) -> list[dict]:
    """Get all reviews for a PR."""
    return gh_api(f"repos/{repo}/pulls/{pr_number}/reviews")


def get_commits(repo: str, pr_number: int) -> list[dict]:
    """Get all commits for a PR, in chronological order."""
    return gh_api(f"repos/{repo}/pulls/{pr_number}/commits")


def get_compare(repo: str, base: str, head: str) -> dict:
    """Get the comparison between two commits."""
    return gh_api(f"repos/{repo}/compare/{base}...{head}")


def find_last_approval(reviews: list[dict]) -> dict | None:
    """Find the last APPROVED review, or None."""
    approvals = [r for r in reviews if r.get("state") == "APPROVED"]
    return approvals[-1] if approvals else None


def find_post_approval_commits(
    commits: list[dict], approved_commit_sha: str
) -> list[dict]:
    """Return commits that come after the approved commit in the PR's commit list."""
    shas = [c["sha"] for c in commits]
    try:
        idx = shas.index(approved_commit_sha)
    except ValueError:
        return []  # approved commit not in list (shouldn't happen)
    return commits[idx + 1:]


def print_pr_summary(repo: str, pr: dict) -> None:
    """Print the post-approval summary for a single PR."""
    pr_num = pr["number"]
    pr_title = pr["title"]
    merged_date = pr["mergedAt"][:10]  # just the date portion

    pr_header = f'PR #{pr_num} - "{pr_title}" {DIM}(merged {merged_date}){RESET}'

    # Find last approval
    reviews = get_reviews(repo, pr_num)
    approval = find_last_approval(reviews)

    if approval is None:
        print(f"{YELLOW}?{RESET} {pr_header} {DIM}- no approval reviews found{RESET}")
        return

    approved_sha = approval["commit_id"]
    approver = approval["user"]["login"]
    approval_time = approval["submitted_at"]

    # Get commits and find post-approval ones
    commits = get_commits(repo, pr_num)
    if not commits:
        print(f"{YELLOW}?{RESET} {pr_header} {DIM}- could not fetch commits{RESET}")
        return

    post_approval = find_post_approval_commits(commits, approved_sha)

    if not post_approval:
        print(f"{GREEN}✓{RESET} {pr_header}")
        return

    # Has post-approval changes — full output
    print(f"{RED}✗{RESET} {pr_header}")
    print(f"  Last approval: {CYAN}@{approver}{RESET} at {approval_time} {DIM}(commit {approved_sha[:7]}){RESET}")
    print(f"  {BOLD}{len(post_approval)} commit(s) after approval:{RESET}")
    for commit in post_approval:
        short_sha = commit["sha"][:7]
        msg = commit["commit"]["message"].split("\n")[0]
        print(f'    {YELLOW}{short_sha}{RESET} - {msg}')

    # Diffstat: compare first post-approval commit's parent to last post-approval commit
    first_sha = post_approval[0]["sha"]
    last_sha = post_approval[-1]["sha"]
    try:
        comparison = get_compare(repo, f"{first_sha}^", last_sha)
        files = comparison.get("files", [])
        num_files = len(files)
        additions = sum(f.get("additions", 0) for f in files)
        deletions = sum(f.get("deletions", 0) for f in files)
        print(f"  Diffstat: {num_files} file(s) changed, {GREEN}+{additions}{RESET} / {RED}-{deletions}{RESET}")
    except RuntimeError:
        print(f"  Diffstat: {DIM}(unavailable){RESET}")

    print()


def main():
    parser = argparse.ArgumentParser(
        description="Summarize post-approval changes in merged PRs."
    )
    parser.add_argument(
        "-n", type=int, default=10,
        help="Number of merged PRs to check (default: 10)",
    )
    parser.add_argument(
        "--repo", type=str, default=None,
        help="Repository as owner/repo (default: current repo)",
    )
    args = parser.parse_args()

    repo = args.repo or get_repo()

    print(f"Checking last {args.n} merged PRs in {repo}")
    print("=" * 50)
    print()

    prs = get_merged_prs(repo, args.n)
    if not prs:
        print("No merged PRs found.")
        sys.exit(0)

    for pr in prs:
        print_pr_summary(repo, pr)


if __name__ == "__main__":
    main()
