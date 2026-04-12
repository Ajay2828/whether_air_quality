#!/usr/bin/env python3
"""CLI entry point for the AI Code Review Agent.

Production-hardened with:
- Top-level error handling (posts failure notice to PR, never blocks merge)
- Graceful exit on all error paths

Usage:
    python main.py --repo owner/repo --pr 123

Or with env vars already set:
    python main.py --pr 123
"""

import argparse
import logging
import os
import sys


def main():
    parser = argparse.ArgumentParser(
        description="AI Code Review Agent — review a pull request using an LLM"
    )
    parser.add_argument(
        "--pr", type=int, required=True,
        help="Pull request number to review"
    )
    parser.add_argument(
        "--repo", type=str, default=None,
        help="Repository name (owner/repo). Overrides GITHUB_REPO env var."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print review comments instead of posting them to the PR"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable debug logging"
    )
    args = parser.parse_args()

    # ── logging ──────────────────────────────────────────────
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("code-review-agent")

    # ── override repo if provided via CLI ────────────────────
    if args.repo:
        os.environ["GITHUB_REPO"] = args.repo

    # ── build providers ──────────────────────────────────────
    from src.config import get_git_provider, get_llm_provider
    from src.core.review_agent import ReviewAgent

    git = None
    try:
        git = get_git_provider()
        llm = get_llm_provider()
    except (KeyError, ValueError) as e:
        logger.error("Configuration error: %s", e)
        logger.error("Copy .env.example to .env and fill in your keys.")
        sys.exit(1)

    # ── run review ───────────────────────────────────────────
    agent = ReviewAgent(git=git, llm=llm)

    try:
        if args.dry_run:
            logger.info("DRY RUN — comments will be printed, not posted")
            comments = _dry_run_review(agent, git, llm, args.pr)
        else:
            comments = agent.review_pr(args.pr)

        # ── summary ──────────────────────────────────────────
        logger.info("Review complete — %d comments generated", len(comments))
        for c in comments:
            severity = c.severity.upper()
            loc = f"{c.file}:{c.line}" if c.file and c.line else "(general)"
            logger.info("  [%s] %s — %s", severity, loc, c.comment[:120])

    except Exception as e:
        logger.error("PR Guardian failed: %s", e, exc_info=True)

        # Post a failure notice to the PR so the team knows
        if git and not args.dry_run:
            try:
                git.post_comment(
                    args.pr,
                    "## 🤖 AI Code Review\n\n"
                    "⚠️ **PR Guardian encountered an error** and could not "
                    "complete the review.\n\n"
                    f"```\n{type(e).__name__}: {e}\n```\n\n"
                    "This does not block your PR. A team member can "
                    "re-run the review from the Actions tab.",
                )
            except Exception as post_err:
                logger.error(
                    "Failed to post error notice to PR: %s", post_err
                )

        # Exit 0 so the GitHub Action does NOT block the PR merge
        sys.exit(0)


def _dry_run_review(agent, git, llm, pr_id):
    """Run the LLM review but print results instead of posting."""
    from src.core.diff_filter import filter_diff
    from src.core.prompts import SYSTEM_PROMPT, build_user_prompt

    raw_files = git.get_pr_diff(pr_id)
    filtered = filter_diff(raw_files)

    if not filtered:
        print("No reviewable files in this PR.")
        return []

    diff_text = agent._build_combined_diff(filtered)
    user_prompt = build_user_prompt(diff_text)
    comments = llm.generate_review(SYSTEM_PROMPT, user_prompt)

    print(f"\n{'='*60}")
    print(f"  AI Code Review — PR #{pr_id}  ({len(comments)} findings)")
    print(f"{'='*60}\n")

    for i, c in enumerate(comments, 1):
        loc = f"{c.file}:{c.line}" if c.file and c.line else "General"
        print(f"  {i}. [{c.severity.upper()}] {loc}")
        print(f"     {c.comment}")
        print()

    return comments


if __name__ == "__main__":
    main()
