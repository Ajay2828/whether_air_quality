"""ReviewAgent — the central orchestrator.

This module knows NOTHING about GitHub, Bitbucket, OpenRouter, or Gemini.
It only talks to the abstract GitProvider and LLMProvider interfaces.
"""

import logging

from src.interfaces.git_provider import GitProvider
from src.interfaces.llm_provider import LLMProvider, ReviewComment
from src.core.diff_filter import filter_diff
from src.core.prompts import SYSTEM_PROMPT, build_user_prompt

logger = logging.getLogger(__name__)


class ReviewAgent:
    """Orchestrates the full code-review pipeline."""

    def __init__(self, git: GitProvider, llm: LLMProvider):
        self._git = git
        self._llm = llm

    def review_pr(self, pr_id: int) -> list[ReviewComment]:
        """Run a full review on a pull request.

        Steps:
            1. Fetch the PR diff from the Git provider
            2. Filter out noisy / irrelevant files
            3. Build the prompt and call the LLM
            4. Post the review comments back to the PR
            5. Return the comments for logging / testing
        """
        # ── 1. fetch ─────────────────────────────────────────
        logger.info("Fetching diff for PR #%d ...", pr_id)
        raw_files = self._git.get_pr_diff(pr_id)
        logger.info("  → %d files changed", len(raw_files))

        # ── 2. filter ────────────────────────────────────────
        filtered_files = filter_diff(raw_files)
        logger.info("  → %d files after filtering", len(filtered_files))

        if not filtered_files:
            logger.info("  → nothing to review (all files filtered out)")
            return []

        # ── 3. build prompt & call LLM ───────────────────────
        diff_text = self._build_combined_diff(filtered_files)
        user_prompt = build_user_prompt(diff_text)
        logger.info("  → sending %d chars to LLM ...", len(user_prompt))

        comments = self._llm.generate_review(SYSTEM_PROMPT, user_prompt)
        logger.info("  → LLM returned %d comments", len(comments))

        # ── 4. post comments ─────────────────────────────────
        self._post_comments(pr_id, comments)

        return comments

    # ── helpers ───────────────────────────────────────────────

    @staticmethod
    def _build_combined_diff(files) -> str:
        """Merge per-file diffs into a single text block."""
        parts: list[str] = []
        for f in files:
            parts.append(f"── {f.filename} ({f.status}) ──")
            parts.append(f.patch)
            parts.append("")  # blank line separator
        return "\n".join(parts)

    def _post_comments(
        self, pr_id: int, comments: list[ReviewComment]
    ) -> None:
        """Post review comments back to the PR.

        - Inline comments (file + line > 0) are posted as line-level comments
        - General comments are aggregated into a single top-level comment
        """
        general_parts: list[str] = []
        inline_errors = 0

        for c in comments:
            if c.file and c.line > 0:
                try:
                    self._git.post_inline_comment(
                        pr_id, c.file, c.line,
                        f"**[{c.severity.upper()}]** {c.comment}"
                    )
                except Exception as e:
                    # Inline comments can fail if the line is not part of the
                    # diff hunk — fall back to including it in the summary
                    logger.warning(
                        "Failed to post inline comment on %s:%d — %s",
                        c.file, c.line, e,
                    )
                    inline_errors += 1
                    general_parts.append(
                        f"- **[{c.severity.upper()}]** `{c.file}` L{c.line}: {c.comment}"
                    )
            else:
                general_parts.append(
                    f"- **[{c.severity.upper()}]** {c.comment}"
                )

        # Post the summary comment if there's anything to say
        if general_parts:
            header = "## 🤖 AI Code Review\n\n"
            body = header + "\n".join(general_parts)
            self._git.post_comment(pr_id, body)

        if not comments:
            self._git.post_comment(
                pr_id,
                "## 🤖 AI Code Review\n\n✅ No issues found — looks good!",
            )

        logger.info(
            "  → posted %d inline + %d summary comments (%d inline failures)",
            len(comments) - len(general_parts) + inline_errors,
            1 if general_parts or not comments else 0,
            inline_errors,
        )
