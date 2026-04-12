"""ReviewAgent — the central orchestrator.

Production-hardened with:
- Duplicate review prevention (skips if PR already has a review)
- PR size guard (warns on 50+ file PRs)
- Graceful inline-comment fallback
"""

import logging

from src.interfaces.git_provider import GitProvider
from src.interfaces.llm_provider import LLMProvider, ReviewComment
from src.core.diff_filter import filter_diff
from src.core.prompts import SYSTEM_PROMPT, build_user_prompt

logger = logging.getLogger(__name__)

# Maximum number of changed files before we skip the LLM review
MAX_FILES_FOR_REVIEW = 50


class ReviewAgent:
    """Orchestrates the full code-review pipeline."""

    def __init__(self, git: GitProvider, llm: LLMProvider):
        self._git = git
        self._llm = llm

    def review_pr(self, pr_id: int) -> list[ReviewComment]:
        """Run a full review on a pull request.

        Steps:
            1. Check for duplicate reviews
            2. Fetch the PR diff from the Git provider
            3. Guard against oversized PRs
            4. Filter out noisy / irrelevant files
            5. Build the prompt and call the LLM
            6. Post the review comments back to the PR
            7. Return the comments for logging / testing
        """
        # ── 1. duplicate check ───────────────────────────────
        if hasattr(self._git, "has_existing_review"):
            if self._git.has_existing_review(pr_id):
                logger.info(
                    "PR #%d already has an AI review — skipping to avoid "
                    "duplicates. Delete the previous review comment to "
                    "re-trigger.",
                    pr_id,
                )
                return []

        # ── 2. fetch ─────────────────────────────────────────
        logger.info("Fetching diff for PR #%d ...", pr_id)
        raw_files = self._git.get_pr_diff(pr_id)
        logger.info("  → %d files changed", len(raw_files))

        # ── 3. size guard ────────────────────────────────────
        if len(raw_files) > MAX_FILES_FOR_REVIEW:
            logger.warning(
                "  ⚠ PR has %d files — too large for automated review",
                len(raw_files),
            )
            self._git.post_comment(
                pr_id,
                "## 🤖 AI Code Review\n\n"
                f"⚠️ **This PR is too large for automated review** "
                f"({len(raw_files)} files changed).\n\n"
                "Please consider breaking it into smaller, focused PRs "
                "for better review quality. The AI reviewer works best "
                "with PRs under 50 files.",
            )
            return []

        # ── 4. filter ────────────────────────────────────────
        filtered_files = filter_diff(raw_files)
        logger.info("  → %d files after filtering", len(filtered_files))

        if not filtered_files:
            logger.info("  → nothing to review (all files filtered out)")
            return []

        # ── 5. build prompt & call LLM ───────────────────────
        diff_text = self._build_combined_diff(filtered_files)
        user_prompt = build_user_prompt(diff_text)
        logger.info("  → sending %d chars to LLM ...", len(user_prompt))

        comments = self._llm.generate_review(SYSTEM_PROMPT, user_prompt)
        logger.info("  → LLM returned %d comments", len(comments))

        # ── 6. post comments ─────────────────────────────────
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
