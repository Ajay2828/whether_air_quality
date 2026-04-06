"""Diff filter — strips noisy / irrelevant files from PR diffs.

Free-tier LLMs have small context windows (4–8k tokens).
This module ensures we only send meaningful source code diffs to the LLM.
"""

from src.interfaces.git_provider import PRFile

# File patterns to always skip (case-insensitive suffix matching)
SKIP_EXTENSIONS = frozenset({
    ".lock",
    ".svg",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",
    ".eot",
    ".mp4",
    ".webm",
    ".webp",
    ".pdf",
    ".zip",
    ".tar",
    ".gz",
    ".min.js",
    ".min.css",
    ".map",
})

# Exact filenames to always skip
SKIP_FILENAMES = frozenset({
    "package-lock.json",
    "yarn.lock",
    "pnpm-lock.yaml",
    "poetry.lock",
    "Pipfile.lock",
    "composer.lock",
    "Gemfile.lock",
    "go.sum",
    ".DS_Store",
})

# Maximum characters per file patch (to stay within context limits)
MAX_PATCH_CHARS = 8_000

# Maximum total characters across all patches
MAX_TOTAL_CHARS = 30_000


def should_skip(filename: str) -> bool:
    """Return True if a file should be excluded from the review."""
    basename = filename.rsplit("/", 1)[-1].lower()

    if basename in SKIP_FILENAMES:
        return True

    for ext in SKIP_EXTENSIONS:
        if basename.endswith(ext):
            return True

    return False


def filter_diff(files: list[PRFile]) -> list[PRFile]:
    """Filter and truncate PR files to fit within context limits.

    Returns a new list of PRFile objects with:
    1. Noisy files removed
    2. Oversized patches truncated
    3. Total size capped
    """
    filtered: list[PRFile] = []
    total_chars = 0

    for f in files:
        if should_skip(f.filename):
            continue

        if not f.patch:
            continue

        patch = f.patch
        if len(patch) > MAX_PATCH_CHARS:
            patch = patch[:MAX_PATCH_CHARS] + "\n... [truncated — file too large]"

        if total_chars + len(patch) > MAX_TOTAL_CHARS:
            # Add a note that we stopped including files
            filtered.append(
                PRFile(
                    filename="[TRUNCATED]",
                    patch="... remaining files omitted to fit context window",
                    status="truncated",
                    additions=0,
                    deletions=0,
                )
            )
            break

        total_chars += len(patch)
        filtered.append(
            PRFile(
                filename=f.filename,
                patch=patch,
                status=f.status,
                additions=f.additions,
                deletions=f.deletions,
            )
        )

    return filtered
