"""Context resolver — analyzes imports in changed files to find related code.

This module parses Python import statements from PR diffs and maps them
to repo file paths. The resolved files are fetched and sent as context
to the LLM so it can catch cross-file bugs (e.g., wrong function arguments,
missing parameters, broken interfaces).
"""

import re
import logging

logger = logging.getLogger(__name__)

# Maximum total characters for context files (budget alongside 30K diff)
MAX_CONTEXT_CHARS = 15_000

# Known stdlib / third-party modules to skip (not worth fetching from repo)
STDLIB_AND_COMMON = frozenset({
    # Python stdlib (most common)
    "os", "sys", "re", "json", "csv", "math", "time", "datetime",
    "pathlib", "typing", "dataclasses", "abc", "collections",
    "functools", "itertools", "logging", "argparse", "hashlib",
    "base64", "copy", "io", "shutil", "tempfile", "threading",
    "multiprocessing", "subprocess", "unittest", "contextlib",
    "traceback", "inspect", "importlib", "pickle", "socket",
    "http", "urllib", "email", "html", "xml", "sqlite3",
    "asyncio", "concurrent", "enum", "string", "textwrap",
    "struct", "decimal", "fractions", "random", "statistics",
    "secrets", "uuid", "pprint", "warnings", "signal",
    "glob", "fnmatch", "configparser", "codecs",
    # Common third-party
    "flask", "django", "fastapi", "uvicorn", "starlette",
    "requests", "httpx", "aiohttp",
    "sqlalchemy", "psycopg2", "pymysql", "mysql",
    "pandas", "numpy", "scipy", "matplotlib", "seaborn",
    "pytest", "mock", "unittest",
    "celery", "redis", "boto3", "botocore",
    "pydantic", "marshmallow",
    "openai", "github", "dotenv",
    "yaml", "toml",
    "PIL", "cv2", "sklearn", "tensorflow", "torch",
    "airflow",
})


def extract_imports(patch_text: str) -> list[str]:
    """Extract Python import paths from a diff patch.

    Looks at added lines (starting with +) for import statements.
    Returns the module paths (e.g., ['src.utils', 'src.core.helpers']).
    """
    imports: list[str] = []

    for line in patch_text.split("\n"):
        # Only look at the full file content or added lines
        stripped = line.lstrip("+").strip()

        # Match: from src.utils import something
        m = re.match(r"^from\s+([\w.]+)\s+import", stripped)
        if m:
            imports.append(m.group(1))
            continue

        # Match: import src.utils
        m = re.match(r"^import\s+([\w.]+)", stripped)
        if m:
            imports.append(m.group(1))

    return imports


def module_to_filepath(module_path: str) -> str | None:
    """Convert a Python module path to a repo-relative file path.

    Examples:
        'src.utils'          → 'src/utils.py'
        'src.core.helpers'   → 'src/core/helpers.py'
        'os'                 → None (stdlib)
        'pandas'             → None (third-party)
    """
    root = module_path.split(".")[0]

    if root in STDLIB_AND_COMMON:
        return None

    # Convert dot notation to path
    return module_path.replace(".", "/") + ".py"


def resolve_context_files(
    changed_files: list,
    changed_filenames: set[str],
) -> list[str]:
    """Analyze imports in changed files and return paths of referenced files.

    Args:
        changed_files: List of PRFile objects with patch text
        changed_filenames: Set of filenames already in the diff

    Returns:
        Deduplicated list of repo-relative file paths to fetch as context
    """
    context_paths: list[str] = []
    seen: set[str] = set()

    for f in changed_files:
        if not f.patch:
            continue

        # Only analyze Python files for imports
        if not f.filename.endswith(".py"):
            continue

        imports = extract_imports(f.patch)

        for module_path in imports:
            filepath = module_to_filepath(module_path)

            if filepath is None:
                continue  # stdlib or third-party

            if filepath in seen:
                continue  # already added

            if filepath in changed_filenames:
                continue  # already in the diff — LLM sees it

            seen.add(filepath)
            context_paths.append(filepath)

    logger.info(
        "  → resolved %d context files from imports: %s",
        len(context_paths), context_paths,
    )
    return context_paths


def build_context_text(
    file_contents: dict[str, str],
) -> str:
    """Build the context text block from fetched file contents.

    Respects MAX_CONTEXT_CHARS budget. Each file is clearly labeled
    so the LLM knows it's reference material, not part of the diff.
    """
    parts: list[str] = []
    total_chars = 0

    for filepath, content in file_contents.items():
        header = f"── REFERENCED FILE: {filepath} (for context, NOT part of the diff) ──"
        section = f"{header}\n{content}\n"

        if total_chars + len(section) > MAX_CONTEXT_CHARS:
            # Truncate this file to fit
            remaining = MAX_CONTEXT_CHARS - total_chars - len(header) - 50
            if remaining > 200:  # only include if meaningful amount fits
                section = (
                    f"{header}\n"
                    f"{content[:remaining]}\n"
                    "... [truncated — file too large for context]\n"
                )
                parts.append(section)
            break

        total_chars += len(section)
        parts.append(section)

    return "\n".join(parts)
