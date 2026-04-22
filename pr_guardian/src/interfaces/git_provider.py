"""Abstract base class for Git platform providers.

Any Git platform (GitHub, Bitbucket, GitLab, etc.) must implement this
interface. The core ReviewAgent only depends on this abstraction — never
on a concrete platform SDK.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class PRFile:
    """Represents a single file changed in a pull request."""
    filename: str
    patch: str          # unified diff text for this file
    status: str         # added, modified, removed, renamed
    additions: int
    deletions: int


class GitProvider(ABC):
    """Interface every Git-platform adapter must fulfil."""

    @abstractmethod
    def get_pr_diff(self, pr_id: int) -> list[PRFile]:
        """Return the list of changed files with their diffs."""
        ...

    @abstractmethod
    def post_comment(self, pr_id: int, comment: str) -> None:
        """Post a general (top-level) comment on the PR."""
        ...

    @abstractmethod
    def post_inline_comment(
        self, pr_id: int, file: str, line: int, comment: str
    ) -> None:
        """Post a comment on a specific line of a specific file."""
        ...

    @abstractmethod
    def get_file_content(self, path: str, ref: str = "main") -> str | None:
        """Fetch the content of a file from the repository.

        Args:
            path: Repo-relative file path (e.g., 'src/utils.py')
            ref: Branch or commit ref to read from (default: 'main')

        Returns:
            File content as a string, or None if the file doesn't exist.
        """
        ...
