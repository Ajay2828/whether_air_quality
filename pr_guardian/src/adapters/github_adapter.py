"""GitHub adapter — implements GitProvider using PyGithub."""

from github import Github
from github.PullRequest import PullRequest

from src.interfaces.git_provider import GitProvider, PRFile


class GitHubAdapter(GitProvider):
    """Concrete GitProvider for GitHub repositories."""

    def __init__(self, token: str, repo_name: str):
        """
        Args:
            token: GitHub personal access token or GitHub App token.
            repo_name: Full repository name, e.g. "owner/repo".
        """
        self._client = Github(token)
        self._repo = self._client.get_repo(repo_name)

    # ── helpers ──────────────────────────────────────────────

    def _get_pr(self, pr_id: int) -> PullRequest:
        return self._repo.get_pull(pr_id)

    # ── interface implementation ─────────────────────────────

    def get_pr_diff(self, pr_id: int) -> list[PRFile]:
        pr = self._get_pr(pr_id)
        files: list[PRFile] = []
        for f in pr.get_files():
            files.append(
                PRFile(
                    filename=f.filename,
                    patch=f.patch or "",
                    status=f.status,
                    additions=f.additions,
                    deletions=f.deletions,
                )
            )
        return files

    def post_comment(self, pr_id: int, comment: str) -> None:
        pr = self._get_pr(pr_id)
        pr.as_issue().create_comment(comment)

    def post_inline_comment(
        self, pr_id: int, file: str, line: int, comment: str
    ) -> None:
        pr = self._get_pr(pr_id)
        # To post an inline comment we need the latest commit SHA
        commit = pr.get_commits().reversed[0]
        pr.create_review_comment(
            body=comment,
            commit=commit,
            path=file,
            line=line,
        )

    def get_file_content(self, path: str, ref: str = "main") -> str | None:
        """Fetch a file's content from the repo.

        Uses the repo's default branch to get the current version
        of the referenced file.
        """
        try:
            content = self._repo.get_contents(path, ref=ref)
            if hasattr(content, "decoded_content"):
                return content.decoded_content.decode("utf-8", errors="replace")
            return None
        except Exception:
            return None

    def has_existing_review(self, pr_id: int) -> bool:
        """Check if the PR already has an AI review comment.

        Prevents duplicate reviews when a PR is updated rapidly.
        """
        pr = self._get_pr(pr_id)
        for comment in pr.as_issue().get_comments():
            if "🤖 AI Code Review" in (comment.body or ""):
                return True
        return False
