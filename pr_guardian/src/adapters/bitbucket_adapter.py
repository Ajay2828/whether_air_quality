"""Bitbucket adapter — stub implementation of GitProvider.

This adapter is a skeleton for Phase 3. Replace the NotImplementedError
bodies with real atlassian-python-api calls when you migrate to Bitbucket.
"""

from src.interfaces.git_provider import GitProvider, PRFile


class BitbucketAdapter(GitProvider):
    """Concrete GitProvider for Bitbucket Cloud / Server.

    Requires: pip install atlassian-python-api
    Docs: https://atlassian-python-api.readthedocs.io
    """

    def __init__(
        self,
        url: str,
        username: str,
        app_password: str,
        workspace: str,
        repo_slug: str,
    ):
        self._url = url
        self._username = username
        self._app_password = app_password
        self._workspace = workspace
        self._repo_slug = repo_slug
        # Uncomment when ready:
        # from atlassian import Bitbucket
        # self._client = Bitbucket(
        #     url=url, username=username, password=app_password
        # )

    def get_pr_diff(self, pr_id: int) -> list[PRFile]:
        raise NotImplementedError(
            "BitbucketAdapter.get_pr_diff() not yet implemented. "
            "Use atlassian-python-api to fetch the PR diff from "
            f"{self._url}/rest/api/latest/projects/.../pull-requests/{pr_id}/diff"
        )

    def post_comment(self, pr_id: int, comment: str) -> None:
        raise NotImplementedError(
            "BitbucketAdapter.post_comment() not yet implemented."
        )

    def post_inline_comment(
        self, pr_id: int, file: str, line: int, comment: str
    ) -> None:
        raise NotImplementedError(
            "BitbucketAdapter.post_inline_comment() not yet implemented."
        )
