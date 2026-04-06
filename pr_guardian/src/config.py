"""Configuration — adapter factory based on environment variables.

Reads .env and returns the correct GitProvider + LLMProvider instances
based on GIT_PROVIDER and LLM_PROVIDER settings.
"""

import os
import logging
from dotenv import load_dotenv

from src.interfaces.git_provider import GitProvider
from src.interfaces.llm_provider import LLMProvider

logger = logging.getLogger(__name__)

# Load .env file from project root
load_dotenv()


def get_git_provider() -> GitProvider:
    """Instantiate the correct Git adapter from env config."""
    provider = os.getenv("GIT_PROVIDER", "github").lower()

    if provider == "github":
        from src.adapters.github_adapter import GitHubAdapter

        token = os.environ["GITHUB_TOKEN"]
        repo = os.environ["GITHUB_REPO"]
        logger.info("Using GitHubAdapter for repo: %s", repo)
        return GitHubAdapter(token=token, repo_name=repo)

    elif provider == "bitbucket":
        from src.adapters.bitbucket_adapter import BitbucketAdapter

        logger.info("Using BitbucketAdapter")
        return BitbucketAdapter(
            url=os.environ["BITBUCKET_URL"],
            username=os.environ["BITBUCKET_USERNAME"],
            app_password=os.environ["BITBUCKET_APP_PASSWORD"],
            workspace=os.environ["BITBUCKET_WORKSPACE"],
            repo_slug=os.environ["BITBUCKET_REPO_SLUG"],
        )

    else:
        raise ValueError(
            f"Unknown GIT_PROVIDER: '{provider}'. "
            "Supported values: github, bitbucket"
        )


def get_llm_provider() -> LLMProvider:
    """Instantiate the correct LLM adapter from env config."""
    provider = os.getenv("LLM_PROVIDER", "openrouter").lower()

    if provider == "openrouter":
        from src.adapters.openrouter_adapter import OpenRouterAdapter

        api_key = os.environ["OPENROUTER_API_KEY"]
        model = os.getenv("OPENROUTER_MODEL")
        logger.info("Using OpenRouterAdapter with model: %s", model or "default")
        return OpenRouterAdapter(api_key=api_key, model=model)

    elif provider == "gemini":
        from src.adapters.gemini_adapter import GeminiAdapter

        api_key = os.environ["GEMINI_API_KEY"]
        model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
        logger.info("Using GeminiAdapter with model: %s", model)
        return GeminiAdapter(api_key=api_key, model=model)

    else:
        raise ValueError(
            f"Unknown LLM_PROVIDER: '{provider}'. "
            "Supported values: openrouter, gemini"
        )
