"""Abstract base class for LLM providers.

Any LLM backend (OpenRouter, Gemini, local Ollama, etc.) must implement
this interface. The core ReviewAgent only depends on this abstraction.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class ReviewComment:
    """A single review finding produced by the LLM."""
    file: str
    line: int           # 0 means "general / file-level comment"
    severity: str       # info, warning, error
    comment: str


class LLMProvider(ABC):
    """Interface every LLM adapter must fulfil."""

    @abstractmethod
    def generate_review(
        self, system_prompt: str, user_prompt: str
    ) -> list[ReviewComment]:
        """Send the prompts to the LLM and return structured review comments."""
        ...
