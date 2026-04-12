"""OpenRouter adapter — implements LLMProvider via the OpenAI-compatible API.

Production-hardened with:
- Retry logic (3 attempts with exponential backoff)
- HTTP timeout (120s per request)
- Token usage tracking
"""

import json
import logging
import time

from openai import OpenAI, APIError, RateLimitError, APIConnectionError

from src.interfaces.llm_provider import LLMProvider, ReviewComment

logger = logging.getLogger(__name__)


class LLMError(Exception):
    """Raised when the LLM call fails after all retries."""
    pass


class OpenRouterAdapter(LLMProvider):
    """LLM adapter that calls OpenRouter (OpenAI-compatible endpoint)."""

    DEFAULT_MODEL = "openrouter/auto"
    MAX_RETRIES = 3
    BASE_BACKOFF_SECONDS = 2  # 2s → 4s → 8s

    def __init__(self, api_key: str, model: str | None = None):
        self._model = model or self.DEFAULT_MODEL
        self._client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key,
            timeout=120.0,  # 2-minute HTTP timeout
        )
        # Cumulative token usage for this session
        self.total_prompt_tokens = 0
        self.total_completion_tokens = 0
        self.total_tokens = 0

    def generate_review(
        self, system_prompt: str, user_prompt: str
    ) -> list[ReviewComment]:
        """Call OpenRouter with retry logic and parse the response."""

        response = self._call_with_retries(system_prompt, user_prompt)

        # ── track token usage ────────────────────────────────
        if response.usage:
            prompt_tok = response.usage.prompt_tokens or 0
            completion_tok = response.usage.completion_tokens or 0
            total_tok = response.usage.total_tokens or 0
            self.total_prompt_tokens += prompt_tok
            self.total_completion_tokens += completion_tok
            self.total_tokens += total_tok
            logger.info(
                "  → Token usage: %d prompt + %d completion = %d total "
                "(session cumulative: %d)",
                prompt_tok, completion_tok, total_tok, self.total_tokens,
            )

        raw_text = response.choices[0].message.content or ""
        return self._parse_response(raw_text)

    # ── retry logic ──────────────────────────────────────────

    def _call_with_retries(self, system_prompt: str, user_prompt: str):
        """Call the LLM with exponential backoff retries."""
        last_error = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                response = self._client.chat.completions.create(
                    model=self._model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=0.2,
                    max_tokens=4096,
                )
                return response

            except (APIError, RateLimitError, APIConnectionError) as e:
                last_error = e
                if attempt < self.MAX_RETRIES:
                    wait = self.BASE_BACKOFF_SECONDS ** attempt
                    logger.warning(
                        "  ⚠ LLM call failed (attempt %d/%d): %s — "
                        "retrying in %ds ...",
                        attempt, self.MAX_RETRIES, e, wait,
                    )
                    time.sleep(wait)
                else:
                    logger.error(
                        "  ✗ LLM call failed after %d attempts: %s",
                        self.MAX_RETRIES, e,
                    )

        raise LLMError(
            f"LLM call failed after {self.MAX_RETRIES} attempts. "
            f"Last error: {last_error}"
        )

    # ── response parsing ─────────────────────────────────────

    @staticmethod
    def _parse_response(raw_text: str) -> list[ReviewComment]:
        """Try to extract structured JSON from the LLM response.

        The LLM is prompted to return JSON, but free-tier models sometimes
        wrap it in markdown code fences or add commentary. This method
        handles both clean JSON and wrapped text gracefully.
        """
        comments: list[ReviewComment] = []

        # Strip markdown code fences if present
        text = raw_text.strip()
        if text.startswith("```"):
            # Remove opening fence (```json or ```)
            first_newline = text.index("\n")
            text = text[first_newline + 1:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

        # Attempt JSON parse
        try:
            data = json.loads(text)
            if isinstance(data, dict) and "comments" in data:
                data = data["comments"]
            if isinstance(data, list):
                for item in data:
                    comments.append(
                        ReviewComment(
                            file=item.get("file", ""),
                            line=int(item.get("line", 0)),
                            severity=item.get("severity", "info"),
                            comment=item.get("comment", ""),
                        )
                    )
                return comments
        except (json.JSONDecodeError, ValueError, TypeError):
            pass

        # Fallback: treat the entire response as a single general comment
        if text:
            logger.warning(
                "LLM response was not valid JSON — returning as a single comment."
            )
            comments.append(
                ReviewComment(
                    file="",
                    line=0,
                    severity="info",
                    comment=raw_text,
                )
            )

        return comments
