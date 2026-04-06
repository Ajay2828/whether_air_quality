"""OpenRouter adapter — implements LLMProvider via the OpenAI-compatible API."""

import json
import logging

from openai import OpenAI

from src.interfaces.llm_provider import LLMProvider, ReviewComment

logger = logging.getLogger(__name__)


class OpenRouterAdapter(LLMProvider):
    """LLM adapter that calls OpenRouter (OpenAI-compatible endpoint)."""

    DEFAULT_MODEL = "openrouter/auto"

    def __init__(self, api_key: str, model: str | None = None):
        self._model = model or self.DEFAULT_MODEL
        self._client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key,
        )

    def generate_review(
        self, system_prompt: str, user_prompt: str
    ) -> list[ReviewComment]:
        """Call OpenRouter and parse the response into ReviewComments."""

        response = self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
            max_tokens=4096,
        )

        raw_text = response.choices[0].message.content or ""
        return self._parse_response(raw_text)

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
