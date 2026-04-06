"""Gemini adapter — stub implementation of LLMProvider.

This adapter is a skeleton for Phase 3. When you switch to Gemini,
replace the body with real google-genai SDK calls. Gemini's massive
context window (up to 2M tokens) lets you send full file contents
alongside the diff for much deeper reviews.
"""

from src.interfaces.llm_provider import LLMProvider, ReviewComment


class GeminiAdapter(LLMProvider):
    """LLM adapter for Google Gemini.

    Requires: pip install google-genai
    """

    def __init__(self, api_key: str, model: str = "gemini-2.0-flash"):
        self._api_key = api_key
        self._model = model
        # Uncomment when ready:
        # from google import genai
        # self._client = genai.Client(api_key=api_key)

    def generate_review(
        self, system_prompt: str, user_prompt: str
    ) -> list[ReviewComment]:
        raise NotImplementedError(
            "GeminiAdapter.generate_review() not yet implemented. "
            "Use google-genai SDK with structured JSON output: "
            "client.models.generate_content(model=..., contents=..., "
            "config=GenerateContentConfig(response_mime_type='application/json'))"
        )
