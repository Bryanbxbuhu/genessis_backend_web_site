"""Validate JSON completions before their contents can enter a report."""

import json
from typing import Any


def parse_json_completion(response: Any) -> dict:
    """Reject incomplete, refused, empty, and non-object model responses."""
    if not response.choices:
        raise RuntimeError("OpenAI returned no completion choices.")
    choice = response.choices[0]
    if choice.finish_reason != "stop":
        raise RuntimeError(f"OpenAI completion did not finish: {choice.finish_reason}.")
    message = choice.message
    if getattr(message, "refusal", None):
        raise RuntimeError("OpenAI refused to generate the requested JSON.")
    if not isinstance(message.content, str) or not message.content.strip():
        raise RuntimeError("OpenAI returned empty JSON content.")
    try:
        result = json.loads(message.content)
    except json.JSONDecodeError as exc:
        raise RuntimeError("OpenAI returned invalid JSON content.") from exc
    if not isinstance(result, dict):
        raise RuntimeError("OpenAI returned JSON that is not an object.")
    return result
