"""Shape checks for decoded JSON shared by the protocol adapters."""

from typing import Any


def as_object(value: object) -> dict[str, Any]:
    """Return a JSON object, or an empty dict when the value has another shape."""
    return value if isinstance(value, dict) else {}


def as_string(value: object) -> str | None:
    """Return a JSON string as-is without coercing other value types."""
    return value if isinstance(value, str) else None
