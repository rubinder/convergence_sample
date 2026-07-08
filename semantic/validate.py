"""Input validation for the reach semantic layer.

Athena has no true bind parameters, so the primary defense against SQL injection
is strict validation at the point where values enter a query. Campaign/segment
identifiers are allow-listed by character class; dates are parsed and canonically
re-serialized. Anything else is rejected before a query is ever built.
"""

import re
from datetime import date

_NAME = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


class InvalidInput(ValueError):
    """Raised when a caller-supplied value fails validation."""


def valid_name(value: str, field: str) -> str:
    if not isinstance(value, str) or not _NAME.match(value):
        raise InvalidInput(f"invalid {field}")
    return value


def valid_date(value: str, field: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except (ValueError, TypeError):
        raise InvalidInput(f"invalid {field}")


def valid_segments(values, field: str = "segments") -> list:
    if not isinstance(values, (list, tuple)) or not values:
        raise InvalidInput(f"invalid {field}")
    return [valid_name(v, "segment") for v in values]
