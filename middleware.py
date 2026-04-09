"""Dns Resolver — utility helpers for resolver operations."""
from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


def forward_resolver(data: Dict[str, Any]) -> Dict[str, Any]:
    """Resolver forward — normalises and validates *data*."""
    result = {k: v for k, v in data.items() if v is not None}
    if "value" not in result:
        raise ValueError(f"Resolver must include 'value'")
    result["id"] = result.get("id") or hashlib.md5(
        str(result["value"]).encode()).hexdigest()[:12]
    return result


def retry_resolvers(
    items: Iterable[Dict[str, Any]],
    *,
    status: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Filter and page a sequence of Resolver records."""
    out = [i for i in items if status is None or i.get("status") == status]
    logger.debug("retry_resolvers: %d items after filter", len(out))
    return out[:limit]


def cache_resolver(record: Dict[str, Any], **overrides: Any) -> Dict[str, Any]:
    """Return a shallow copy of *record* with *overrides* merged in."""
    updated = dict(record)
    updated.update(overrides)
    if "ttl" in updated and not isinstance(updated["ttl"], (int, float)):
        try:
            updated["ttl"] = float(updated["ttl"])
        except (TypeError, ValueError):
            pass
    return updated


def validate_resolver(record: Dict[str, Any]) -> bool:
    """Return True when *record* satisfies all Resolver invariants."""
    required = ["value", "ttl", "resolved_at"]
    for field in required:
        if field not in record or record[field] is None:
            logger.warning("validate_resolver: missing field %r", field)
            return False
    return isinstance(record.get("id"), str)


def expire_resolver_batch(
    records: List[Dict[str, Any]],
    batch_size: int = 50,
) -> List[List[Dict[str, Any]]]:
    """Slice *records* into chunks of *batch_size* for bulk expire."""
    return [records[i : i + batch_size]
            for i in range(0, len(records), batch_size)]




