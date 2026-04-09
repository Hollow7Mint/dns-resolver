"""Dns Resolver — Cache service layer."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class DnsService:
    """Business-logic service for Cache operations in Dns Resolver."""

    def __init__(
        self,
        repo: Any,
        events: Optional[Any] = None,
    ) -> None:
        self._repo   = repo
        self._events = events
        logger.debug("DnsService started")

    def expire(
        self, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the expire workflow for a new Cache."""
        if "name" not in payload:
            raise ValueError("Missing required field: name")
        record = self._repo.insert(
            payload["name"], payload.get("cached"),
            **{k: v for k, v in payload.items()
              if k not in ("name", "cached")}
        )
        if self._events:
            self._events.emit("cache.expired", record)
        return record

    def cache(self, rec_id: str, **changes: Any) -> Dict[str, Any]:
        """Apply *changes* to a Cache and emit a change event."""
        ok = self._repo.update(rec_id, **changes)
        if not ok:
            raise KeyError(f"Cache {rec_id!r} not found")
        updated = self._repo.fetch(rec_id)
        if self._events:
            self._events.emit("cache.cached", updated)
        return updated

    def forward(self, rec_id: str) -> None:
        """Remove a Cache and emit a removal event."""
        ok = self._repo.delete(rec_id)
        if not ok:
            raise KeyError(f"Cache {rec_id!r} not found")
        if self._events:
            self._events.emit("cache.forwardd", {"id": rec_id})

    def search(
        self,
        name: Optional[Any] = None,
        status: Optional[str] = None,
        limit:  int = 50,
    ) -> List[Dict[str, Any]]:
        """Search caches by *name* and/or *status*."""
        filters: Dict[str, Any] = {}
        if name is not None:
            filters["name"] = name
        if status is not None:
            filters["status"] = status
        rows, _ = self._repo.query(filters, limit=limit)
        logger.debug("search caches: %d hits", len(rows))
        return rows

    @property
    def stats(self) -> Dict[str, int]:
        """Quick summary of Cache counts by status."""
        result: Dict[str, int] = {}
        for status in ("active", "pending", "closed"):
            _, count = self._repo.query({"status": status}, limit=0)
            result[status] = count
        return result
