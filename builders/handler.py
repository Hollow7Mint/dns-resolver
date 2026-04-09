"""Dns Resolver — Resolver service layer."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class DnsHandler:
    """Business-logic service for Resolver operations in Dns Resolver."""

    def __init__(
        self,
        repo: Any,
        events: Optional[Any] = None,
    ) -> None:
        self._repo   = repo
        self._events = events
        logger.debug("DnsHandler started")

    def resolve(
        self, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the resolve workflow for a new Resolver."""
        if "resolved_at" not in payload:
            raise ValueError("Missing required field: resolved_at")
        record = self._repo.insert(
            payload["resolved_at"], payload.get("ttl"),
            **{k: v for k, v in payload.items()
              if k not in ("resolved_at", "ttl")}
        )
        if self._events:
            self._events.emit("resolver.resolved", record)
        return record

    def retry(self, rec_id: str, **changes: Any) -> Dict[str, Any]:
        """Apply *changes* to a Resolver and emit a change event."""
        ok = self._repo.update(rec_id, **changes)
        if not ok:
            raise KeyError(f"Resolver {rec_id!r} not found")
        updated = self._repo.fetch(rec_id)
        if self._events:
            self._events.emit("resolver.retryd", updated)
        return updated

    def forward(self, rec_id: str) -> None:
        """Remove a Resolver and emit a removal event."""
        ok = self._repo.delete(rec_id)
        if not ok:
            raise KeyError(f"Resolver {rec_id!r} not found")
        if self._events:
            self._events.emit("resolver.forwardd", {"id": rec_id})

    def search(
        self,
        resolved_at: Optional[Any] = None,
        status: Optional[str] = None,
        limit:  int = 50,
    ) -> List[Dict[str, Any]]:
        """Search resolvers by *resolved_at* and/or *status*."""
        filters: Dict[str, Any] = {}
        if resolved_at is not None:
            filters["resolved_at"] = resolved_at
        if status is not None:
            filters["status"] = status
        rows, _ = self._repo.query(filters, limit=limit)
        logger.debug("search resolvers: %d hits", len(rows))
        return rows

    @property
    def stats(self) -> Dict[str, int]:
        """Quick summary of Resolver counts by status."""
        result: Dict[str, int] = {}
        for status in ("active", "pending", "closed"):
            _, count = self._repo.query({"status": status}, limit=0)
            result[status] = count
        return result
