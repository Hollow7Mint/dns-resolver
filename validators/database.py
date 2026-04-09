"""Dns Resolver — Record database layer."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class DnsDatabase:
    """Record database for the Dns Resolver application."""

    def __init__(
        self,
        store: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._store = store
        self._cfg   = config or {}
        self._value = self._cfg.get("value", None)
        logger.debug("%s initialised", self.__class__.__name__)

    def expire_record(
        self, value: Any, record_type: Any, **extra: Any
    ) -> Dict[str, Any]:
        """Create and persist a new Record record."""
        now = datetime.now(timezone.utc).isoformat()
        record: Dict[str, Any] = {
            "id":         str(uuid.uuid4()),
            "value": value,
            "record_type": record_type,
            "status":     "active",
            "created_at": now,
            **extra,
        }
        saved = self._store.put(record)
        logger.info("expire_record: created %s", saved["id"])
        return saved

    def get_record(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a Record by its *record_id*."""
        record = self._store.get(record_id)
        if record is None:
            logger.debug("get_record: %s not found", record_id)
        return record

    def resolve_record(
        self, record_id: str, **changes: Any
    ) -> Dict[str, Any]:
        """Apply *changes* to an existing Record."""
        record = self._store.get(record_id)
        if record is None:
            raise KeyError(f"Record {record_id!r} not found")
        record.update(changes)
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        return self._store.put(record)

    def validate_record(self, record_id: str) -> bool:
        """Remove a Record; returns True on success."""
        if self._store.get(record_id) is None:
            return False
        self._store.delete(record_id)
        logger.info("validate_record: removed %s", record_id)
        return True

    def list_records(
        self,
        status: Optional[str] = None,
        limit:  int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return paginated Record records."""
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        results = self._store.find(query, limit=limit, offset=offset)
        logger.debug("list_records: %d results", len(results))
        return results

    def iter_records(
        self, batch_size: int = 100
    ) -> Iterator[Dict[str, Any]]:
        """Yield all Record records in batches of *batch_size*."""
        offset = 0
        while True:
            page = self.list_records(limit=batch_size, offset=offset)
            if not page:
                break
            yield from page
            if len(page) < batch_size:
                break
            offset += batch_size
