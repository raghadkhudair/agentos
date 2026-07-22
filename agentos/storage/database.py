"""Backward-compatible PostgreSQL boundary.

New code should import :class:`PostgresClient` from ``agentos.storage.clients``.
The historical ``DatabaseManager`` name is retained for repository compatibility.
"""

from agentos.storage.clients.postgres import PostgresClient

DatabaseManager = PostgresClient

__all__ = ["DatabaseManager", "PostgresClient"]
