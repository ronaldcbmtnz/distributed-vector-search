"""Authentication (API key → client) and per-collection authorization, enforced server-side.

Keys are configured as sha256 digests; the clear key is only hashed, never stored or logged.
Isolation between clients never relies on a filter sent by the client (decision D16).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Literal

from dvs.config import Config
from dvs.core.errors import PermissionDenied, Unauthenticated

Action = Literal["read", "write"]


@dataclass(frozen=True, slots=True)
class Principal:
    client: str
    read: frozenset[str]
    write: frozenset[str]
    admin: bool


class Authenticator:
    def __init__(self, config: Config) -> None:
        self._by_digest = {
            digest: Principal(p.client, frozenset(p.read), frozenset(p.write), p.admin)
            for digest, p in config.api_keys.items()
        }

    def authenticate(self, authorization: str | None) -> Principal:
        if not authorization or not authorization.startswith("Bearer "):
            raise Unauthenticated("missing bearer API key")
        digest = hashlib.sha256(authorization[len("Bearer ") :].strip().encode()).hexdigest()
        principal = self._by_digest.get(digest)
        if principal is None:
            raise Unauthenticated("invalid API key")
        return principal

    @staticmethod
    def authorize(p: Principal, collection: str, action: Action) -> None:
        allowed = p.read if action == "read" else p.write
        if collection not in allowed:
            raise PermissionDenied(f"no {action} permission on collection", collection=collection)

    @staticmethod
    def require_admin(p: Principal) -> None:
        if not p.admin:
            raise PermissionDenied("admin permission required")
