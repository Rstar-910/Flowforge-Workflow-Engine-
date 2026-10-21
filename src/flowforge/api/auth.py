"""API-key authentication and role/tenant authorisation.

Keys are configured as ``key:tenant:role`` (``FLOWFORGE_API_KEYS``). Every
resource belongs to a tenant and every query filters on the caller's tenant, so
a key can never read or act on another tenant's definitions, runs or tasks
(such requests get 404, not 403, to avoid confirming existence).
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status

from flowforge.config import ApiKeyEntry, Role


@dataclass(frozen=True, slots=True)
class Principal:
    tenant: str
    role: Role
    key_id: str  # non-secret fingerprint used for created_by / logs

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


def fingerprint(key: str) -> str:
    return "key_" + hashlib.sha256(key.encode()).hexdigest()[:12]


def _lookup(keys: dict[str, ApiKeyEntry], presented: str) -> ApiKeyEntry | None:
    match = None
    for key, entry in keys.items():
        # Constant-time comparison against every key.
        if hmac.compare_digest(key.encode(), presented.encode()):
            match = entry
    return match


async def authenticate(
    request: Request,
    x_api_key: Annotated[str | None, Header()] = None,
    authorization: Annotated[str | None, Header()] = None,
) -> Principal:
    presented = x_api_key
    if presented is None and authorization and authorization.lower().startswith("bearer "):
        presented = authorization[7:].strip()
    if not presented:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "missing API key (X-API-Key header or Bearer token)",
            headers={"WWW-Authenticate": "Bearer"},
        )
    entry = _lookup(request.app.state.api_keys, presented)
    if entry is None:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED, "invalid API key", headers={"WWW-Authenticate": "Bearer"}
        )
    return Principal(tenant=entry.tenant, role=entry.role, key_id=fingerprint(presented))


async def require_admin(principal: Annotated[Principal, Depends(authenticate)]) -> Principal:
    if not principal.is_admin:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "admin role required")
    return principal


CurrentPrincipal = Annotated[Principal, Depends(authenticate)]
AdminPrincipal = Annotated[Principal, Depends(require_admin)]
