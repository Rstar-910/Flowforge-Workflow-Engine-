"""The built-in dashboard: static files served under /ui.

The page is plain HTML/CSS/JS shipped inside the package (no CDN, no build
step). It talks only to this API with the API key the user enters; the key is
kept in sessionStorage, so it is forgotten when the tab closes. A strict
Content-Security-Policy forbids inline and third-party scripts, so any data
that slipped past the page's escaping still could not execute.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from starlette.responses import Response
from starlette.staticfiles import StaticFiles
from starlette.types import Scope

STATIC_DIR = Path(__file__).parent / "static"

SECURITY_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
        "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    ),
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Cache-Control": "no-cache",
}


class DashboardFiles(StaticFiles):
    async def get_response(self, path: str, scope: Scope) -> Response:
        response = await super().get_response(path, scope)
        response.headers.update(SECURITY_HEADERS)
        return response


def mount_dashboard(app: FastAPI) -> None:
    app.mount("/ui", DashboardFiles(directory=STATIC_DIR, html=True), name="ui")

    @app.get("/", include_in_schema=False)
    async def root() -> Any:
        return RedirectResponse("/ui/")
