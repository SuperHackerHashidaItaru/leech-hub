"""hub client - the runner's only way home.

sync httpx, retries on network trouble / 5xx, and every failure comes
back as a HubError with a human sentence (never an html blob, never a
traceback). claim() returns None on a 409 so two runners racing for the
same job resolve quietly.
"""

from __future__ import annotations

import time
from typing import Optional

import httpx


class HubError(Exception):
    pass


class HubClient:
    def __init__(self, base_url: str, token: str,
                 transport: Optional[object] = None):
        self._base = (base_url or "").rstrip("/")
        self._headers = {"x-leech-token": (token or "").strip()}
        self._transport = transport      # httpx.MockTransport in tests

    # ------------------------------------------------------------ plumbing

    def _client(self) -> httpx.Client:
        if self._transport is not None:
            return httpx.Client(timeout=30.0, transport=self._transport)
        return httpx.Client(timeout=30.0)

    def _call(self, method: str, path: str, *, json_body: Optional[dict] = None,
              params: Optional[dict] = None, allow_409: bool = False,
              retries: int = 3) -> Optional[dict]:
        last = "unreachable"
        for attempt in range(1, retries + 1):
            try:
                with self._client() as client:
                    resp = client.request(
                        method, self._base + path, json=json_body,
                        params=params, headers=self._headers,
                    )
            except httpx.HTTPError as exc:
                last = type(exc).__name__
                if attempt < retries:
                    time.sleep(min(2 * attempt, 6))
                continue
            if resp.status_code == 409 and allow_409:
                return None
            if 500 <= resp.status_code < 600:
                last = f"http {resp.status_code}"
                if attempt < retries:
                    time.sleep(min(2 * attempt, 6))
                continue
            try:
                data = resp.json()
            except ValueError:
                raise HubError(f"hub answered non-json ({resp.status_code})")
            if resp.status_code != 200 or not data.get("ok"):
                raise HubError(str(data.get("error") or
                                   f"hub answered {resp.status_code}"))
            return data
        raise HubError(f"hub unreachable after {retries} attempts ({last})")

    # ------------------------------------------------------------ the lane

    def claim(self, job_id: str, worker: str) -> Optional[dict]:
        data = self._call("POST", f"/api/jobs/{job_id}/claim",
                          json_body={"worker": worker}, allow_409=True)
        return (data or {}).get("job") if data else None

    def next_job(self, types: str) -> Optional[dict]:
        data = self._call("GET", "/api/jobs/next", params={"types": types})
        return (data or {}).get("job") if data else None

    def stage(self, job_id: str, stage: str, done: int = 0, total: int = 0,
              note: str = "") -> None:
        self._call("POST", f"/api/jobs/{job_id}/stage", json_body={
            "stage": stage, "done": done, "total": total, "note": note,
        }, retries=1)

    def done(self, job_id: str, result: dict) -> None:
        self._call("POST", f"/api/jobs/{job_id}/done",
                   json_body={"result": result})

    def fail(self, job_id: str, error: str) -> None:
        self._call("POST", f"/api/jobs/{job_id}/fail", json_body={
            "error": str(error)[:400],
        }, retries=2)

    def lyrics_get(self, artist: str, track: str,
                   album: Optional[str] = None, duration_ms: Optional[int] = None,
                   prefer: str = "synced") -> dict:
        params: dict = {"artist": artist, "track": track, "prefer": prefer}
        if album:
            params["album"] = album
        if duration_ms and int(duration_ms) > 0:
            params["duration"] = int(duration_ms) // 1000
        data = self._call("GET", "/api/lyrics/get", params=params)
        return dict((data or {}).get("data") or {})
