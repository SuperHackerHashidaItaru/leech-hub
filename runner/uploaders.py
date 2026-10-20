"""the upload chain - ported verbatim from the proven colab worker
(worker.py, verified oct 2026 from datacenter ips):

    pixeldrain   20GB, longest-lived - needs a free account key
    x0           x0.at - 1GB, kept 3-100 days by size (403s fast on
                 some cloud ranges, chain moves on)
    uguu         128MB, 3h - small but answers upload AND download
    litterbox    1GB, 72h - accepts when others sulk
    catbox       200MB - often dead from datacenter ips, last resort

auto order: [pixeldrain if key] -> x0 -> uguu -> litterbox -> catbox.
pin the first host with UPLOAD_BACKEND, reorder backups with
UPLOAD_FALLBACKS.

public-log hygiene: failures are printed WITHOUT the returned url
(bodies can echo the filename); only host + status shape is logged.
"""

from __future__ import annotations

import os
import time
from urllib.parse import quote

import httpx

_MB = 1024 * 1024
_UA = {"User-Agent": "leech-runner/1"}
_UPLOAD_TIMEOUT = 900.0

UPLOAD_BACKEND = (os.getenv("UPLOAD_BACKEND") or "auto").strip().lower()
UPLOAD_FALLBACKS = [
    b.strip().lower()
    for b in (os.getenv("UPLOAD_FALLBACKS") or "").split(",") if b.strip()
]
PIXELDRAIN_KEY = (os.getenv("PIXELDRAIN_KEY") or "").strip()


def _post_file(url: str, data: dict | None, field: str, path: str) -> str:
    with open(path, "rb") as fh:
        files = {field: (os.path.basename(path), fh, "application/octet-stream")}
        with httpx.Client(timeout=_UPLOAD_TIMEOUT, headers=_UA) as client:
            response = client.post(url, data=data, files=files)
    text = response.text.strip()
    if response.status_code == 200 and text.startswith("http") and len(text) < 500:
        return text
    raise RuntimeError(f"http {response.status_code}: {text[:160]}")


def _upload_x0(path: str) -> str:
    return _post_file("https://x0.at/", {}, "file", path)


def _upload_uguu(path: str) -> str:
    return _post_file("https://uguu.se/upload?output=text", None, "files[]", path)


def _upload_litterbox(path: str) -> str:
    return _post_file(
        "https://litterbox.catbox.moe/resources/internals/api.php",
        {"reqtype": "fileupload", "time": "72h"}, "fileToUpload", path,
    )


def _upload_catbox(path: str) -> str:
    return _post_file(
        "https://catbox.moe/user/api.php",
        {"reqtype": "fileupload"}, "fileToUpload", path,
    )


def _upload_pixeldrain(path: str) -> str:
    if not PIXELDRAIN_KEY:
        raise RuntimeError("pixeldrain needs PIXELDRAIN_KEY (free account key)")
    with open(path, "rb") as fh:
        with httpx.Client(timeout=_UPLOAD_TIMEOUT, headers=_UA,
                          auth=("", PIXELDRAIN_KEY)) as client:
            response = client.put(
                "https://pixeldrain.com/api/file/" + quote(os.path.basename(path)),
                content=fh,
            )
    if response.status_code in (200, 201):
        try:
            file_id = str(response.json().get("id") or "")
        except ValueError:
            file_id = ""
        if file_id:
            return f"https://pixeldrain.com/api/file/{file_id}"
    raise RuntimeError(f"http {response.status_code}: {response.text[:160]}")


_UPLOADERS = {
    "pixeldrain": _upload_pixeldrain,
    "x0": _upload_x0,
    "uguu": _upload_uguu,
    "litterbox": _upload_litterbox,
    "catbox": _upload_catbox,
}
_UPLOAD_CAP_MB = {
    "pixeldrain": 20000, "x0": 1024, "uguu": 128,
    "litterbox": 1000, "catbox": 200,
}
_AUTO_ORDER = ["x0", "uguu", "litterbox", "catbox"]


def _upload_order(backend: str | None = None) -> list[str]:
    choice = (backend or UPLOAD_BACKEND or "auto").strip().lower()
    if choice in ("", "none"):
        raise RuntimeError("uploads are disabled (UPLOAD_BACKEND=none)")
    auto = (["pixeldrain"] if PIXELDRAIN_KEY else []) + list(_AUTO_ORDER)
    if choice == "auto":
        return auto
    if choice not in _UPLOADERS:
        raise RuntimeError(f"unknown upload backend {choice!r} - one of: "
                           + ", ".join(_UPLOADERS))
    if UPLOAD_FALLBACKS:
        return [choice] + [b for b in UPLOAD_FALLBACKS
                           if b in _UPLOADERS and b != choice]
    return [choice] + [b for b in auto if b != choice]


def upload_file(path: str, backend: str | None = None) -> tuple[str, str]:
    """-> (url, host). walks the failover chain until a host takes the
    file; raises with every host's reason if none does."""
    order = _upload_order(backend)
    size_mb = os.path.getsize(path) / _MB
    problems: list[str] = []
    for host in order:
        cap = _UPLOAD_CAP_MB[host]
        if size_mb > cap:
            problems.append(f"{host}: {size_mb:.0f}mb over its {cap}mb cap")
            continue
        try:
            url = _UPLOADERS[host](path)
            print(f"upload ok via {host} ({size_mb:.1f}mb)")
            return url, host
        except Exception as exc:  # noqa: BLE001 - next host gets a chance
            problems.append(f"{host}: {type(exc).__name__}")
            print(f"upload via {host} failed ({type(exc).__name__}) - trying next")
            time.sleep(2)
    raise RuntimeError("every upload host refused the file - "
                       + " | ".join(problems))
