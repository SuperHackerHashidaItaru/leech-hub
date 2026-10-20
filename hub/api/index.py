"""
leech hub - the wispbyte leech-server's replacement, minus the always-on part.

    bot (bot hosting, always up)
      ├─ search: DIRECT https GETs to tracks.monochrome.st (unchanged; this
      │  hub's /api/mono/search stays as cached proxy fallback + lyrics lane)
      ├─ POST /api/mono/track|album, /api/ffmpeg/convert|trim ──▶ ┌───────┐
      ├─ GET  /api/jobs/{id} ◀── polling ◀──────────────────────── │ this  │
      └─ GET  /api/lyrics/*, /api/mono/release/{id}               │ file  │
                                                                   │ vercel│
    github actions runner (public repo = free unlimited minutes)   │ hobby│
      ├─ repository_dispatch {"job_id"}  ◀── hub fires this on    │+upstash│
      │   every submit (PAT, contents:write)                       │ redis#2│
      ├─ schedule */15 * * * * janitor for missed dispatches      └───────┘
      └─ claim → download/convert/tag/zip → upload (pixeldrain chain)
          → POST /api/jobs/{id}/done → bot's next poll sees the CDN url.

why: wispbyte killed the 24/7 box for averaging 18% cpu. nothing here is
always-on - vercel functions wake per request, the runner boots per job
(4 vcpu, ~30-60s cold), tts stays on the old manager + colab/kaggle lane
(jobs redis #1). this hub uses its OWN upstash database (#2) exactly so
the two lanes never fight over keys, quotas or a shared failure.

API COMPAT: the bot-facing surface intentionally mirrors the old
leech-server (same paths, same x-leech-token header, same {ok,...}
envelopes, same job doc shape: status queued|running|done|error,
progress {stage,done,total,note}). mono_service.py / ffmpeg_cog.py keep
working with LEECH_MONO_URL pointed here. differences, all additive:
result carries a full `url` now (the CDN link) instead of a /files path,
plus new endpoints for lyrics and release tracklists.

errors: everything is json {"ok": false, "error": "..."} - never html,
never a traceback. upstream timeouts get 502s with a human sentence.

env:
    TOKEN                          shared secret (x-leech-token), fail closed
    UPSTASH_REDIS_REST_URL         upstash database #2 REST url
    UPSTASH_REDIS_REST_TOKEN       its token
    MONO_BASE                      default https://tracks.monochrome.st
    LRCLIB_BASE                    default https://lrclib.net
    GITHUB_REPO                    owner/leech-runner (the public repo)
    GITHUB_PAT                     fine-grained PAT: contents read+write on
                                   that repo only (repository_dispatch)

deploy: see DEPLOY.md at the zip root.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import secrets
import time
import random
import unicodedata
from difflib import SequenceMatcher
from typing import Any, Optional

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

_logger = logging.getLogger("leech.hub")

# ----------------------------------------------------------------- env

TOKEN = (os.environ.get("TOKEN") or "").strip()
UPSTASH_URL = (os.environ.get("UPSTASH_REDIS_REST_URL") or "").strip().rstrip("/")
UPSTASH_TOKEN = (os.environ.get("UPSTASH_REDIS_REST_TOKEN") or "").strip()
MONO_BASE = (os.environ.get("MONO_BASE") or "https://tracks.monochrome.st").strip().rstrip("/")
LRCLIB_BASE = (os.environ.get("LRCLIB_BASE") or "https://lrclib.net").strip().rstrip("/")
GITHUB_API = (os.environ.get("GITHUB_API") or "https://api.github.com").strip().rstrip("/")
GITHUB_REPO = (os.environ.get("GITHUB_REPO") or "").strip().strip("/")
GITHUB_PAT = (os.environ.get("GITHUB_PAT") or "").strip()

JOB_TTL_S = 86400          # results stay readable for a day
CLAIM_TTL_S = 2700         # a claim older than 45 min is stale
QUEUE_KEY = "leech:queue"
KINDS = ("mono_track", "mono_album", "ffmpeg_convert", "ffmpeg_trim", "yt")
QUALITIES = ("flac", "320", "256", "192", "128", "96", "64", "wav", "m4a", "opus", "copy")
AUDIO_FMTS = ("mp3", "wav", "flac", "m4a", "opus", "copy")
VIDEO_FMTS = ("mp4", "mkv", "webm", "gif")
RESOLUTIONS = (240, 360, 480, 720, 1080, 1440, 2160)
LYRICS_MODES = ("off", "embed", "sidecar")
LYRICS_PREFERS = ("synced", "plain")
LRCLIB_UA = (
    "leech-hub/1.0 (discord music bot; "
    + (f"https://github.com/{GITHUB_REPO}" if GITHUB_REPO else "personal project")
    + ")"
)

_PRIVATE_HOST_RE = re.compile(
    r"^(localhost|127\.|0\.0\.0\.0|10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\."
    r"|169\.254\.|\[?::1\]?|.+\.internal|metadata\.google\.internal)",
    re.IGNORECASE,
)

RUNNING_TASKS: set = set()          # strong refs for fire-and-forget dispatches


# ----------------------------------------------------------------- redis

class _Redis:
    """upstash REST, pipelined array form (same client pattern as the manager)."""

    def __init__(self):
        if not UPSTASH_URL or not UPSTASH_TOKEN:
            raise HTTPException(503, "upstash env vars are not set")

    async def run(self, *cmd) -> Any:
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(
                    UPSTASH_URL,
                    headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
                    json=[str(c) for c in cmd],
                )
        except httpx.HTTPError as exc:
            raise HTTPException(502, f"upstash unreachable: {type(exc).__name__}")
        if response.status_code != 200:
            raise HTTPException(502, f"upstash http {response.status_code}")
        try:
            data = response.json()
        except ValueError:
            raise HTTPException(502, "upstash returned non-json")
        if isinstance(data, dict) and "error" in data:
            raise HTTPException(502, f"upstash: {data['error']}")
        return data.get("result") if isinstance(data, dict) else None


def _redis() -> _Redis:
    return _Redis()


async def _cache_get(key: str) -> Any:
    """cache reads degrade to None on redis trouble - the request still runs."""
    try:
        raw = await _redis().run("GET", key)
    except HTTPException as exc:
        _logger.warning("cache read skipped (%s)", exc.detail)
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None


async def _cache_set(key: str, value: Any, ttl: int) -> None:
    try:
        await _redis().run("SET", key, json.dumps(value), "EX", str(ttl))
    except HTTPException as exc:
        _logger.warning("cache write skipped (%s)", exc.detail)


async def _cached(key: str, ttl: int, factory):
    value = await _cache_get(key)
    if value is not None:
        return value
    value = await factory()
    await _cache_set(key, value, ttl)
    return value


# ---------------------------------------------------- upstream: monochrome

async def _mono_get_json(path: str, params: dict | None = None) -> dict:
    """one GET against mono, clean failure. tests monkeypatch this."""
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.get(MONO_BASE + path, params=params)
    except httpx.HTTPError as exc:
        raise HTTPException(502, f"monochrome unreachable: {type(exc).__name__}")
    if resp.status_code != 200:
        raise HTTPException(502, f"monochrome answered {resp.status_code}")
    try:
        return resp.json()
    except ValueError:
        raise HTTPException(502, "monochrome returned non-json")


def _unwrap(data):
    if isinstance(data, dict):
        for key in ("release", "data", "result"):
            inner = data.get(key)
            if isinstance(inner, dict):
                return inner
    return data


def _names(value) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(v) for v in value][:4]
    return []


def _norm_track(item) -> Optional[dict]:
    if not isinstance(item, dict):
        return None
    tid = str(item.get("id") or item.get("trackId") or "").strip()
    if not tid.isdigit() or len(tid) < 6:
        return None
    try:
        duration = int(item.get("duration") or 0)
    except (TypeError, ValueError):
        duration = 0
    return {
        "id": tid,
        "title": str(item.get("title") or f"track {tid[-4:]}"),
        "artists": _names(item.get("artistNames")),
        "duration": duration,
        "artwork": item.get("artwork") or None,
        "position": item.get("position") or item.get("trackNumber") or None,
    }


def _release_tracks(rel: dict) -> list[dict]:
    for key in ("tracks", "trackList", "tracklist", "track_list"):
        value = rel.get(key)
        if isinstance(value, list):
            found = [t for t in (_norm_track(x) for x in value) if t]
            if found:
                return found
    for key in ("media", "discs", "volumes"):
        value = rel.get(key)
        if isinstance(value, list):
            found: list[dict] = []
            for disc in value:
                if isinstance(disc, dict):
                    for inner in ("tracks", "trackList", "tracklist"):
                        if isinstance(disc.get(inner), list):
                            found += [t for t in (_norm_track(x) for x in disc[inner]) if t]
            if found:
                return found
    return []


def _release_meta(rel: dict) -> dict:
    date = str(rel.get("releaseDate") or "")
    return {
        "title": rel.get("title"),
        "artists": _names(rel.get("artistNames")),
        "artwork": rel.get("artwork"),
        "year": date[:4] if date[:4].isdigit() else None,
        "type": (rel.get("releaseType") or "").lower() or None,
        "label": rel.get("label"),
    }


async def mono_search(query: str) -> dict:
    """raw mono /search json (the cog parses it with parse_search), cached 15m."""
    query = (query or "").strip()
    if len(query) < 2:
        raise HTTPException(400, "search query too short")

    async def factory() -> dict:
        return await _mono_get_json("/search", {"q": query})

    key = "c:mono:search:" + hashlib.sha1(query.lower().encode()).hexdigest()
    return await _cached(key, 900, factory)


async def mono_release(release_id: str) -> dict:
    """normalized {meta..., tracks:[...]} for /releases/{id}, cached 1h."""
    rid = (release_id or "").strip()
    if not rid.isdigit() or len(rid) < 6:
        raise HTTPException(400, "that doesn't look like a release id")

    async def factory() -> dict:
        rel = _unwrap(await _mono_get_json(f"/releases/{rid}"))
        if not isinstance(rel, dict):
            raise HTTPException(502, f"release {rid} json is not an object (shape?)")
        meta = _release_meta(rel)
        tracks = _release_tracks(rel)
        if not tracks:
            raise HTTPException(
                502,
                f"couldn't find a track list for release {rid} (unexpected shape)",
            )
        return {"id": rid, **meta, "tracks": tracks}

    return await _cached(f"c:mono:rel:{rid}", 3600, factory)


# ---------------------------------------------------- upstream: lrclib
def _retry_after_seconds(raw: Optional[str]) -> Optional[float]:
    """Retry-After as delay-seconds (lrclib's form). http-dates/junk -> None."""
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return max(0.0, float(text))
    except ValueError:
        return None


async def _lrclib_get(path: str, params: dict) -> Any:
    """one lrclib GET, Retry-After aware.

    their docs: 429 (RateLimitExceeded) and 503 (ServerOverloaded - "retry
    after the Retry-After header, usually 1 second") both carry Retry-After,
    clients MUST honor it with a little jitter, and under floods the edge
    may answer non-json - so the status code + header decide, never the
    body. 200 -> json, 404 -> None (a real miss), still busy after the
    retries -> clean 503. waits cap at 5s so one lyrics call can't eat
    the whole function budget.
    """
    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(
                    LRCLIB_BASE + path, params=params,
                    headers={"User-Agent": LRCLIB_UA},
                )
        except httpx.HTTPError as exc:
            raise HTTPException(502, f"lrclib unreachable: {type(exc).__name__}")
        if resp.status_code == 404:
            return None
        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError:
                raise HTTPException(502, "lrclib returned non-json")
        busy = resp.status_code in (429, 503) or 500 <= resp.status_code < 600
        if busy and attempt < attempts:
            wait = _retry_after_seconds(resp.headers.get("Retry-After"))
            if wait is None:
                wait = 1.0 * attempt          # no header: gentle backoff
            wait = min(wait, 5.0)
            await asyncio.sleep(wait + random.uniform(0.0, wait * 0.3))
            continue
        if resp.status_code in (429, 503):
            raise HTTPException(
                503, "lrclib is busy (rate limited / overloaded) - "
                     "try again in a bit")
        raise HTTPException(502, f"lrclib answered {resp.status_code}")
    raise HTTPException(502, "lrclib unreachable after retries")

def _norm_text(value: str) -> str:
    """lowercase, fold accents, drop (feat/live/remix) parens + punctuation."""
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"\([^)]*\)", " ", text.lower())
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _ratio(a: str, b: str) -> float:
    na, nb = _norm_text(a), _norm_text(b)
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


_LRC_TAG_RE = re.compile(r"^\[[0-9:.]+\]\s*")


def _strip_lrc(synced: str) -> Optional[str]:
    """plain lyrics from synced ones (drop [mm:ss.xx] tags and empty lines)."""
    lines = []
    for line in str(synced or "").splitlines():
        clean = _LRC_TAG_RE.sub("", line).strip()
        if clean:
            lines.append(clean)
    return "\n".join(lines) or None


def _lyrics_record(item: dict) -> dict:
    return {
        "id": item.get("id"),
        "track": item.get("trackName"),
        "artist": item.get("artistName"),
        "album": item.get("albumName"),
        "duration": item.get("duration"),
        "instrumental": bool(item.get("instrumental")),
        "synced": item.get("syncedLyrics") or None,
        "plain": item.get("plainLyrics") or None,
    }


async def lyrics_search(query: str) -> list[dict]:
    query = (query or "").strip()
    if len(query) < 2:
        raise HTTPException(400, "search query too short")

    async def factory() -> list[dict]:
        data = await _lrclib_get("/api/search", {"q": query})
        if not isinstance(data, list):
            raise HTTPException(502, "lrclib search returned non-list")
        return [_lyrics_record(x) for x in data if isinstance(x, dict)][:12]

    key = "c:lyr:search:" + hashlib.sha1(query.lower().encode()).hexdigest()
    return await _cached(key, 1800, factory)


async def lyrics_get(artist: str, track: str, album: Optional[str] = None,
                     duration: Optional[int] = None,
                     prefer: str = "synced") -> dict:
    """the validated fetch: exact /api/get first, then filtered search with
    fuzzy artist/track/album matching, so 'same artist/album' is actually
    enforced and a lookalike song never sneaks in. cached 6h (misses 30m)."""
    artist = (artist or "").strip()
    track = (track or "").strip()
    prefer = prefer if prefer in LYRICS_PREFERS else "synced"
    if not artist or not track:
        raise HTTPException(400, "lyrics get needs artist and track")
    duration = int(duration) if duration and int(duration) > 0 else None

    key = "c:lyr:get:" + hashlib.sha1(
        "|".join([
            _norm_text(artist), _norm_text(track),
            _norm_text(album or ""), str(duration or ""), prefer,
        ]).encode()
    ).hexdigest()

    async def factory() -> dict:
        params: dict[str, Any] = {"artist_name": artist, "track_name": track}
        if album:
            params["album_name"] = album
        if duration:
            params["duration"] = duration
        best: Optional[dict] = None
        try:
            exact = await _lrclib_get("/api/get", params)
        except HTTPException as exc:
            raise HTTPException(502, f"lrclib exact lookup failed: {exc.detail}")
        if isinstance(exact, dict):
            best = _lyrics_record(exact)
        if best is None:
            # v3: no validation - first search hit wins, the user picks
            # with index= on the cog side. exact /api/get above already
            # returned "the" match for these fields when lrclib has one.
            results = await _lrclib_get("/api/search", {
                "artist_name": artist, "track_name": track,
            })
            candidates = [x for x in (results or []) if isinstance(x, dict)]
            if candidates:
                best = _lyrics_record(candidates[0])
        if best is None:
            return {"found": False, "reason": "lrclib has nothing for that "
                    "artist/track", "prefer": prefer}
        if best["instrumental"]:
            out = {**best, "found": True, "plain": None, "synced": None,
                   "chosen": "instrumental", "prefer": prefer}
            return out
        plain = best["plain"] or _strip_lrc(best["synced"] or "")
        chosen = ("synced" if prefer == "synced" and best["synced"] else
                  "plain" if plain else None)
        return {**best, "plain": plain, "found": True, "chosen": chosen,
                "prefer": prefer}

    value = await _cache_get(key)
    if value is not None:
        return value
    value = await factory()
    await _cache_set(key, value, 21600 if value.get("found") else 1800)
    return value


# ----------------------------------------------------------------- jobs

def _view(job: dict) -> dict:
    return {
        "id": job["id"], "kind": job["kind"], "status": job["status"],
        "payload": job.get("payload"),          # the runner needs the body
        "progress": job.get("progress"), "error": job.get("error"),
        "result": job.get("result"),
        "created": job.get("created"), "started": job.get("started"),
        "finished": job.get("finished"),
    }


async def _job_get(job_id: str) -> Optional[dict]:
    raw = await _redis().run("GET", f"j:{job_id}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return None


async def _job_save(job: dict) -> None:
    job["updated"] = time.time()
    await _redis().run("SET", f"j:{job['id']}", json.dumps(job), "EX", str(JOB_TTL_S))


def _dispatch_job(job_id: str) -> None:
    """fire repository_dispatch at the runner repo; never raises (janitor
    catches whatever this misses)."""
    if not (GITHUB_REPO and GITHUB_PAT):
        _logger.info("dispatch skipped (no GITHUB_REPO/GITHUB_PAT) - janitor only")
        return

    async def _go() -> None:
        try:
            async with httpx.AsyncClient(timeout=12.0) as client:
                resp = await client.post(
                    f"{GITHUB_API}/repos/{GITHUB_REPO}/dispatches",
                    headers={
                        "Authorization": f"Bearer {GITHUB_PAT}",
                        "Accept": "application/vnd.github+json",
                    },
                    json={"event_type": "leech-job",
                          "client_payload": {"job_id": job_id}},
                )
            if resp.status_code not in (200, 204):
                _logger.warning("dispatch http %s for job %s (janitor will catch it)",
                                resp.status_code, job_id)
        except httpx.HTTPError as exc:
            _logger.warning("dispatch %s for job %s (janitor will catch it)",
                            type(exc).__name__, job_id)

    task = asyncio.get_running_loop().create_task(_go())
    RUNNING_TASKS.add(task)
    task.add_done_callback(RUNNING_TASKS.discard)


async def _submit(kind: str, payload: dict) -> dict:
    if kind not in KINDS:
        raise HTTPException(400, f"unknown job kind {kind!r}")
    job = {
        "id": secrets.token_hex(6), "kind": kind, "status": "queued",
        "payload": payload, "progress": None, "error": None, "result": None,
        "worker": None, "created": time.time(), "updated": time.time(),
        "started": None, "finished": None,
    }
    await _job_save(job)
    await _redis().run("LPUSH", QUEUE_KEY, job["id"])
    _dispatch_job(job["id"])
    return job


async def _queue_remove(job_id: str) -> None:
    try:
        await _redis().run("LREM", QUEUE_KEY, "0", job_id)
    except HTTPException:
        pass


async def _require_job(job_id: str) -> dict:
    job = await _job_get(job_id)
    if job is None:
        raise HTTPException(404, f"no such job {job_id!r} (typo, or older than a day)")
    return job


def _parse_time(value) -> Optional[float]:
    """'90', '45.5', '1:30', '1:02:03' → seconds. else None."""
    text = str(value or "").strip()
    if not text:
        return None
    if ":" in text:
        parts = text.split(":")
        if len(parts) > 3 or not all(parts):
            return None
        try:
            if len(parts) == 3:
                hours, minutes, seconds = int(parts[0]), int(parts[1]), float(parts[2])
            else:
                hours, minutes, seconds = 0, int(parts[0]), float(parts[1])
        except ValueError:
            return None
        if minutes >= 60 or seconds >= 60:
            return None
        return hours * 3600 + minutes * 60 + seconds
    if not re.match(r"^\d+(\.\d+)?$", text):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _check_url(url: str) -> str:
    from urllib.parse import urlparse
    parsed = urlparse(url or "")
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise HTTPException(400, "invalid http(s) url")
    if _PRIVATE_HOST_RE.match(parsed.netloc):
        raise HTTPException(400, "i don't fetch urls that point at private hosts")
    return url


def _lyrics_cfg(raw: Optional[dict]) -> dict:
    cfg = raw if isinstance(raw, dict) else {}
    mode = str(cfg.get("mode") or "off").lower()
    prefer = str(cfg.get("prefer") or "synced").lower()
    return {
        "mode": mode if mode in LYRICS_MODES else "off",
        "prefer": prefer if prefer in LYRICS_PREFERS else "synced",
    }


# ----------------------------------------------------------------- models

class MonoSubmit(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str
    quality: str = "flac"
    meta: dict = {}
    lyrics: Optional[dict] = None
    zip: bool = True          # v4: False = loose files instead of an album zip

class ConvertSubmit(BaseModel):
    model_config = ConfigDict(extra="ignore")
    url: str
    format: Optional[str] = None
    bitrate: Optional[int] = None
    res: Optional[int] = None
    crf: Optional[int] = None
    fps: Optional[int] = None
    mute: bool = False


class TrimSubmit(ConvertSubmit):
    start: Optional[str] = None
    end: Optional[str] = None


class YtSubmit(BaseModel):
    model_config = ConfigDict(extra="ignore")
    url: str
    audio_only: bool = True
    format: Optional[str] = None
    bitrate: Optional[int] = None


class JobIn(BaseModel):
    model_config = ConfigDict(extra="ignore")
    kind: str
    payload: dict = {}


class ClaimIn(BaseModel):
    model_config = ConfigDict(extra="ignore")
    worker: str = "runner"


class StageIn(BaseModel):
    model_config = ConfigDict(extra="ignore")
    stage: str
    done: Optional[int] = None
    total: Optional[int] = None
    note: Optional[str] = None


class DoneIn(BaseModel):
    model_config = ConfigDict(extra="ignore")
    result: dict = {}


class FailIn(BaseModel):
    model_config = ConfigDict(extra="ignore")
    error: str = "the job failed on the runner"


# ----------------------------------------------------------------- app

app = FastAPI(title="leech hub", version=1)


def _authorized(
    x_leech_token: str | None = Header(default=None, alias="x-leech-token"),
) -> None:
    if not TOKEN:
        raise HTTPException(503, "hub has no TOKEN set. refusing (fail closed)")
    supplied = (x_leech_token or "").strip()
    if not supplied or not secrets.compare_digest(supplied, TOKEN):
        raise HTTPException(401, "bad or missing token")


@app.get("/")
async def root():
    return {
        "ok": True, "service": "leech-hub", "version": 1,
        "auth": bool(TOKEN), "redis": bool(UPSTASH_URL and UPSTASH_TOKEN),
        "runner": {"repo": GITHUB_REPO or None,
                   "dispatch": bool(GITHUB_REPO and GITHUB_PAT)},
    }


@app.get("/api/health")
async def health(_=Depends(_authorized)):
    try:
        depth = await _redis().run("LLEN", QUEUE_KEY)
    except HTTPException as exc:
        raise HTTPException(502, f"redis trouble: {exc.detail}")
    return {
        "ok": True, "queue_depth": int(depth or 0),
        "runner": {"repo": GITHUB_REPO or None,
                   "dispatch": bool(GITHUB_REPO and GITHUB_PAT)},
        "kinds": list(KINDS),
    }


# ---- mono (bot-facing, old-server compatible shapes) -------------------

@app.get("/api/mono/search")
async def api_mono_search(q: str = "", _=Depends(_authorized)):
    return {"ok": True, "data": await mono_search(q)}


@app.get("/api/mono/release/{release_id}")
async def api_mono_release(release_id: str, _=Depends(_authorized)):
    return {"ok": True, "data": await mono_release(release_id)}


@app.post("/api/mono/track")
async def api_mono_track(body: MonoSubmit, _=Depends(_authorized)):
    tid = (body.id or "").strip()
    if not tid.isdigit() or len(tid) < 6:
        raise HTTPException(400, "that doesn't look like a track id")
    quality = (body.quality or "").strip().lower()
    if quality not in QUALITIES:
        raise HTTPException(400, "quality must be one of: " + ", ".join(QUALITIES))
    payload = {
        "track_id": tid,
        "url": f"{MONO_BASE}/track/{tid}",
        "quality": quality,
        "meta": body.meta or {},
        "lyrics": _lyrics_cfg(body.lyrics),
    }
    job = await _submit("mono_track", payload)
    return {"ok": True, "job": job["id"]}


@app.post("/api/mono/album")
async def api_mono_album(body: MonoSubmit, _=Depends(_authorized)):
    rid = (body.id or "").strip()
    if not rid.isdigit() or len(rid) < 6:
        raise HTTPException(400, "that doesn't look like a release id")
    quality = (body.quality or "").strip().lower()
    if quality not in QUALITIES:
        raise HTTPException(400, "quality must be one of: " + ", ".join(QUALITIES))
    try:
        release = await mono_release(rid)
    except HTTPException as exc:
        raise HTTPException(502, f"album lookup failed: {exc.detail}")
    meta = dict(body.meta or {})
    meta.update({k: v for k, v in release.items()
                 if k not in ("tracks", "id") and v})
    tracks = [
        {**t, "url": f"{MONO_BASE}/track/{t['id']}"}
        for t in release["tracks"]
    ]
    payload = {
        "release_id": rid, "quality": quality, "meta": meta,
        "lyrics": _lyrics_cfg(body.lyrics), "tracks": tracks,
        "zip": body.zip,          # v4: False = loose files (needs runner v4)
    }
    job = await _submit("mono_album", payload)
    return {"ok": True, "job": job["id"]}


# ---- ffmpeg (audio + video now) ----------------------------------------

def _check_convert(body: ConvertSubmit) -> dict:
    url = _check_url(body.url)
    fmt = (body.format or "mp3").strip().lower()
    if fmt not in AUDIO_FMTS and fmt not in VIDEO_FMTS:
        raise HTTPException(
            400, f"format must be one of: {', '.join(AUDIO_FMTS + VIDEO_FMTS)}")
    bitrate = body.bitrate
    if bitrate is not None and not (32 <= int(bitrate) <= 320):
        raise HTTPException(400, "bitrate must be 32-320 kbps")
    if body.res is not None and int(body.res) not in RESOLUTIONS:
        raise HTTPException(400, f"res must be one of: {', '.join(map(str, RESOLUTIONS))}")
    if body.crf is not None and not (14 <= int(body.crf) <= 34):
        raise HTTPException(400, "crf must be 14-34")
    if body.fps is not None and not (1 <= int(body.fps) <= 120):
        raise HTTPException(400, "fps must be 1-120")
    return {
        "url": url, "format": fmt,
        "bitrate": int(bitrate) if bitrate is not None else None,
        "res": int(body.res) if body.res is not None else None,
        "crf": int(body.crf) if body.crf is not None else None,
        "fps": int(body.fps) if body.fps is not None else None,
        "mute": bool(body.mute),
    }


@app.post("/api/ffmpeg/convert")
async def api_ffmpeg_convert(body: ConvertSubmit, _=Depends(_authorized)):
    job = await _submit("ffmpeg_convert", _check_convert(body))
    return {"ok": True, "job": job["id"]}


@app.post("/api/ffmpeg/trim")
async def api_ffmpeg_trim(body: TrimSubmit, _=Depends(_authorized)):
    checked = _check_convert(body)
    start = _parse_time(body.start)
    end = _parse_time(body.end)
    if start is None or end is None or end <= start:
        raise HTTPException(400, "trim needs start and end (end after start), "
                                 "seconds or m:ss or h:mm:ss")
    checked.update({"start": start, "end": end})
    job = await _submit("ffmpeg_trim", checked)
    return {"ok": True, "job": job["id"]}


# ---- generic jobs + worker lane ----------------------------------------

@app.post("/api/jobs")
async def api_jobs_create(body: JobIn, _=Depends(_authorized)):
    """generic lane (yt today; anything else gets its own endpoint)."""
    kind = (body.kind or "").strip()
    if kind not in KINDS:
        raise HTTPException(400, f"unknown job kind {kind!r} - i know: {', '.join(KINDS)}")
    payload = body.payload or {}
    if kind == "yt":
        url = _check_url(str(payload.get("url") or ""))
        audio = bool(payload.get("audio"))
        quality = payload.get("quality")
        if quality is not None:
            try:
                quality = int(quality)
            except (TypeError, ValueError):
                raise HTTPException(400, "quality must be a number (video height)")
            if not 144 <= quality <= 2160:
                raise HTTPException(400, "quality must be 144-2160")
        max_mb = payload.get("max_mb")
        if max_mb is not None:
            try:
                max_mb = float(max_mb)
            except (TypeError, ValueError):
                raise HTTPException(400, "max_mb must be a number")
            if not 1 <= max_mb <= 2000:
                raise HTTPException(400, "max_mb must be 1-2000")
        start, end = payload.get("start"), payload.get("end")
        if start is not None or end is not None:
            try:
                start = float(start or 0)
                end = float(end if end is not None else 0)
            except (TypeError, ValueError):
                raise HTTPException(400, "clip start/end must be numbers")
            if end <= start:
                raise HTTPException(400, "clip end must be after start")
        else:
            start = end = None
        payload = {
            "url": url, "audio": audio, "quality": quality,
            "max_mb": max_mb, "start": start, "end": end,
        }
    job = await _submit(kind, payload)
    return {"ok": True, "job": job["id"]}


@app.get("/api/jobs/next")
async def api_jobs_next(types: str = "", _=Depends(_authorized)):
    wanted = {t.strip() for t in types.split(",") if t.strip()} or set(KINDS)
    ids = await _redis().run("LRANGE", QUEUE_KEY, "0", "99")
    for jid in ids or []:
        job = await _job_get(str(jid))
        if job and job.get("status") == "queued" and job.get("kind") in wanted:
            await _queue_remove(job["id"])
            return {"ok": True, "job": _view(job)}
    return {"ok": True, "job": None}


@app.get("/api/jobs/{job_id}")
async def api_job_status(job_id: str, _=Depends(_authorized)):
    job = await _require_job(job_id)
    return {"ok": True, "job": _view(job)}


@app.post("/api/jobs/{job_id}/claim")
async def api_job_claim(job_id: str, body: ClaimIn, _=Depends(_authorized)):
    job = await _require_job(job_id)
    if job.get("status") != "queued":
        raise HTTPException(409, f"job {job_id} is {job.get('status')} already")
    claimed = await _redis().run(
        "SET", f"claim:{job_id}", (body.worker or "runner")[:60],
        "NX", "EX", str(CLAIM_TTL_S))
    if claimed is None:
        raise HTTPException(409, f"job {job_id} is already claimed by another runner")
    job["status"] = "running"
    job["worker"] = (body.worker or "runner")[:60]
    job["started"] = time.time()
    job["progress"] = {"stage": "claimed", "done": 0, "total": 0, "note": ""}
    await _job_save(job)
    return {"ok": True, "job": _view(job)}


@app.post("/api/jobs/{job_id}/stage")
async def api_job_stage(job_id: str, body: StageIn, _=Depends(_authorized)):
    job = await _require_job(job_id)
    if job.get("status") != "running":
        raise HTTPException(409, f"job {job_id} is {job.get('status')}, not running")
    job["progress"] = {
        "stage": (body.stage or "working")[:40],
        "done": body.done if body.done is not None else 0,
        "total": body.total if body.total is not None else 0,
        "note": (body.note or "")[:160],
    }
    await _job_save(job)
    return {"ok": True}


@app.post("/api/jobs/{job_id}/done")
async def api_job_done(job_id: str, body: DoneIn, _=Depends(_authorized)):
    job = await _require_job(job_id)
    if job.get("status") not in ("running", "queued"):
        raise HTTPException(409, f"job {job_id} already finished")
    job["status"] = "done"
    job["result"] = body.result or {}
    job["finished"] = time.time()
    await _job_save(job)
    await _queue_remove(job_id)
    return {"ok": True}


@app.post("/api/jobs/{job_id}/fail")
async def api_job_fail(job_id: str, body: FailIn, _=Depends(_authorized)):
    job = await _require_job(job_id)
    if job.get("status") in ("done", "error"):
        return {"ok": True}                     # idempotent - late failure is fine
    job["status"] = "error"
    job["error"] = str(body.error or "the job failed on the runner")[:400]
    job["finished"] = time.time()
    await _job_save(job)
    await _queue_remove(job_id)
    return {"ok": True}


# ---- lyrics -------------------------------------------------------------

@app.get("/api/lyrics/search")
async def api_lyrics_search(q: str = "", _=Depends(_authorized)):
    return {"ok": True, "data": await lyrics_search(q)}


@app.get("/api/lyrics/get")
async def api_lyrics_get(
    artist: str = Query(..., min_length=1),
    track: str = Query(..., min_length=1),
    album: Optional[str] = Query(None),
    duration: Optional[int] = Query(None, ge=1, le=36000),
    prefer: str = Query("synced"),
    _=Depends(_authorized),
):
    data = await lyrics_get(artist, track, album, duration, prefer)
    return {"ok": True, "data": data}


# ----------------------------------------------------------------- errors
#
# everything json, never html, never a traceback. the cogs read
# data["error"] (ok:false) or ["detail"] - both shapes come out of this.

@app.exception_handler(HTTPException)
async def _http_error(_, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code,
                        content={"ok": False, "error": str(exc.detail)})


@app.exception_handler(RequestValidationError)
async def _validation_error(_, exc: RequestValidationError):
    try:
        first = exc.errors()[0]
        where = ".".join(str(p) for p in first.get("loc", [])[1:]) or "body"
        what = str(first.get("msg", "invalid"))
        note = f"{where}: {what}"
    except Exception:
        note = "invalid request body"
    return JSONResponse(status_code=422, content={"ok": False, "error": note})


@app.exception_handler(Exception)
async def _internal_error(_, exc: Exception):
    _logger.exception("hub crashed on a request (%s)", type(exc).__name__)
    return JSONResponse(status_code=500,
                        content={"ok": False,
                                 "error": "internal hub error - check the vercel logs"})
