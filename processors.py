"""job bodies: mono track/album, ffmpeg convert/trim (audio AND video), yt.

everything here is synchronous on purpose - one job per runner, no event
loop to babysit. proven bits are ported from the old leech-server
(_fetch_to with Range-resume, _quality_profile, _sanitize, the trim
seek math) and the colab worker (yt-dlp progress hook).

public-log hygiene: stage notes carry indexes and percents, never track
titles, urls or filenames - these logs are world-readable on a public
repo. error text that leaves for the hub is scrubbed by runner.py.
"""

from __future__ import annotations
import os
import json
import re
import shutil
import subprocess
import time
import zipfile
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import unquote, urlparse

import httpx

from tagging import cover_bytes, sidecar_text, tag_file
from uploaders import upload_file

MAX_TRACK_MB = 400
MAX_ALBUM_MB = 2500
LOOSE_FILES_CAP = 60       # v4: loose-file albums over this fall back to zip
MAX_INPUT_MB = 1500
MAX_YT_MB = 2000
FFMPEG_TIMEOUT_S = 2400
DISK_GUARD_MB = 6000

Stage = Callable[..., None]


class JobError(Exception):
    pass


# ------------------------------------------------------------ small utils

def _sanitize(name: str) -> str:
    clean = re.sub(r"[^\w\s\-.\[\]()']", "_", (name or "").strip())
    clean = re.sub(r"\s+", " ", clean).strip(" ._")[:120]
    return clean or "file"


def _fmt_dur(ms) -> str:
    seconds = max(0, int(ms or 0) // 1000)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}" if hours else f"{minutes}:{secs:02d}"


def _quality_profile(quality: str) -> tuple[Optional[list[str]], str]:
    """quality -> (ffmpeg codec args, ext). (None, 'flac') = passthrough."""
    q = (quality or "").strip().lower()
    if q in ("flac", "copy"):
        return None, "flac"
    if q == "wav":
        return ["-vn", "-c:a", "pcm_s16le"], "wav"
    if q == "m4a":
        return ["-vn", "-c:a", "aac", "-b:a", "256k"], "m4a"
    if q == "opus":
        return ["-vn", "-c:a", "libopus", "-b:a", "96k"], "opus"
    if q.isdigit() and 32 <= int(q) <= 320:
        return ["-vn", "-c:a", "libmp3lame", "-b:a", f"{q}k"], "mp3"
    raise JobError(f"quality must be flac, wav, m4a, opus or an mp3 kbps 32-320, "
                   f"not {quality!r}")


def _source_name(url: str) -> str:
    tail = unquote(urlparse(url).path.rsplit("/", 1)[-1] or "")
    stem = tail.rsplit(".", 1)[0] if "." in tail else tail
    return _sanitize(stem) or "source"


def _disk_free_mb() -> int:
    usage = shutil.disk_usage("/tmp")
    return usage.free // (1024 * 1024)


# ------------------------------------------------------------ fetch/ffmpeg

def fetch_to(url: str, dest: Path, *, max_mb: int, label: str) -> Path:
    """download url -> dest with cap, retries and Range resume (mono and
    cdns in general sometimes reset mid-file; we continue from disk)."""
    cap = max_mb * 1024 * 1024
    attempts = 3
    last = "unreachable"
    for attempt in range(1, attempts + 1):
        offset = dest.stat().st_size if dest.exists() else 0
        headers = {}
        if offset:
            headers["Range"] = f"bytes={offset}-"
        try:
            with httpx.Client(timeout=httpx.Timeout(60.0, connect=15.0),
                              follow_redirects=True) as client:
                with client.stream("GET", url, headers=headers) as resp:
                    mode = None
                    if resp.status_code == 206 and offset:
                        mode = "ab"
                    elif resp.status_code == 200:
                        offset, mode = 0, "wb"
                    elif resp.status_code == 416 and offset:
                        offset, mode = 0, "wb"
                    elif resp.status_code < 500 and resp.status_code != 429:
                        raise JobError(f"{label}: http {resp.status_code}")
                    if mode is not None:
                        clen = int(resp.headers.get("content-length") or 0)
                        if clen and offset + clen > cap:
                            raise JobError(
                                f"{label}: {(offset + clen) / 1e6:.0f} mb is over "
                                f"the {max_mb} mb cap")
                        written = offset
                        with dest.open(mode) as fh:
                            for chunk in resp.iter_bytes(512 * 1024):
                                written += len(chunk)
                                if written > cap:
                                    raise JobError(f"{label}: blew past the "
                                                   f"{max_mb} mb cap mid-download")
                                fh.write(chunk)
                        return dest
                    last = f"http {resp.status_code}"
        except JobError:
            raise
        except httpx.HTTPError as exc:
            last = type(exc).__name__
        if attempt < attempts:
            time.sleep(attempt)
    raise JobError(f"{label}: unreachable after {attempts} attempts ({last})")


def run_ffmpeg(args: list[str], timeout_s: int = FFMPEG_TIMEOUT_S) -> None:
    if not shutil.which("ffmpeg"):
        raise JobError("no ffmpeg on this runner")
    full = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            *[str(a) for a in args]]
    proc = subprocess.Popen(full, stdout=subprocess.DEVNULL,
                            stderr=subprocess.PIPE, text=True)
    try:
        _, stderr = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        raise JobError(f"ffmpeg timed out after {timeout_s}s")
    if proc.returncode != 0:
        tail = (stderr or "").strip()[-300:]
        raise JobError(f"ffmpeg failed: {tail or 'exit ' + str(proc.returncode)}")


def probe(path: Path) -> dict:
    """ffprobe the source: duration, has_video, size."""
    if not shutil.which("ffprobe"):
        return {"duration": 0.0, "has_video": False, "size": path.stat().st_size}
    out = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json",
         "-show_format", "-show_streams", str(path)],
        capture_output=True, text=True, timeout=60,
    )
    try:
        data = json.loads(out.stdout or "{}")
    except ValueError:
        data = {}
    duration = 0.0
    try:
        duration = float((data.get("format") or {}).get("duration") or 0)
    except (TypeError, ValueError):
        duration = 0.0
    has_video = any(
        (s.get("codec_type") == "video" and not s.get("disposition", {}).get("attached_pic"))
        for s in data.get("streams") or [] if isinstance(s, dict)
    )
    return {"duration": duration, "has_video": has_video,
            "size": path.stat().st_size}


# ------------------------------------------------------------ ffmpeg args

AUDIO_CODEC = {
    "mp3": lambda b: ["-vn", "-c:a", "libmp3lame", "-b:a", f"{b or 320}k"],
    "m4a": lambda b: ["-vn", "-c:a", "aac", "-b:a", f"{b or 256}k"],
    "wav": lambda b: ["-vn", "-c:a", "pcm_s16le"],
    "flac": lambda b: ["-vn", "-c:a", "flac"],
    "opus": lambda b: ["-vn", "-c:a", "libopus", "-b:a", f"{b or 96}k"],
}
VIDEO_FMTS = ("mp4", "mkv", "webm", "gif")


def ffmpeg_convert_args(payload: dict, src: Path, out_stem: Path,
                        src_suffix: str) -> tuple[list[str], str]:
    """build the full ffmpeg argv for convert/trim. everything validated
    again runner-side - the hub's word is not trusted blindly.
    `out_stem` is a path WITHOUT extension; the output file is always
    f"{out_stem}.{ext}" and the returned ext says which."""
    fmt = str(payload.get("format") or "mp3").strip().lower()
    bitrate = payload.get("bitrate")
    bitrate = int(bitrate) if isinstance(bitrate, int) and 32 <= bitrate <= 320 else None
    res = payload.get("res")
    res = int(res) if res in (240, 360, 480, 720, 1080, 1440, 2160) else None
    crf = payload.get("crf")
    crf = int(crf) if isinstance(crf, int) and 14 <= crf <= 34 else None
    fps = payload.get("fps")
    fps = int(fps) if isinstance(fps, int) and 1 <= fps <= 120 else None
    mute = bool(payload.get("mute"))
    start = payload.get("start")
    end = payload.get("end")
    trim = isinstance(start, (int, float)) and isinstance(end, (int, float)) \
        and float(end) > float(start)
    pre = ["-ss", f"{float(start):.3f}", "-t", f"{max(0.0, float(end) - float(start)):.3f}"] \
        if trim else []

    if fmt == "copy":
        ext = (src_suffix or "").lstrip(".").lower()
        if not re.match(r"^[a-z0-9]{1,5}$", ext or ""):
            raise JobError("stream copy needs a url that ends in a file extension")
        target = str(out_stem.with_suffix("." + ext))
        if trim:
            # a slice of EVERYTHING (video too, if the source has it)
            return [*pre, "-i", str(src), "-c", "copy", target], ext
        # convert-to-copy keeps old semantics: audio-only remux, instant
        return [*pre, "-i", str(src), "-vn", "-c:a", "copy", target], ext
    if fmt in AUDIO_CODEC:
        return ([*pre, "-i", str(src), *AUDIO_CODEC[fmt](bitrate),
                 str(out_stem.with_suffix("." + fmt))], fmt)

    if fmt == "gif":
        gfps = fps or 12
        gres = res or 480
        fc = (f"[0:v]fps={gfps},scale=-2:{gres}:flags=lanczos,"
              f"split[a][b];[a]palettegen[p];[b][p]paletteuse")
        return ([*pre, "-i", str(src), "-filter_complex", fc, "-an",
                 str(out_stem.with_suffix(".gif"))], "gif")

    vf = []
    if res:
        vf.append(f"scale=-2:{res}")
    if fps:
        vf.append(f"fps={fps}")
    vf_args = ["-vf", ",".join(vf)] if vf else []

    if fmt == "webm":
        v = ["-c:v", "libvpx-vp9", "-deadline", "realtime", "-cpu-used", "5",
             "-b:v", "0", "-crf", str(crf or 30)]
        a = ["-an"] if mute else ["-c:a", "libopus", "-b:a", "128k"]
        return ([*pre, "-i", str(src), *v, *vf_args, *a,
                 str(out_stem.with_suffix(".webm"))], "webm")

    if fmt not in ("mp4", "mkv"):
        raise JobError(f"unsupported format {fmt!r}")

    # mp4 / mkv
    v = ["-c:v", "libx264", "-preset", "veryfast", "-crf", str(crf or 23),
         "-pix_fmt", "yuv420p"]
    a = ["-an"] if mute else ["-c:a", "aac", "-b:a", "128k"]
    tail = ["-movflags", "+faststart"] if fmt == "mp4" else []
    return ([*pre, "-i", str(src), *v, *vf_args, *a, *tail,
             str(out_stem.with_suffix("." + fmt))], fmt)


# ---------------------------------------------------------------- lyrics

def _lyrics_for_track(hub, artists, title, album, duration_ms,
                      cfg: dict) -> dict:
    """never raises - a lyrics outage degrades the download, not kills it."""
    artist = ", ".join(artists) if isinstance(artists, list) else str(artists or "")
    if not artist or not title:
        return {"found": False, "reason": "no artist/title to search with"}
    try:
        return hub.lyrics_get(artist, title, album, duration_ms,
                              cfg.get("prefer") or "synced")
    except Exception:
        return {"found": False, "reason": "lyrics lookup failed"}


def _embed_text(lyrics: dict) -> Optional[str]:
    if not lyrics.get("found") or lyrics.get("instrumental"):
        return None
    return lyrics.get("plain") or lyrics.get("synced")


# ------------------------------------------------------------ mono jobs

def _mono_track(job: dict, hub, work: Path, stage: Stage) -> dict:
    p = job["payload"]
    meta = p.get("meta") or {}
    quality = str(p.get("quality") or "flac")
    codec, ext = _quality_profile(quality)
    lyrics_cfg = p.get("lyrics") or {"mode": "off", "prefer": "synced"}

    title = (meta.get("title") or "").strip() or f"track {p.get('track_id')}"
    artists = [str(a) for a in (meta.get("artists") or [])][:4]
    base = _sanitize(f"{', '.join(artists)} - {title}") if artists else _sanitize(title)

    stage("download", 0, 3, "track")
    raw = work / "raw.flac"
    fetch_to(str(p.get("url") or ""), raw, max_mb=MAX_TRACK_MB, label="track")

    lyrics: dict = {"found": False}
    if lyrics_cfg.get("mode") != "off":
        stage("lyrics", 1, 3, "")
        lyrics = _lyrics_for_track(hub, artists, title, meta.get("album"),
                                   meta.get("duration"), lyrics_cfg)
    cover = cover_bytes(meta.get("artwork"))

    if codec is None:
        out = work / f"{base}.flac"
        if out.name != raw.name:
            raw.rename(out)
    else:
        stage("convert", 1, 3, ext)
        out = work / f"{base}.{ext}"
        run_ffmpeg(["-i", str(raw), *codec, str(out)])
    tag_file(out, {"title": title, "artists": artists,
                   "album": meta.get("album"), "year": meta.get("year"),
                   "position": meta.get("position")},
             cover, _embed_text(lyrics) if lyrics_cfg.get("mode") == "embed" else None)

    stage("upload", 2, 3, "")
    url, host = upload_file(str(out))
    result = {
        "kind": "track", "url": url, "host": host,
        "filename": out.name, "format": ext, "quality": quality,
        "size": out.stat().st_size,
        "title": title or None, "artists": artists,
        "artwork": meta.get("artwork"), "duration": meta.get("duration"),
        "track_id": p.get("track_id"),
    }
    if lyrics_cfg.get("mode") == "embed":
        result["lyrics_embedded"] = bool(_embed_text(lyrics))
    if lyrics_cfg.get("mode") == "sidecar":
        text, sc_ext = sidecar_text(lyrics.get("synced"), lyrics.get("plain"),
                                    lyrics_cfg.get("prefer") or "synced")
        if text:
            sc = work / f"{base}.{sc_ext}"
            sc.write_text(text, encoding="utf-8")
            stage("upload", 3, 3, "lyrics")
            try:
                lurl, _ = upload_file(sc)
                result["lyrics_url"] = lurl
                result["lyrics_synced"] = sc_ext == "lrc"
            except Exception:
                pass
    return result

def _mono_album(job: dict, hub, work: Path, stage: Stage) -> dict:
    p = job["payload"]
    meta = p.get("meta") or {}
    tracks = p.get("tracks") or []
    if not tracks:
        raise JobError("the album job arrived with no tracks")
    quality = str(p.get("quality") or "flac")
    codec, ext = _quality_profile(quality)
    lyrics_cfg = p.get("lyrics") or {"mode": "off", "prefer": "synced"}
    zip_wanted = bool(p.get("zip", True))     # v4: False = loose files

    title = (meta.get("title") or "").strip() or f"album {p.get('release_id')}"
    artists = [str(a) for a in (meta.get("artists") or [])][:4]
    folder = _sanitize(f"{', '.join(artists)} - {title}")
    out_dir = work / "album" / folder
    out_dir.mkdir(parents=True, exist_ok=True)

    if _disk_free_mb() < 2000:
        raise JobError("runner is low on disk")

    cover = cover_bytes(meta.get("artwork"))
    if cover:
        (out_dir / "cover.png").write_bytes(cover)
    lyrics_found = 0

    def _desc(track: dict, index: int) -> dict:
        try:
            position = int(track.get("position") or index + 1)
        except (TypeError, ValueError):
            position = index + 1
        return {"position": position,
                "title": (track.get("title") or f"track {position}").strip()}

    def _one(index: int, track: dict) -> None:
        """one track end-to-end. anything it raises gets one retry, then a
        skip + a report entry - a single bad track never kills the album."""
        nonlocal lyrics_found
        stage("download", index, len(tracks), f"{index + 1}/{len(tracks)}")
        raw = work / f"{index:02d}.flac"
        fetch_to(str(track.get("url") or ""), raw, max_mb=MAX_TRACK_MB,
                 label=f"track {index + 1}")
        info = _desc(track, index)
        t_title = info["title"] or f"track {info['position']}"
        stem = _sanitize(f"{info['position']:02d}. {t_title}")

        lyrics: dict = {"found": False}
        if lyrics_cfg.get("mode") != "off":
            t_artists = track.get("artists") or artists
            lyrics = _lyrics_for_track(hub, t_artists, t_title, meta.get("title"),
                                       track.get("duration"), lyrics_cfg)
            if lyrics.get("found"):
                lyrics_found += 1

        if codec is None:
            dest = out_dir / f"{stem}.flac"
            dest.unlink(missing_ok=True)
            raw.rename(dest)
        else:
            stage("convert", index, len(tracks), f"{index + 1}/{len(tracks)}")
            dest = out_dir / f"{stem}.{ext}"
            run_ffmpeg(["-i", str(raw), *codec, str(dest)])
            raw.unlink(missing_ok=True)
        tag_file(dest, {"title": t_title,
                        "artists": track.get("artists") or artists,
                        "album": title, "year": meta.get("year"),
                        "position": info["position"]},
                 cover, _embed_text(lyrics) if lyrics_cfg.get("mode") == "embed"
                 else None)
        if lyrics_cfg.get("mode") == "sidecar":
            text, sc_ext = sidecar_text(lyrics.get("synced"), lyrics.get("plain"),
                                        lyrics_cfg.get("prefer") or "synced")
            if text:
                (out_dir / f"{stem}.{sc_ext}").write_text(text, encoding="utf-8")

    failed: list[dict] = []
    ok = 0
    pending = list(enumerate(tracks))
    for attempt in (1, 2):              # v4: one extra pass over the failures
        still: list[tuple[int, dict]] = []
        for index, track in pending:
            try:
                _one(index, track)
                ok += 1
            except Exception as exc:    # noqa: BLE001 - skip, don't die
                if attempt == 2:
                    reason = str(exc) if isinstance(exc, JobError) \
                        else f"runner error: {type(exc).__name__}"
                    failed.append({**_desc(track, index), "error": reason[:160]})
                    print(f"track {index + 1} failed twice - skipped")
                else:
                    still.append((index, track))
            grown = sum(f.stat().st_size for f in out_dir.rglob("*")
                        if f.is_file())
            if grown > MAX_ALBUM_MB * 1024 * 1024:
                raise JobError(f"album passed {MAX_ALBUM_MB} mb on disk - "
                               f"stopped after track {index + 1}/{len(tracks)} "
                               "- try a lower quality")
        pending = still
        if not pending:
            break
        stage("download", 0, len(tracks), f"retrying {len(pending)}")
    if ok == 0:
        raise JobError(f"all {len(tracks)} tracks failed - mono might be down "
                       "or the release is unreachable")

    items = sorted((f for f in out_dir.rglob("*") if f.is_file()),
                   key=lambda f: f.name)
    result = {
        "kind": "album", "quality": quality,
        "release_id": p.get("release_id"), "title": title, "artists": artists,
        "artwork": meta.get("artwork"), "year": meta.get("year"),
        "type": meta.get("type"), "track_count": len(tracks),
        "lyrics_found": lyrics_found,
        "tracks": [{"id": t.get("id"), "title": t.get("title"),
                    "artists": t.get("artists"), "duration": t.get("duration")}
                   for t in tracks],
    }
    if failed:
        result["failed"] = failed       # the cog alerts on these

    if zip_wanted or len(items) > LOOSE_FILES_CAP:
        if not zip_wanted:
            result["zip_fallback"] = (f"{len(items)} files is over the loose "
                                      f"cap ({LOOSE_FILES_CAP}) - zipped instead")
        stage("zip", ok, len(tracks), f"{len(items)} files")
        zip_name = _sanitize(f"{', '.join(artists)} - {title}") or "album"
        zip_path = work / f"{zip_name}.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as zf:
            for item in items:
                zf.write(item, item.relative_to(out_dir.parent))
        stage("upload", ok, len(tracks), "zip")
        url, host = upload_file(zip_path)
        result.update({
            "zip": True, "url": url, "host": host,
            "filename": zip_path.name, "size": zip_path.stat().st_size,
        })
        return result

    # loose mode: sidecars upload first (they're small) so the music lines
    # can link them; then every file on its own. one upload failing skips
    # that file into `failed`, not the whole job.
    sidecar_urls: dict[str, str] = {}
    stage("upload", 0, len(items), f"{len(items)} files")
    for item in items:
        if item.suffix.lower() in (".lrc", ".txt"):
            try:
                surl, _ = upload_file(item)
                sidecar_urls[item.stem] = surl
            except Exception:  # noqa: BLE001
                pass
    files: list[dict] = []
    for i, item in enumerate(items):
        if item.suffix.lower() in (".lrc", ".txt"):
            continue
        stage("upload", i, len(items), f"{i + 1}/{len(items)}")
        try:
            furl, fhost = upload_file(item)
        except Exception as exc:  # noqa: BLE001 - skip, don't die
            try:
                upos = int(item.stem.split(".", 1)[0])
            except ValueError:
                upos = 0
            failed.append({"position": upos, "title": item.stem,
                           "error": f"upload failed: {type(exc).__name__}"})
            result["failed"] = failed
            continue
        entry = {"name": item.name, "url": furl, "host": fhost,
                 "size": item.stat().st_size}
        if item.stem in sidecar_urls:
            entry["lyrics_url"] = sidecar_urls[item.stem]
        files.append(entry)
    if not files:
        raise JobError("every file upload failed - the store chain is down")
    result.update({"zip": False, "files": files,
                   "url": None, "host": None, "filename": None,
                   "size": sum(f["size"] for f in files)})
    return result

# ------------------------------------------------------------ ffmpeg job

def _process_ffmpeg(job: dict, hub, work: Path, stage: Stage) -> dict:
    p = job["payload"]
    url = str(p.get("url") or "")
    fmt = str(p.get("format") or "mp3").strip().lower()

    stage("fetch", 0, 3, "source")
    suffix = Path(urlparse(url).path).suffix
    suffix = suffix if re.match(r"^\.[A-Za-z0-9]{1,5}$", suffix or "") else ""
    src = work / f"raw{suffix}"
    fetch_to(url, src, max_mb=MAX_INPUT_MB, label="source")

    info = probe(src)
    stage("convert", 1, 3, fmt)
    out_stem = work / "out"
    args, ext = ffmpeg_convert_args(p, src, out_stem, suffix)
    out = work / f"out.{ext}"
    run_ffmpeg(args)
    if not out.exists() or out.stat().st_size == 0:
        raise JobError("ffmpeg produced nothing (wrong kind of input for that format?)")

    name = _source_name(url)
    start, end = p.get("start"), p.get("end")
    if start is not None and end is not None:
        name = f"{name} trim {_fmt_dur(int(float(start) * 1000))}-{_fmt_dur(int(float(end) * 1000))}"
    final = work / f"{_sanitize(name)}.{ext}"
    out.rename(final)

    stage("upload", 2, 3, "")
    cdn, host = upload_file(final)
    return {
        "kind": "ffmpeg", "url": cdn, "host": host, "filename": final.name,
        "format": fmt, "bitrate": p.get("bitrate"), "size": final.stat().st_size,
        "duration": info.get("duration") or None,
        "start": start, "end": end,
        "video": fmt in VIDEO_FMTS or None,
    }


# ------------------------------------------------------------ yt job

def _process_yt(job: dict, hub, work: Path, stage: Stage) -> dict:
    try:
        import yt_dlp
    except ImportError:
        raise JobError("yt-dlp is not installed on this runner")
    p = job["payload"]
    url = str(p.get("url") or "")
    audio = bool(p.get("audio"))
    quality = p.get("quality")
    quality = int(quality) if isinstance(quality, int) and 144 <= quality <= 2160 else 720
    max_mb = p.get("max_mb")
    max_mb = float(max_mb) if isinstance(max_mb, (int, float)) and 1 <= float(max_mb) <= MAX_YT_MB else float(MAX_YT_MB)
    start, end = p.get("start"), p.get("end")
    clip = isinstance(start, (int, float)) and isinstance(end, (int, float)) and float(end) > float(start)

    # v3: per-job cookies from the payload (sent by >ytdlp cookies). the
    # work dir is wiped after the job, so nothing lingers on disk.
    cookie_path = None
    raw_cookies = p.get("cookies")
    if isinstance(raw_cookies, str) and raw_cookies.strip():
        cookie_path = work / "cookies.txt"
        cookie_path.write_text(raw_cookies if raw_cookies.endswith("\n")
                               else raw_cookies + "\n", encoding="utf-8")

    def _blocked(exc: Exception) -> bool:
        text = str(exc).lower()
        return any(mark in text for mark in (
            "sign in to confirm", "not a bot", "confirm you're not",
            "confirm your age", "age-restricted", "login required"))

    stage("probe", 0, 4, "")
    probe_opts: dict = {"quiet": True, "no_warnings": True,
                        "noplaylist": True, "skip_download": True}
    if cookie_path:
        probe_opts["cookiefile"] = str(cookie_path)
    try:
        with yt_dlp.YoutubeDL(probe_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as exc:
        if _blocked(exc):
            raise JobError("youtube wants a login on this runner's ip "
                           "(bot check) - upload cookies with >ytdlp cookies")
        raise JobError("yt-dlp couldn't read that link (private, dead, or unsupported)")
    duration = int(info.get("duration") or 0)
    if duration > 4 * 3600:
        raise JobError("that's over 4 hours - too long for this lane")
    if clip:
        duration = int(min(duration or 1e9, float(end) - float(start)))

    stage("download", 1, 4, "")
    last_note: dict = {"t": 0.0}

    def hook(d: dict) -> None:
        if d.get("status") != "downloading":
            return
        now = time.time()
        if now - last_note["t"] >= 5:
            last_note["t"] = now
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            done = d.get("downloaded_bytes") or 0
            stage("download", 1, 4,
                  f"{int(done * 100 / total)}%" if total else "downloading")

    opts: dict = {
        "outtmpl": str(work / "src.%(ext)s"),
        "noplaylist": True, "quiet": True, "no_warnings": True,
        "progress_hooks": [hook],
        "max_filesize": int(max_mb * 1024 * 1024),
    }
    if cookie_path:
        opts["cookiefile"] = str(cookie_path)
    if clip:
        # yt-dlp downloads only the slice (needs a reencode-friendly cut,
        # hence force_keyframes_at_cuts so the boundaries actually land)
        opts["download_ranges"] = yt_dlp.utils.download_range_func(
            None, float(start), float(end))
        opts["force_keyframes_at_cuts"] = True
    if audio:
        opts["format"] = "bestaudio/best"
        opts["postprocessors"] = [
            {"key": "FFmpegExtractAudio", "preferredcodec": "mp3",
             "preferredquality": "192"},
        ]
    else:
        opts["format"] = f"bv*[height<={quality}]+ba/b[height<={quality}]"
        opts["merge_output_format"] = "mp4"
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])
    except JobError:
        raise
    except Exception as exc:
        if _blocked(exc):
            raise JobError("youtube wants a login on this runner's ip "
                           "(bot check) - upload cookies with >ytdlp cookies")
        raise JobError("yt-dlp couldn't download that link (over the size "
                       "cap, private, or unsupported)")
    files = [f for f in work.iterdir() if f.name.startswith("src.")]
    if not files:
        raise JobError("yt-dlp produced nothing (over the size cap, private, or unsupported)")
    final = max(files, key=lambda f: f.stat().st_size)

    if final.stat().st_size > max_mb * 1024 * 1024:
        if audio:
            raise JobError(f"the audio is {final.stat().st_size / 1e6:.0f} mb - "
                           f"over the {max_mb:.0f} mb cap")
        stage("convert", 2, 4, "shrinking")
        shrunk = work / "shrunk.mp4"
        run_ffmpeg(["-i", str(final), "-vf", "scale=-2:480",
                    "-c:v", "libx264", "-preset", "veryfast", "-crf", "28",
                    "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart",
                    str(shrunk)])
        if shrunk.stat().st_size > max_mb * 1024 * 1024:
            raise JobError(f"still {shrunk.stat().st_size / 1e6:.0f} mb after one "
                           f"shrink pass - try a lower quality or max_mb")
        final.unlink(missing_ok=True)
        final = shrunk

    stage("upload", 3, 4, "")
    cdn, host = upload_file(str(final))
    return {
        "kind": "yt", "url": cdn, "host": host, "filename": final.name,
        "format": final.suffix.lstrip(".") or ("mp3" if audio else "mp4"),
        "size": final.stat().st_size,
        "size_mb": round(final.stat().st_size / 1e6, 1),
        "duration": duration or None, "title": str(info.get("title") or None),
        "webpage_url": str(info.get("webpage_url") or url),
        "uploader": (info.get("uploader") or info.get("channel") or None),
        "audio": audio or None,
        "thumbnail": str(info.get("thumbnail") or None),
    }

# ------------------------------------------------------------ dispatcher

_HANDLERS = {
    "mono_track": _mono_track,
    "mono_album": _mono_album,
    "ffmpeg_convert": _process_ffmpeg,
    "ffmpeg_trim": _process_ffmpeg,
    "yt": _process_yt,
}


def process(job: dict, hub, work: Path, stage: Stage) -> dict:
    handler = _HANDLERS.get(job.get("kind"))
    if handler is None:
        raise JobError(f"unknown job kind {job.get('kind')!r}")
    return handler(job, hub, work, stage)
