"""best-effort tagging with mutagen: title/artists/album/year/track no,
embedded cover art, embedded lyrics. never raises - a tag failure must
not kill a finished conversion. sidecar helpers pick .lrc vs .txt.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional


def sidecar_text(synced: Optional[str], plain: Optional[str],
                 prefer: str = "synced") -> tuple[Optional[str], str]:
    """-> (text, ext) for a lyrics sidecar file, or (None, '').
    prefer synced: .lrc when synced exists (timestamps kept), else .txt.
    prefer plain: .txt always (timestamps stripped)."""
    prefer = (prefer or "synced").lower()
    if prefer == "synced" and synced:
        return synced, "lrc"
    text = plain
    if text is None and synced:
        text = _strip_tags(synced)
    if text:
        return text, "txt"
    if synced and prefer != "plain":
        return synced, "lrc"
    return None, ""


def _strip_tags(synced: str) -> Optional[str]:
    import re

    lines = []
    for line in str(synced or "").splitlines():
        clean = re.sub(r"^\[[0-9:.]+\]\s*", "", line).strip()
        if clean:
            lines.append(clean)
    return "\n".join(lines) or None


def tag_file(path: Path, meta: dict, cover_bytes: Optional[bytes] = None,
             lyrics_text: Optional[str] = None) -> bool:
    """write tags for whatever container `path` is. returns True if the
    main tags landed. all failures are swallowed (best effort)."""
    try:
        return _tag(str(path), meta, cover_bytes, lyrics_text)
    except Exception:
        return False


def _tag(path: str, meta: dict, cover: Optional[bytes], lyrics: Optional[str]) -> bool:
    title = (meta.get("title") or "").strip()
    artists = [str(a) for a in (meta.get("artists") or [])][:4]
    album = (meta.get("album") or "").strip() or None
    year = str(meta.get("year") or "")[:4] or None
    track_no = meta.get("position") or meta.get("track") or None

    lower = path.lower()
    if lower.endswith(".flac"):
        from mutagen.flac import FLAC, Picture

        audio = FLAC(path)
        if title:
            audio["title"] = title
        if artists:
            audio["artist"] = ", ".join(artists)
        if album:
            audio["album"] = album
        if year:
            audio["date"] = year
        if track_no:
            audio["tracknumber"] = str(track_no)
        if lyrics:
            audio["lyrics"] = lyrics
        if cover:
            pic = Picture()
            pic.type = 3
            pic.mime = "image/jpeg" if cover[:3] == b"\xff\xd8\xff" else "image/png"
            pic.desc = "cover"
            pic.data = cover
            audio.clear_pictures()
            audio.add_picture(pic)
        audio.save()
        return True

    if lower.endswith((".mp3", ".wav")):
        from mutagen.id3 import ID3, TIT2, TPE1, TALB, TDRC, TRCK, USLT, APIC

        tags = ID3(path)
        if title:
            tags.add(TIT2(encoding=3, text=title))
        if artists:
            tags.add(TPE1(encoding=3, text=", ".join(artists)))
        if album:
            tags.add(TALB(encoding=3, text=album))
        if year:
            tags.add(TDRC(encoding=3, text=year))
        if track_no:
            tags.add(TRCK(encoding=3, text=str(track_no)))
        if lyrics:
            tags.add(USLT(encoding=3, lang="eng", desc="", text=lyrics))
        if cover:
            mime = "image/jpeg" if cover[:3] == b"\xff\xd8\xff" else "image/png"
            tags.add(APIC(encoding=3, mime=mime, type=3, desc="cover", data=cover))
        tags.save(path)
        return True

    if lower.endswith(".m4a") or lower.endswith(".mp4"):
        from mutagen.mp4 import MP4, MP4Cover

        audio = MP4(path)
        if title:
            audio["\xa9nam"] = [title]
        if artists:
            audio["\xa9ART"] = [", ".join(artists)]
        if album:
            audio["\xa9alb"] = [album]
        if year:
            audio["\xa9day"] = [year]
        if track_no:
            audio["trkn"] = [(int(track_no), 0)]
        if lyrics:
            audio["\xa9lyr"] = [lyrics]
        if cover:
            audio["covr"] = [MP4Cover(cover)]
        audio.save()
        return True

    if lower.endswith((".opus", ".ogg")):
        if lower.endswith(".opus"):
            from mutagen.oggopus import OggOpus as OggFile
        else:
            from mutagen.oggvorbis import OggVorbis as OggFile

        audio = OggFile(path)
        if title:
            audio["title"] = title
        if artists:
            audio["artist"] = ", ".join(artists)
        if album:
            audio["album"] = album
        if year:
            audio["date"] = year
        if track_no:
            audio["tracknumber"] = str(track_no)
        if lyrics:
            audio["lyrics"] = lyrics
        if cover:
            try:
                from mutagen.flac import Picture

                pic = Picture()
                pic.type = 3
                pic.mime = "image/jpeg" if cover[:3] == b"\xff\xd8\xff" else "image/png"
                pic.data = cover
                audio.clear_pictures()
                audio.add_picture(pic)
            except Exception:
                pass                    # opus picture support varies by mutagen
        audio.save()
        return True

    return False


def cover_bytes(artwork_url: str | None, cap_mb: int = 12) -> Optional[bytes]:
    """download album art once (caller caches per job)."""
    if not artwork_url:
        return None
    import httpx

    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            resp = client.get(artwork_url)
        if resp.status_code == 200 and len(resp.content) <= cap_mb * 1024 * 1024:
            return resp.content
    except httpx.HTTPError:
        pass
    return None
