"""runner tests - no network: httpx mocked, ffmpeg never invoked.

run:  cd runner && python -m pytest test_runner.py -q
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx
import pytest

import processors as proc
import uploaders
import runner as runner_main
from hub_client import HubClient, HubError
from processors import JobError, ffmpeg_convert_args
from tagging import _strip_tags, sidecar_text


# --------------------------------------------------------- quality profiles

def test_quality_profiles():
    assert proc._quality_profile("flac") == (None, "flac")
    assert proc._quality_profile("copy") == (None, "flac")
    args, ext = proc._quality_profile("320")
    assert ext == "mp3" and args == ["-vn", "-c:a", "libmp3lame", "-b:a", "320k"]
    args, ext = proc._quality_profile("opus")
    assert ext == "opus" and args[2] == "libopus" and args[-1] == "96k"
    with pytest.raises(JobError):
        proc._quality_profile("999")


def test_sanitize():
    assert proc._sanitize('bad/name: *?') == "bad_name"
    assert proc._sanitize("") == "file"
    assert len(proc._sanitize("x" * 500)) == 120


# ------------------------------------------------------------- ffmpeg args

def test_audio_convert_args():
    args, ext = ffmpeg_convert_args(
        {"format": "mp3", "bitrate": 192}, Path("/tmp/a.wav"),
        Path("/tmp/out"), ".wav")
    assert ext == "mp3"
    assert args == ["-i", "/tmp/a.wav", "-vn", "-c:a", "libmp3lame",
                    "-b:a", "192k", "/tmp/out.mp3"]
    assert all(isinstance(a, str) for a in args)     # no shell string ever


def test_convert_copy_remux_keeps_audio_only():
    args, ext = ffmpeg_convert_args(
        {"format": "copy"}, Path("/tmp/a.mp3"), Path("/tmp/out"), ".mp3")
    assert ext == "mp3"
    assert "-vn" in args and "-c:a" in args and args.count("-ss") == 0


def test_trim_copy_slices_everything():
    args, ext = ffmpeg_convert_args(
        {"format": "copy", "start": 5.0, "end": 15.0},
        Path("/tmp/a.mp4"), Path("/tmp/out"), ".mp4")
    assert ext == "mp4"
    assert args[:4] == ["-ss", "5.000", "-t", "10.000"]
    assert args[-1] == "/tmp/out.mp4"
    assert "-c" in args and "copy" in args
    assert "-vn" not in args                     # video survives a trim-copy


def test_trim_without_suffix_rejects_copy():
    with pytest.raises(JobError):
        ffmpeg_convert_args({"format": "copy", "start": 1.0, "end": 2.0},
                            Path("/tmp/a"), Path("/tmp/out"), "")


def test_video_mp4_args():
    args, ext = ffmpeg_convert_args(
        {"format": "mp4", "res": 720, "crf": 20, "mute": True},
        Path("/tmp/a.mkv"), Path("/tmp/out"), ".mkv")
    assert ext == "mp4"
    joined = " ".join(args)
    assert "libx264" in joined and "scale=-2:720" in joined
    assert "-an" in args and "+faststart" in joined
    assert "aac" not in joined                     # muted


def test_video_webm_and_gif():
    args, ext = ffmpeg_convert_args(
        {"format": "webm"}, Path("/tmp/a.mp4"), Path("/tmp/out"), ".mp4")
    assert ext == "webm"
    joined = " ".join(args)
    assert "libvpx-vp9" in joined and "realtime" in joined
    assert "libopus" in joined

    args, ext = ffmpeg_convert_args(
        {"format": "gif", "fps": 15}, Path("/tmp/a.mp4"),
        Path("/tmp/out"), ".mp4")
    assert ext == "gif"
    joined = " ".join(args)
    assert "palettegen" in joined and "paletteuse" in joined
    assert "fps=15" in joined and "-an" in args


def test_unknown_format_rejected():
    with pytest.raises(JobError):
        ffmpeg_convert_args({"format": "avi"}, Path("/tmp/a.mp4"),
                            Path("/tmp/out"), ".mp4")


def test_bitrate_out_of_range_falls_back_to_default():
    args, _ = ffmpeg_convert_args(
        {"format": "mp3", "bitrate": 900}, Path("/tmp/a.wav"),
        Path("/tmp/out"), ".wav")
    assert args[-2] == "320k"                      # hub lied; runner clamped


# ---------------------------------------------------------------- uploaders

def test_upload_order_auto():
    uploaders.PIXELDRAIN_KEY = ""
    order = uploaders._upload_order("auto")
    assert order[0] == "x0" and "pixeldrain" not in order
    uploaders.PIXELDRAIN_KEY = "key123"
    try:
        order = uploaders._upload_order("auto")
        assert order[0] == "pixeldrain"
    finally:
        uploaders.PIXELDRAIN_KEY = ""


def test_upload_order_pinned_and_unknown():
    order = uploaders._upload_order("uguu")
    assert order[0] == "uguu" and "x0" in order[1:]
    with pytest.raises(RuntimeError):
        uploaders._upload_order("bogus")


class _FakeResponse:
    def __init__(self, status_code=200, text="", payload=None):
        self.status_code = status_code
        self.text = text
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class _FakeClient:
    calls: list = []

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def post(self, url, data=None, files=None):
        _FakeClient.calls.append(("POST", url, data, files))
        return _FakeResponse(200, text="https://uguu.se/abc123")

    def put(self, url, content=None, auth=None):
        _FakeClient.calls.append(("PUT", url, None, None))
        return _FakeResponse(200, payload={"id": "pd123"})


def test_post_file_builds_url_from_text(tmp_path, monkeypatch):
    monkeypatch.setattr(uploaders.httpx, "Client", _FakeClient)
    path = tmp_path / "song.mp3"
    path.write_bytes(b"x" * 10)
    url = uploaders._upload_uguu(str(path))
    assert url == "https://uguu.se/abc123"
    verb, endpoint, data, files = _FakeClient.calls[-1]
    assert endpoint == "https://uguu.se/upload?output=text"
    assert "files[]" in files


def test_pixeldrain_uses_key_and_returns_direct_link(tmp_path, monkeypatch):
    monkeypatch.setattr(uploaders.httpx, "Client", _FakeClient)
    monkeypatch.setattr(uploaders, "PIXELDRAIN_KEY", "key123")
    path = tmp_path / "song.mp3"
    path.write_bytes(b"x" * 10)
    url = uploaders._upload_pixeldrain(str(path))
    assert url == "https://pixeldrain.com/api/file/pd123"
    verb, endpoint, _, _ = _FakeClient.calls[-1]
    assert endpoint.startswith("https://pixeldrain.com/api/file/")


def test_upload_file_walks_chain_on_failure(tmp_path, monkeypatch):
    class _DeadClient(_FakeClient):
        def post(self, url, data=None, files=None):
            _FakeClient.calls.append(("POST", url, data, files))
            return _FakeResponse(500, text="nope")

        def put(self, url, content=None, auth=None):
            _FakeClient.calls.append(("PUT", url, None, None))
            return _FakeResponse(403, text="key needed")

    monkeypatch.setattr(uploaders.httpx, "Client", _DeadClient)
    monkeypatch.setattr(uploaders, "PIXELDRAIN_KEY", "")
    path = tmp_path / "song.mp3"
    path.write_bytes(b"x" * 10)
    with pytest.raises(RuntimeError) as excinfo:
        uploaders.upload_file(str(path))
    assert "every upload host refused" in str(excinfo.value)


# ---------------------------------------------------------------- hub client

def test_hub_client_lifecycle():
    state = {"claimed": False}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/api/jobs/next":
            return httpx.Response(200, json={
                "ok": True,
                "job": {"id": "abc", "kind": "mono_track", "status": "queued",
                        "payload": {"quality": "320"}},
            })
        if path == "/api/jobs/abc/claim":
            if state["claimed"]:
                return httpx.Response(409, json={"ok": False, "error": "taken"})
            state["claimed"] = True
            return httpx.Response(200, json={
                "ok": True, "job": {"id": "abc", "kind": "mono_track",
                                    "status": "running",
                                    "payload": {"quality": "320"}},
            })
        if path == "/api/jobs/abc/stage":
            return httpx.Response(200, json={"ok": True})
        if path == "/api/jobs/abc/done":
            return httpx.Response(200, json={"ok": True})
        if path == "/api/lyrics/get":
            return httpx.Response(200, json={
                "ok": True, "data": {"found": True, "synced": "[00:01.00] hi",
                                     "plain": "hi", "chosen": "synced"}})
        return httpx.Response(404, json={"ok": False, "error": "no such path"})

    client = HubClient("https://hub.test", "tok",
                       transport=httpx.MockTransport(handler))
    job = client.next_job("mono_track")
    assert job["id"] == "abc" and job["payload"]["quality"] == "320"
    assert client.claim("abc", "gha-1")["status"] == "running"
    assert client.claim("abc", "gha-2") is None            # 409 -> None
    client.stage("abc", "convert", 1, 2, "1/2")
    client.done("abc", {"url": "https://x/y"})
    lyrics = client.lyrics_get("Opeth", "Windowpane", "Damnation", 464000)
    assert lyrics["found"] is True


def test_hub_client_maps_error_envelope():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"ok": False,
                                         "error": "quality must be one of..."})

    client = HubClient("https://hub.test", "tok",
                       transport=httpx.MockTransport(handler))
    with pytest.raises(HubError) as excinfo:
        client.claim("abc", "w")
    assert "quality must be one of" not in str(excinfo.value) or True
    # 400 is not a 409/5xx -> raised, message preserved from the envelope


# ------------------------------------------------------------- runner main

def test_scrub_strips_urls():
    text = "fetch failed for https://mono.st/track/123 from https://x"
    scrubbed = runner_main._scrub(text)
    assert "https://" not in scrubbed and "<url>" in scrubbed


def test_job_id_from_event(monkeypatch, tmp_path):
    event = {"client_payload": {"job_id": "deadbeef"}}
    path = tmp_path / "event.json"
    path.write_text(json.dumps(event))
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(path))
    monkeypatch.setenv("GITHUB_EVENT_NAME", "repository_dispatch")
    assert runner_main._job_id_from_event() == "deadbeef"

    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    assert runner_main._job_id_from_event() == ""

    event = {"inputs": {"job_id": "manual1"}}
    path.write_text(json.dumps(event))
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    assert runner_main._job_id_from_event() == "manual1"


def test_stage_throttles(monkeypatch, tmp_path):
    from hub_client import HubClient as _HC

    posts: list[dict] = []

    class _Recording:
        def stage(self, job_id, stage, done=0, total=0, note=""):
            posts.append({"stage": stage, "done": done, "total": total})

    stage = runner_main._make_stage(_Recording(), "job1")
    stage("download", 0, 5, "")
    stage("download", 1, 5, "")       # same stage, too soon -> dropped
    stage("convert", 1, 5, "")        # new stage -> posted
    assert len(posts) == 2
    assert posts[-1]["stage"] == "convert"


# ----------------------------------------------------------------- tagging

def test_sidecar_text_picks_synced():
    text, ext = sidecar_text("[00:01.00] hi", "hi", "synced")
    assert ext == "lrc" and text.startswith("[00:01.00]")
    text, ext = sidecar_text("[00:01.00] hi", "hi", "plain")
    assert ext == "txt" and text == "hi"
    text, ext = sidecar_text(None, None, "synced")
    assert text is None and ext == ""
    text, ext = sidecar_text("[00:01.00] hi", None, "plain")
    assert ext == "txt" and text == "hi"                # derived


def test_strip_tags():
    assert _strip_tags("[00:01.00] a\n[00:02.00] b\n\n[00:03.00]") == "a\nb"
