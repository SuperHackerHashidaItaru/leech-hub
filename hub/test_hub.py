"""hub tests - no network: mono/lrclib/github-dispatch/redis all faked.

run:  cd hub && python -m pytest test_hub.py -q
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault("TOKEN", "testtoken")

import pytest
from fastapi.testclient import TestClient

import api.index as hub

HEADERS = {"x-leech-token": "testtoken"}


# ----------------------------------------------------------------- fakes

class FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}
        self.lists: dict[str, list[str]] = {}

    async def run(self, *cmd):
        name = str(cmd[0]).upper()
        args = [str(c) for c in cmd[1:]]
        if name == "GET":
            return self.store.get(args[0])
        if name == "SET":
            key, value = args[0], args[1]
            rest = [a.upper() for a in args[2:]]
            if "NX" in rest and key in self.store:
                return None
            self.store[key] = value
            return "OK"
        if name == "LPUSH":
            bucket = self.lists.setdefault(args[0], [])
            bucket.insert(0, args[1])
            return len(bucket)
        if name == "LRANGE":
            bucket = self.lists.get(args[0], [])
            start = int(args[1]) if int(args[1]) >= 0 else max(0, len(bucket) + int(args[1]))
            stop = len(bucket) if args[2] == "-1" else int(args[2]) + 1
            return bucket[start:stop]
        if name == "LREM":
            bucket = self.lists.get(args[0], [])
            value = args[2]
            while value in bucket:
                bucket.remove(value)
            return 1
        if name == "LLEN":
            return len(self.lists.get(args[0], []))
        if name == "DEL":
            return int(args[0] in self.store) + int(
                self.lists.pop(args[0], []) is not None)
        raise AssertionError(f"FakeRedis got an unexpected command: {name}")


class State:
    def __init__(self):
        self.redis = FakeRedis()
        self.mono_calls: list[tuple] = []
        self.lrclib_calls: list[tuple] = []
        self.dispatches: list[str] = []
        self.mono_response: dict = {}
        self.lrclib_get_response: object = "unset"     # sentinel
        self.lrclib_search_response: object = "unset"


@pytest.fixture()
def state(monkeypatch):
    st = State()

    monkeypatch.setattr(hub, "_redis", lambda: st.redis)
    async def fake_mono(path, params=None):
        st.mono_calls.append((path, params))
        return st.mono_response

    async def fake_lrclib(path, params=None):
        st.lrclib_calls.append((path, params))
        if path == "/api/get":
            return None if st.lrclib_get_response == "unset" \
                else st.lrclib_get_response
        return [] if st.lrclib_search_response == "unset" \
            else st.lrclib_search_response

    def fake_dispatch(job_id):
        st.dispatches.append(job_id)

    monkeypatch.setattr(hub, "_mono_get_json", fake_mono)
    monkeypatch.setattr(hub, "_lrclib_get", fake_lrclib)
    monkeypatch.setattr(hub, "_dispatch_job", fake_dispatch)
    return st


@pytest.fixture()
def client(state):
    # raise_server_exceptions=False mirrors production: the app's catch-all
    # Exception handler returns the 500 json; uvicorn logs the traceback.
    return TestClient(hub.app, raise_server_exceptions=False)


def _submit_track(client, track_id="166617220766674944", **extra):
    body = {"id": track_id, "quality": "320",
            "meta": {"title": "Windowpane", "artists": ["Opeth"],
                     "artwork": None, "duration": 464000},
            "lyrics": {"mode": "sidecar", "prefer": "synced"}}
    body.update(extra)
    return client.post("/api/mono/track", json=body, headers=HEADERS)


# ------------------------------------------------------------------- tests

def test_root_and_health(state, client):
    r = client.get("/")
    assert r.status_code == 200 and r.json()["ok"] is True
    assert r.json()["service"] == "leech-hub"
    r = client.get("/api/health", headers=HEADERS)
    assert r.status_code == 200 and r.json()["ok"] is True
    assert r.json()["queue_depth"] == 0


def test_auth_rejected_without_token(state, client):
    r = client.get("/api/health")
    assert r.status_code == 401
    body = r.json()
    assert body["ok"] is False and "error" in body


def test_mono_search_proxied_and_cached(state, client):
    state.mono_response = {"tracks": [], "releases": [], "artists": []}
    r1 = client.get("/api/mono/search", params={"q": "opeth"}, headers=HEADERS)
    r2 = client.get("/api/mono/search", params={"q": "opeth"}, headers=HEADERS)
    assert r1.status_code == 200 and r1.json()["ok"] is True
    assert r1.json()["data"] == state.mono_response
    assert len(state.mono_calls) == 1                 # second call hit redis


def test_release_normalized(state, client):
    state.mono_response = {
        "title": "Damnation", "artistNames": ["Opeth"], "releaseDate": "2003-03-25",
        "releaseType": "Album", "artwork": "https://art/x.jpg",
        "tracks": [
            {"id": "166617220766674944", "title": "Windowpane",
             "artistNames": ["Opeth"], "duration": 464000, "position": 1},
            {"id": "166617220766674945", "title": "In My Time Of Need",
             "artistNames": ["Opeth"], "duration": 486000, "position": 2},
        ],
    }
    r = client.get("/api/mono/release/166617220766674944", headers=HEADERS)
    assert r.status_code == 200
    data = r.json()["data"]
    assert data["title"] == "Damnation" and data["year"] == "2003"
    assert [t["title"] for t in data["tracks"]] == \
        ["Windowpane", "In My Time Of Need"]


def test_release_bad_shape_is_clean_json(state, client):
    state.mono_response = {"nope": "nothing useful"}
    r = client.get("/api/mono/release/166617220766674944", headers=HEADERS)
    assert r.status_code == 502
    body = r.json()
    assert body["ok"] is False and "track list" in body["error"]


def test_track_job_lifecycle(state, client):
    r = _submit_track(client)
    assert r.status_code == 200
    job_id = r.json()["job"]
    assert state.dispatches == [job_id]              # hub fired the runner

    r = client.post(f"/api/jobs/{job_id}/claim",
                    json={"worker": "gha-1"}, headers=HEADERS)
    assert r.status_code == 200
    job = r.json()["job"]
    assert job["status"] == "running"
    assert job["payload"]["url"].endswith(f"/track/166617220766674944")
    assert job["payload"]["lyrics"]["mode"] == "sidecar"

    r = client.post(f"/api/jobs/{job_id}/claim",
                    json={"worker": "gha-2"}, headers=HEADERS)
    assert r.status_code == 409                      # double claim rejected

    r = client.post(f"/api/jobs/{job_id}/stage", json={
        "stage": "convert", "done": 1, "total": 2, "note": "1/2",
    }, headers=HEADERS)
    assert r.status_code == 200

    r = client.post(f"/api/jobs/{job_id}/done", json={
        "result": {"url": "https://pixeldrain.com/api/file/abc",
                   "size": 11800000, "quality": "320"},
    }, headers=HEADERS)
    assert r.status_code == 200

    r = client.get(f"/api/jobs/{job_id}", headers=HEADERS)
    job = r.json()["job"]
    assert job["status"] == "done"
    assert job["result"]["url"].startswith("https://pixeldrain.com/api/file/")
    assert job["progress"]["stage"] == "convert"


def test_fail_path_is_sanitized_length(state, client):
    job_id = _submit_track(client).json()["job"]
    client.post(f"/api/jobs/{job_id}/claim", json={"worker": "w"},
                headers=HEADERS)
    r = client.post(f"/api/jobs/{job_id}/fail", json={
        "error": "x" * 5000,
    }, headers=HEADERS)
    assert r.status_code == 200
    job = client.get(f"/api/jobs/{job_id}", headers=HEADERS).json()["job"]
    assert job["status"] == "error" and len(job["error"]) <= 400


def test_next_filters_by_type(state, client):
    _submit_track(client)                                     # mono_track
    other = client.post("/api/ffmpeg/convert", json={
        "url": "https://example.com/a.wav", "format": "flac",
    }, headers=HEADERS)
    assert other.status_code == 200

    r = client.get("/api/jobs/next", params={"types": "ffmpeg_convert"},
                   headers=HEADERS)
    job = r.json()["job"]
    assert job is not None and job["kind"] == "ffmpeg_convert"
    r = client.get("/api/jobs/next", params={"types": "ffmpeg_convert"},
                   headers=HEADERS)
    assert r.json()["job"] is None                            # taken already


def test_job_404_is_clean(state, client):
    r = client.get("/api/jobs/deadbeef", headers=HEADERS)
    assert r.status_code == 404
    assert r.json()["ok"] is False


def test_album_submit_enriches_tracks(state, client):
    state.mono_response = {
        "title": "Damnation", "artistNames": ["Opeth"],
        "releaseDate": "2003-03-25",
        "tracks": [{"id": "166617220766674944", "title": "Windowpane",
                    "artistNames": ["Opeth"], "duration": 464000,
                    "position": 1}],
    }
    r = client.post("/api/mono/album", json={
        "id": "166617220766674944", "quality": "320",
        "meta": {"title": "Damnation", "artists": ["Opeth"]},
        "lyrics": {"mode": "embed", "prefer": "plain"},
    }, headers=HEADERS)
    assert r.status_code == 200
    job_id = r.json()["job"]
    job = client.post(f"/api/jobs/{job_id}/claim", json={"worker": "w"},
                      headers=HEADERS).json()["job"]
    track = job["payload"]["tracks"][0]
    assert track["url"] == f"{hub.MONO_BASE}/track/166617220766674944"
    assert track["title"] == "Windowpane"
    assert job["payload"]["meta"]["year"] == "2003"
    assert job["payload"]["lyrics"]["mode"] == "embed"


def test_convert_validation_errors(state, client):
    cases = [
        ({"url": "https://example.com/a.wav", "format": "avi"}, "format"),
        ({"url": "https://example.com/a.wav", "format": "mp3",
          "bitrate": 999}, "bitrate"),
        ({"url": "https://example.com/a.wav", "format": "mp4",
          "res": 333}, "res"),
        ({"url": "https://127.0.0.1/a.wav", "format": "mp3"}, "private"),
        ({"url": "https://169.254.169.254/latest/meta", "format": "mp3"}, "private"),
        ({"url": "not-a-url", "format": "mp3"}, "invalid"),
    ]
    for body, _ in cases:
        r = client.post("/api/ffmpeg/convert", json=body, headers=HEADERS)
        assert r.status_code == 400, body
        assert r.json()["ok"] is False
    r = client.post("/api/ffmpeg/trim", json={
        "url": "https://example.com/a.wav", "start": "2:00", "end": "1:00",
    }, headers=HEADERS)
    assert r.status_code == 400 and r.json()["ok"] is False


def test_validation_error_is_json_not_html(state, client):
    r = client.post("/api/ffmpeg/convert", json={"format": "mp3"},
                    headers=HEADERS)
    assert r.status_code == 422
    body = r.json()
    assert body["ok"] is False and "error" in body
    assert "<html" not in r.text.lower()


def test_lyrics_exact_hit_derives_plain(state, client):
    state.lrclib_get_response = {
        "id": 1, "trackName": "Windowpane", "artistName": "Opeth",
        "albumName": "Damnation", "duration": 464, "instrumental": False,
        "plainLyrics": None,
        "syncedLyrics": "[00:01.00] hello\n[00:02.00] world",
    }
    r = client.get("/api/lyrics/get", params={
        "artist": "Opeth", "track": "Windowpane", "album": "Damnation",
        "duration": 464, "prefer": "synced",
    }, headers=HEADERS)
    data = r.json()["data"]
    assert data["found"] is True
    assert data["plain"] == "hello\nworld"           # derived from synced
    assert data["chosen"] == "synced" and data["synced"].startswith("[00:01")


def test_lyrics_fuzzy_rejects_lookalike(state, client):
    state.lrclib_get_response = None                # exact 404s
    state.lrclib_search_response = [
        {"trackName": "Windowpain", "artistName": "Totally Other Band",
         "albumName": "Unrelated", "duration": 200, "instrumental": False,
         "plainLyrics": "nope", "syncedLyrics": None},
    ]
    r = client.get("/api/lyrics/get", params={
        "artist": "Opeth", "track": "Windowpane", "album": "Damnation",
    }, headers=HEADERS)
    data = r.json()["data"]
    assert data["found"] is False


def test_lyrics_fuzzy_accepts_close_match(state, client):
    state.lrclib_get_response = None
    state.lrclib_search_response = [
        {"trackName": "Windowpane (Remaster)", "artistName": "Opeth",
         "albumName": "Damnation (Remastered)", "duration": 464,
         "instrumental": False, "plainLyrics": "line", "syncedLyrics": None},
    ]
    r = client.get("/api/lyrics/get", params={
        "artist": "Opeth", "track": "Windowpane", "album": "Damnation",
        "prefer": "plain",
    }, headers=HEADERS)
    data = r.json()["data"]
    assert data["found"] is True and data["chosen"] == "plain"


def test_lyrics_search_cached_and_capped(state, client):
    state.lrclib_search_response = [
        {"trackName": f"t{i}", "artistName": "a", "albumName": "b",
         "duration": 100, "instrumental": False, "plainLyrics": "x",
         "syncedLyrics": None}
        for i in range(30)
    ]
    r1 = client.get("/api/lyrics/search", params={"q": "query"},
                    headers=HEADERS)
    r2 = client.get("/api/lyrics/search", params={"q": "query"},
                    headers=HEADERS)
    assert len(r1.json()["data"]) == 12              # capped for chat
    assert len(state.lrclib_calls) == 1              # cached second time


def test_internal_error_never_leaks_traceback(state, client):
    async def boom(path, params=None):
        raise RuntimeError("secret internals " + "z" * 300)

    import api.index as hub_module
    original = hub_module._mono_get_json
    hub_module._mono_get_json = boom
    try:
        r = client.get("/api/mono/search", params={"q": "xxxx"}, headers=HEADERS)
    finally:
        hub_module._mono_get_json = original
    assert r.status_code == 500
    body = r.json()
    assert body["ok"] is False
    assert "Traceback" not in r.text and "secret internals" not in r.text


def test_yt_job_via_generic_lane(state, client):
    r = client.post("/api/jobs", json={
        "kind": "yt",
        "payload": {"url": "https://youtube.com/watch?v=abc",
                    "audio_only": True, "format": "mp3", "bitrate": 192},
    }, headers=HEADERS)
    assert r.status_code == 200
    job_id = r.json()["job"]
    job = client.post(f"/api/jobs/{job_id}/claim", json={"worker": "w"},
                      headers=HEADERS).json()["job"]
    assert job["kind"] == "yt"
    assert job["payload"]["url"].startswith("https://youtube.com/")
    assert job["payload"]["format"] == "mp3"


def test_unknown_kind_rejected(state, client):
    r = client.post("/api/jobs", json={"kind": "tts", "payload": {}},
                    headers=HEADERS)
    assert r.status_code == 400 and r.json()["ok"] is False
