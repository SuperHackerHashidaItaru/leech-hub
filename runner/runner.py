"""runner entrypoint - one workflow run, one job (or one janitor sweep).

how the job id arrives:
    repository_dispatch  event.client_payload.job_id   (the hub's dispatch)
    workflow_dispatch    inputs.job_id                 (manual runs)
    schedule             empty -> janitor scan of /api/jobs/next

the whole thing is synchronous, exits 0 on "nothing to do" so the
janitor's 15-minute runs stay green, and exits 0 on job failures too -
the failure is reported to the hub (which is what the bot sees); a red
X here would just be noise. only a missing-config start is red.

log hygiene (public repo!): job ids, kinds, sizes and stages only -
never urls, tokens, or track/filenames. error text sent to the hub gets
urls scrubbed first; the same scrubbed form is printed.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Callable, Optional

from hub_client import HubClient, HubError
from processors import JobError, process

RUNNER_TYPES = "mono_track,mono_album,ffmpeg_convert,ffmpeg_trim,yt"
_URL_RE = re.compile(r"https?://\S+")


def _scrub(text: str) -> str:
    return _URL_RE.sub("<url>", str(text or ""))[:400]


def _job_id_from_event() -> str:
    path = os.environ.get("GITHUB_EVENT_PATH") or ""
    if not path or not os.path.exists(path):
        return ""
    try:
        event = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ""
    name = os.environ.get("GITHUB_EVENT_NAME") or ""
    if name == "repository_dispatch":
        return str(((event.get("client_payload") or {}).get("job_id")) or "")
    if name == "workflow_dispatch":
        return str(((event.get("inputs") or {}).get("job_id")) or "")
    return ""


def _make_stage(hub: HubClient, job_id: str) -> Callable[..., None]:
    """throttled stage poster - at most one post per 2s unless the stage
    name itself changed (so 'download 3/12' always lands)."""
    state = {"last_post": 0.0, "last_stage": ""}

    def stage(name: str, done: int = 0, total: int = 0, note: str = "") -> None:
        now = time.time()
        if name != state["last_stage"] or now - state["last_post"] >= 2.0:
            state["last_post"], state["last_stage"] = now, name
            try:
                hub.stage(job_id, str(name), int(done or 0), int(total or 0),
                          str(note or "")[:160])
            except HubError:
                pass                      # progress posts are best effort

    return stage


def _acquire(hub: HubClient, wanted_id: str) -> Optional[dict]:
    worker = f"gha-{os.environ.get('GITHUB_RUN_ID') or 'local'}"
    if wanted_id:
        job = hub.claim(wanted_id, worker=worker)
        if job is None:
            print(f"job {wanted_id} is already claimed or finished - exiting")
        return job
    for _ in range(5):
        nxt = hub.next_job(RUNNER_TYPES)
        if not nxt:
            print("queue empty - nothing to do")
            return None
        job = hub.claim(str(nxt.get("id") or ""), worker=worker)
        if job is not None:
            return job
        print("lost a claim race - trying the next queued job")
    print("queue has jobs but every claim lost - exiting")
    return None


def main() -> int:
    hub_url = (os.environ.get("HUB_URL") or "").strip().rstrip("/")
    token = (os.environ.get("TOKEN") or "").strip()
    if not hub_url or not token:
        print("missing HUB_URL / TOKEN secrets - cannot run")
        return 1
    hub = HubClient(hub_url, token)

    wanted = _job_id_from_event() or (os.environ.get("JOB_ID") or "").strip()
    try:
        job = _acquire(hub, wanted)
    except HubError as exc:
        print(f"cannot reach the hub: {_scrub(str(exc))}")
        return 1
    if job is None:
        return 0

    job_id = str(job.get("id"))
    kind = str(job.get("kind"))
    print(f"job {job_id} kind={kind} claimed")
    stage = _make_stage(hub, job_id)
    work = Path(tempfile.mkdtemp(prefix="leech_"))
    try:
        result = process(dict(job), hub, work, stage)
        hub.done(job_id, result)
        size = result.get("size")
        print(f"job {job_id} done"
              + (f" size={size / 1e6:.1f}mb" if size else ""))
        return 0
    except JobError as exc:
        message = _scrub(str(exc))
        hub.fail(job_id, message)
        print(f"job {job_id} failed: {message}")
        return 0
    except Exception as exc:  # noqa: BLE001 - reported, never raised here
        hub.fail(job_id, f"unexpected runner error: {type(exc).__name__}")
        print(f"job {job_id} crashed: {type(exc).__name__}")
        return 0
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
