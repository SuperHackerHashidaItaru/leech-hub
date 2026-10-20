# DEPLOY — leech hub + runner

five short stages, ~20 minutes, zero cards. you keep the old tts lane
(manager + colab) exactly as it is — nothing here touches it.

```
1. upstash redis #2          (the cache/jobs db for this lane)
2. vercel: the hub           (leech-hub/hub folder)
3. github: the runner repo   (leech-hub/runner folder, PUBLIC)
4. the PAT + hub env         (so the hub can dispatch the runner)
5. the bot                   (3 cogs + service, point env at the hub)
```

---

## 1. upstash redis #2

log in with github at upstash.com → create database:
- name: `leech-hub` (whatever), region near vercel's default
- **this must be a NEW database — not the tts/jobs one.** separate db =
  separate quotas and blast radius (exactly as discussed).
- copy **REST URL** (`https://....upstash.io`) and **REST TOKEN**

free tier: 250MB, 500k commands/day, 10k commands/min — this lane uses
a tiny fraction of that (jobs are small json docs, caches expire).

## 2. vercel: the hub

- new project → import the `hub/` folder (or a repo containing it)
- framework preset: **Other**; it's a python function
- env vars:

```
TOKEN=<make one: python -c "import secrets;print(secrets.token_urlsafe(24))">
UPSTASH_REDIS_REST_URL=<from stage 1>
UPSTASH_REDIS_REST_TOKEN=<from stage 1>
MONO_BASE=https://tracks.monochrome.st
LRCLIB_BASE=https://lrclib.net
GITHUB_REPO=<yourname>/leech-runner        (stage 3 — set after, or now)
GITHUB_PAT=                                 (stage 4 — set after)
```

- deploy → smoke test (replace `https://YOUR-HUB` and the token):

```bash
BASE=https://YOUR-HUB.vercel.app
TOKEN=the-token

curl -s $BASE/ | jq                        # {"ok":true,"service":"leech-hub",...}
curl -s $BASE/api/health -H "x-leech-token: $TOKEN" | jq
curl -s $BASE/api/mono/search -H "x-leech-token: $TOKEN" -G \
     --data-urlencode "q=opeth" | jq '.data.tracks | length'
curl -s $BASE/api/lyrics/get -H "x-leech-token: $TOKEN" -G \
     --data-urlencode "artist=Opeth" --data-urlencode "track=Windowpane" \
     | jq '.data.found, .data.chosen'
```

errors come back as `{"ok":false,"error":"human sentence"}` — if you
ever see html or a traceback, that's a bug, paste it to me.

local first (optional): `cd hub && uvicorn app:app --port 8010` with a
`.env` (copy `.env.example`).

## 3. github: the runner repo

- create a **public** repo (e.g. `leech-runner`) — public = unlimited
  free actions minutes on 4 vCPU runners; private would burn your
  2,000/month in no time
- push the contents of `runner/` (so the repo root has `runner.py`,
  `processors.py`, `.github/workflows/run.yml`, …)
- repo → settings → secrets and variables → actions → secrets:

```
HUB_URL=https://YOUR-HUB.vercel.app
LEECH_TOKEN=<the same TOKEN as the hub>
PIXELDRAIN_KEY=<optional but get it: free account → pixeldrain.com/api>
```

pixeldrain (20GB, long-lived links) only joins the chain with a key;
without it the chain is x0(1GB) → uguu(128MB/3h) → litterbox(1GB/72h).
same chain the colab worker uses, verified oct 2026.

- actions tab → enable workflows → the `leech-runner` workflow appears
  with the `*/15 * * * *` janitor schedule (only runs ~20s when the
  queue is empty — free on public repos)

log hygiene: the runner only ever prints job ids / kinds / sizes /
stages — never urls, tokens or filenames (the logs are world-readable).
runners have ~14GB free disk; caps: 400MB/track, 2.5GB/album zip,
1.5GB convert input, 50min per job.

## 4. the PAT + hub env

- github → settings → developer settings → fine-grained tokens →
  generate new:
  - repository access: **only repositories → leech-runner**
  - permissions → repository → **contents: read and write**
    (that's what `repository_dispatch` needs — verified)
- copy the token → vercel → hub project → env vars:

```
GITHUB_PAT=github_pat_xxx...
GITHUB_REPO=yourname/leech-runner
```

- redeploy. now every `POST /api/mono/*|ffmpeg/*|jobs` fires a
  repository_dispatch at the runner; if that ever fails, the */15*
  janitor picks the job up anyway.

## 5. the bot

copy into the bot (paths as the file headers say):

```
bot/core/monochrome/service.py   → leech/core/monochrome/service.py   (v2)
bot/cogs/monochrome/cog.py       → leech/cogs/monochrome/cog.py       (v2)
bot/cogs/music/cog.py            → leech/cogs/music/cog.py            (NEW)
bot/cogs/ffmpeg/cog.py           → leech/cogs/ffmpeg/cog.py           (v2)
```

env on the bot host (replaces the dead wispbyte url):

```
LEECH_MONO_URL=https://YOUR-HUB.vercel.app
LEECH_MONO_TOKEN=<the same TOKEN>
MONO_BASE=https://tracks.monochrome.st
```

the old mono_service/ffmpeg env fallbacks still work, so if you used
`LEECH_SERVER_URL`/`LEECH_TTS_TOKEN` anywhere, repoint those too.

## 6. in-discord smoke battery

```
>monochrome status                    hub + runner armed, queue 0
>monochrome track search opeth
>monochrome track download windowpane         (flac: instant + .lrc attached)
>monochrome config quality 320
>monochrome track download windowpane         (job: ~1-2 min end to end)
>monochrome album view damnation              (tracklist + art)
>monochrome album download damnation          (zip w/ tags + covers + .lrc)
>monochrome config lyrics embed
>monochrome track download windowpane         (lyrics in the tags)
>music track lyrics search daft punk one more time
>music track lyrics view opeth windowpane
>music track lyrics download opeth windowpane
>music album lyrics search damnation
>music album lyrics view damnation
>music album lyrics download damnation
>convert mp3 320                              (attach/reply a file)
>convert https://…/clip.mkv mp4 res=720
>convert https://…/clip.mp4 gif fps=15
>trim 0:10 0:25                               (stream copy, instant)
>trim 0:10 0:25 mp4                           (re-encode, exact cut)
```

first job of the day takes ~30-60s extra (runner cold boot). watch it
live: repo → actions → the `leech-runner` run → the job id in the log
matches the one in `>monochrome status` queue.

## troubleshooting

| symptom | where to look |
|---|---|
| job stuck `queued` forever | hub env `GITHUB_PAT`/`GITHUB_REPO` missing/wrong → janitor still catches it within 15 min; check the actions tab ran |
| actions run green but job still queued | runner log says "already claimed/finished"? another run took it. otherwise check `HUB_URL`/`LEECH_TOKEN` secrets |
| `runner dispatch off (janitor only)` in status | PAT env not set on the hub |
| 401 from the hub | bot/runner `TOKEN` ≠ hub `TOKEN` |
| album bigger than 2.5GB | `>monochrome config quality 192` (or opus) |
| lyrics never found | lrclib genuinely may not have it; check `>music track lyrics search` — fuzzy validation rejects lookalikes on purpose |
| yt video too big | runner caps at 2GB / 4h — that lane stays on colab if you need more |
