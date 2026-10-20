# leech hub + runner

the wispbyte leech-server is dead (killed for averaging 18% cpu on a
35% allowance — worst policy ever). this replaces it with **nothing
that's always on**:

```
 discord (leech cogs, bot hosting — always up, zero cpu drama)
   │  outbound https only, x-leech-token
   ▼
 ┌───────────────── vercel hobby (leech-hub, FastAPI) ─────────────────┐
 │  POST /api/mono/track|album     enqueue + enrich (release lookup)   │
 │  POST /api/ffmpeg/convert|trim  validate (formats, res/crf/fps)     │
 │  GET  /api/jobs/{id}            the bot polls this                  │
 │  GET  /api/mono/release/{id}    album view (cached)                 │
 │  GET  /api/lyrics/search|get    lrclib wrapper, FUZZY VALIDATED     │
 │  └─ on every submit: repository_dispatch {job_id} ──────────┐       │
 └──────────────────────────────────────────────────────────────┼──────┘
                        upstash redis #2 (jobs + caches)         │
                                                                 ▼
 ┌── github actions runner (PUBLIC repo = free unlimited minutes) ─────┐
 │  ubuntu 4 vCPU/16GB, boots per job (~30-60s cold), 50min cap       │
 │  claim → download (Range-resume) → ffmpeg (audio AND video)        │
 │        → mutagen tags (cover + lyrics embed) → zip → upload        │
 │  upload chain: pixeldrain(key) → x0 → uguu → litterbox            │
 │  POST /api/jobs/{id}/done ← the CDN url rides back to the bot      │
 │  schedule */15 = janitor for anything dispatch missed              │
 └─────────────────────────────────────────────────────────────────────┘

 untouched: vercel manager + colab/kaggle keep tts (jobs redis #1).
```

## why github actions is the compute leg

| option | verdict |
|---|---|
| **github actions, public repo** | **unlimited free minutes, 4 vCPU/16GB, no card, 20 concurrent jobs, 6h max per job. on-demand like a lambda, but with real cpu.** |
| cloudflare workers free | 10ms cpu per request — ffmpeg is minutes. dead end |
| vercel hobby functions | 60s cap, thin cpu — perfect as the control plane (that's what it is here), not for transcoding |
| hugging face spaces (free) | 2 vCPU, sleeps after 48h idle, auto-wakes — decent plan B, still "keep alive" games |
| modal | $30/mo credits, nice — but another account to babysit |
| cloud run / oracle free | generous but card-gated (same wall as always) |

reliability: dispatch fails → job stays queued → the */15 janitor run
claims it. runner dies mid-job → the hub job stays `running` until you
re-run (or the claim TTL lapses and a resubmit works). every failure
path ends in a clean json error the cog turns into a readable message —
no tracebacks, no html, either side.

## lyrics (the new bit)

lrclib.net (free, open, no key) wrapped by the hub:
`GET /api/lyrics/get` tries the exact `/api/get`, falls back to filtered
search, then **fuzzy-validates** — artist ≥0.70, track ≥0.70, album
≥0.60 — so a lookalike song never sneaks in. plain is derived from
synced when only synced exists. cached 6h (misses 30m).

per-user (shared by both cogs):
```
>monochrome config lyrics off|embed|sidecar     (default sidecar)
>monochrome config lyrics-prefer synced|plain   (default synced)
```
- `sidecar` — .lrc/.txt: attached to instant flac links, uploaded
  alongside job results, zipped into albums
- `embed` — written into the tags (LYRICS/USLT/©lyr)

dedicated commands:
```
>music track lyrics search|view|download <query>
>music album lyrics search|view|download <query>
```

## what's in here

```
hub/      vercel app (api/index.py is the function) + tests (19)
runner/   github repo contents: workflow + runner + processors + tests (22)
bot/      core/monochrome/service.py v2, cogs: monochrome v2 (album view,
          lyrics), music (NEW), ffmpeg v2 (video: mp4/mkv/webm/gif,
          res=/crf=/fps=/mute=) + tests (7)
DEPLOY.md step-by-step
```

api compatibility: same paths, same `x-leech-token`, same `{ok,...}`
envelopes and job doc shape as the dead wispbyte server — the cogs work
with `LEECH_MONO_URL` pointed at the hub. `result.url` is new (the cdn
link); old-style `result.path` is still honored if present.
