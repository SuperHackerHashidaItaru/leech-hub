"""local dev entry: `uvicorn app:app --reload` from this directory.

on vercel, api/index.py is the function - this file just re-exports the
same app so you can run the hub on your machine with a .env before
deploying (TOKEN, UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN...).
"""

from api.index import app  # noqa: F401

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.index:app", host="127.0.0.1", port=8010, reload=True)
