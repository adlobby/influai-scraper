# hourly_runner.py
import os
import sys
import json
import time
from datetime import datetime, timezone
from collections import Counter

from scraper import run_for_keyword
from utils.ingest import ingest_items
from sources import pick_queries
from extra_sources import newsapi_items, reddit_api_items, parse_reddit_subs
from youtube_source import youtube_docs_for_keyword

# optional notifier (safe no-op if notify.py not present)
try:
    from notify import alert
except Exception:
    def alert(msg: str):  # no-op
        pass

# -------------------- Feature toggles & limits --------------------
ENABLE_NEWSAPI = os.getenv("ENABLE_NEWSAPI", "false").lower() == "true"
ENABLE_REDDIT  = os.getenv("ENABLE_REDDIT_API", "false").lower() == "true"
ENABLE_YOUTUBE = os.getenv("ENABLE_YOUTUBE", "false").lower() == "true"

REDDIT_PER_SUB = int(os.getenv("REDDIT_PER_SUB", "8"))
PAUSE_BETWEEN_QUERIES = float(os.getenv("DELAY_BETWEEN_REQUESTS", "1.0"))
MAX_TOTAL_DOCS = max(1, int(os.getenv("MAX_TOTAL_DOCS", "500")))  # safety cap
INGEST_BATCH_SIZE = max(1, int(os.getenv("INGEST_BATCH_SIZE", "50")))


# Run strategy:
#  - "all": run all enabled sources every time
#  - "rotate": rotate heavy sources by hour (NewsAPI, Reddit, YouTube)
RUN_STRATEGY = os.getenv("RUN_STRATEGY", "all").lower()
ROTATE_SLOTS = ["newsapi", "reddit", "youtube"]  # baseline CSE always runs

def rotation_gate() -> dict:
    """Decide which heavy sources run this time if RUN_STRATEGY=rotate."""
    if RUN_STRATEGY != "rotate":
        return {"newsapi": ENABLE_NEWSAPI, "reddit": ENABLE_REDDIT, "youtube": ENABLE_YOUTUBE}
    # timezone-aware UTC
    slot_idx = int(datetime.now(timezone.utc).strftime("%H")) % len(ROTATE_SLOTS)
    chosen = ROTATE_SLOTS[slot_idx]
    return {
        "newsapi": ENABLE_NEWSAPI and chosen == "newsapi",
        "reddit":  ENABLE_REDDIT  and chosen == "reddit",
        "youtube": ENABLE_YOUTUBE and chosen == "youtube",
    }

# -------------------- Helpers --------------------
def tag_docs(items, source_type: str):
    """Ensure docs have a 'source_type' label for stats/filters."""
    for it in items:
        it.setdefault("source_type", source_type)
    return items

def dedupe_by_url(items):
    """Keep the last occurrence per URL (cheap & simple)."""
    by_url = {}
    for r in items:
        url = (r.get("url") or "").strip()
        if url:
            by_url[url] = r
    return list(by_url.values())

def capped(items):
    """Apply global cap after de-duplication."""
    return items[:MAX_TOTAL_DOCS]

# -------------------- Main pipeline --------------------
def main():
    gates = rotation_gate()
    plan_msg = f"[plan] strategy={RUN_STRATEGY} gates={gates}"
    print(plan_msg)

    queries = pick_queries()
    if not queries:
        msg = "⚠️ No queries picked (check /topics seed list or SCRAPE_KEYWORDS)."
        print(msg)
        alert(msg)
        return 0  # not an error; just nothing to do

    all_docs = []

    # 1) Google CSE (baseline, always) + optional YouTube per query
    for q in queries:
        try:
            print(f"[run] Google CSE -> {q}")
            docs = run_for_keyword(q)
            tag_docs(docs, "web")
            all_docs.extend(docs)
        except Exception as e:
            print(f"[warn] CSE failed for '{q}': {e}")

        if gates["youtube"]:
            try:
                print(f"[run] YouTube transcripts -> {q}")
                ydocs = youtube_docs_for_keyword(q)
                tag_docs(ydocs, "youtube")  # harmless if already set
                all_docs.extend(ydocs)
            except Exception as e:
                print(f"[warn] YouTube failed for '{q}': {e}")

        # De-dupe as we go so cap is meaningful
        all_docs = dedupe_by_url(all_docs)
        all_docs = capped(all_docs)

        time.sleep(PAUSE_BETWEEN_QUERIES)
        if len(all_docs) >= MAX_TOTAL_DOCS:
            print("[info] Hit MAX_TOTAL_DOCS cap during CSE/YouTube loop.")
            break

    # 2) NewsAPI (optional, may be rate-rotated)
    if gates["newsapi"] and len(all_docs) < MAX_TOTAL_DOCS:
        try:
            print("[run] NewsAPI …")
            ndocs = newsapi_items(queries)
            tag_docs(ndocs, "newsapi")
            all_docs.extend(ndocs)
            all_docs = capped(dedupe_by_url(all_docs))
        except Exception as e:
            print(f"[warn] NewsAPI failed: {e}")

    # 3) Reddit API (optional, may be rate-rotated)
    if gates["reddit"] and len(all_docs) < MAX_TOTAL_DOCS:
        try:
            subs = parse_reddit_subs()
            print(f"[run] Reddit API -> subs={subs}")
            rdocs = reddit_api_items(subs, per_sub=REDDIT_PER_SUB)
            tag_docs(rdocs, "reddit")
            all_docs.extend(rdocs)
            all_docs = capped(dedupe_by_url(all_docs))
        except Exception as e:
            print(f"[warn] Reddit failed: {e}")

    if not all_docs:
        msg = "⚠️ Scraper run produced 0 docs."
        print(msg)
        alert(msg)
        return 3  # signal real failure to forever_runner

    # 4) Final stats
    stats = Counter([d.get("source_type", "unknown") for d in all_docs])
    print(f"[info] Docs after de-duplication: {len(all_docs)}  |  breakdown: {dict(stats)}")

    # 5) Ingest (with a quick retry; utils.ingest will outbox on failure anyway)
    ok = True
    all_responses = []
    batches = [all_docs[i:i+INGEST_BATCH_SIZE] for i in range(0, len(all_docs), INGEST_BATCH_SIZE)]

    for bi, batch in enumerate(batches, 1):
        try:
            res = ingest_items(batch)
            all_responses.append(res)
            b_ok = isinstance(res, dict) and bool(res.get("ok", True))
            ok = ok and b_ok
            print(f"[ingest] batch {bi}/{len(batches)} -> {res}")
            time.sleep(1)  # tiny pause between batches
        except Exception as e:
            print(f"[error] ingest batch {bi} failed: {e}")
            ok = False

    print("Ingest (summary):", json.dumps(all_responses, indent=2))

    # Alert summary (success or queued)
    try:
        if ok:
            alert(f"✅ Ingested {len(all_docs)} docs. Breakdown: {dict(stats)}")
        else:
            alert(f"⚠️ Ingest completed with errors. Response: {json.dumps(res)[:1500]}")
    except Exception:
        pass

    return 0 if ok else 2  # 0=success, 2=ingest problem

if __name__ == "__main__":
    sys.exit(main())
