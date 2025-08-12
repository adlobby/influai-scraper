# sources.py
import os, time, logging
from pymongo import MongoClient
from extra_sources import trends_related_queries

# Optional (used only by fallbacks/helpers)
from pytrends.request import TrendReq
import feedparser

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

MONGO_URI = os.getenv("MONGO_URI", "")
DB_NAME   = os.getenv("MONGO_DB", "influAI")

# Safe Mongo init with quick connectivity check
client = None
db = None
if MONGO_URI:
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        db = client[DB_NAME]
    except Exception:
        client = None
        db = None

CORE_RATIO   = 0.7
TREND_RATIO  = 0.3
MAX_QUERIES  = int(os.getenv("MAX_QUERIES_PER_HOUR", "12"))
TRENDS_PER   = int(os.getenv("TRENDS_PER_SEED", "3"))
DELAY_S      = float(os.getenv("DELAY_BETWEEN_REQUESTS", "1.0"))

def _dedupe_keep_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def get_seed_topics():
    """Seeds from DB (topics collection) if present; else from .env SCRAPE_KEYWORDS."""
    if db is not None:
        try:
            seeds = [t["text"] for t in db.topics.find({"type": "seed", "active": True}, {"text": 1, "_id": 0})]
            seeds = [s.strip() for s in seeds if isinstance(s, str) and s.strip()]
            if seeds:
                return _dedupe_keep_order(seeds)
        except Exception:
            pass
    env = os.getenv("SCRAPE_KEYWORDS", "ai marketing, creator economy")
    return _dedupe_keep_order([x.strip() for x in env.split(",") if x.strip()])

def google_trends_related(seeds, per_seed=None):
    """Fallback direct pytrends (used only if extra_sources.trends_related_queries returns empty)."""
    per_seed = per_seed or TRENDS_PER
    pt = TrendReq(hl="en-US", tz=0)
    out = set()
    for kw in seeds:
        try:
            pt.build_payload([kw], timeframe="now 7-d")
            rq = pt.related_queries() or {}
            rising = (rq.get(kw, {}) or {}).get("rising")
            if rising is not None:
                for q in rising["query"].head(per_seed):
                    out.add(str(q).lower())
            time.sleep(DELAY_S)
        except Exception:
            continue
    return list(out)

def reddit_titles(subs=("marketing","Entrepreneur","Artificial","YouTubers"), per_sub=4):
    """Helper (unused by pick_queries): subreddit RSS titles as extra query ideas."""
    qs = set()
    for s in subs:
        try:
            feed = feedparser.parse(f"https://www.reddit.com/r/{s}/.rss")
            for e in feed.entries[:per_sub]:
                t = (e.get("title") or "").strip().lower()
                if t:
                    qs.add(t)
        except Exception:
            continue
    return list(qs)

def pick_queries():
    """Pick up to MAX_QUERIES, ~70% core seeds + ~30% trends."""
    seeds = get_seed_topics()
    if not seeds:
        return []

    n_core = max(1, int(MAX_QUERIES * CORE_RATIO))
    core   = seeds[:n_core] if len(seeds) >= n_core else seeds

    # Trends via helper; if none (rate-limited/etc), fall back to direct pytrends
    trend = trends_related_queries(seeds, per_seed=TRENDS_PER) or google_trends_related(seeds, per_seed=TRENDS_PER)
    trend = _dedupe_keep_order(trend)

    remaining = max(0, MAX_QUERIES - len(core))
    trend = trend[:remaining]

    plan = core + trend
    logging.info(f"[pick_queries] core={len(core)} trend={len(trend)} total={len(plan)}")
    return plan
