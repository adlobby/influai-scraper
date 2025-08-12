# extra_sources.py
import os, time, re, json
from urllib.parse import urlparse
from typing import List, Dict
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ---------------- Flags ----------------
ENABLE_TRENDS      = os.getenv("ENABLE_TRENDS", "true").lower() == "true"
ENABLE_NEWSAPI     = os.getenv("ENABLE_NEWSAPI", "false").lower() == "true"
ENABLE_REDDIT_API  = os.getenv("ENABLE_REDDIT_API", "false").lower() == "true"

# Backward-compat single toggle, plus per-source
FETCH_FULLTEXT_GLOBAL   = os.getenv("FETCH_FULLTEXT", "false").lower() == "true"
FETCH_FULLTEXT_NEWSAPI  = (os.getenv("FETCH_FULLTEXT_NEWSAPI", "").lower() == "true") or FETCH_FULLTEXT_GLOBAL
FETCH_FULLTEXT_REDDIT   = (os.getenv("FETCH_FULLTEXT_REDDIT", "").lower() == "true") or FETCH_FULLTEXT_GLOBAL

# ---------------- Common limits ----------------
MAX_RESULTS_PER_QUERY   = int(os.getenv("MAX_RESULTS_PER_QUERY", "8"))
DELAY_BETWEEN_REQUESTS  = float(os.getenv("DELAY_BETWEEN_REQUESTS", "1.0"))
MIN_CONTENT_LEN         = int(os.getenv("MIN_CONTENT_LEN", "150"))

# ---------------- Blocklist (static + dynamic) ----------------
DEFAULT_BLOCKED = {
    "business.adobe.com", "sciencedirect.com", "tandfonline.com",
    "statista.com", "collabstr.com", "make.com"
}
EXTRA_BLOCKED = {h.strip().lower() for h in os.getenv("DOMAIN_BLOCKLIST", "").split(",") if h.strip()}
DOMAIN_BLOCKLIST = DEFAULT_BLOCKED | EXTRA_BLOCKED

# Per-run learned restricted hosts (don’t retry once detected)
RESTRICTED_HOSTS: set[str] = set()

# --- Utilities from scraper.py (import safely) ---
try:
    from scraper import fetch, extract_text
except Exception:
    fetch = None
    extract_text = None

# ---------- Paywall / restriction detection ----------
PAYWALL_HINTS = (
    "subscribe to continue",
    "for subscribers",
    "subscriber-only",
    "paywall",
    "metered",
    "register to continue",
    "sign in to continue",
    "log in to continue",
    "this content is only available to",
    "purchase a subscription",
    "unlock unlimited access",
)

def _has_paywall_hints(html_text: str) -> bool:
    s = html_text.lower()
    return any(h in s for h in PAYWALL_HINTS)

def _jsonld_is_restricted(html_text: str) -> bool:
    # Look for schema.org JSON-LD blocks that mark access as restricted
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html_text,
        flags=re.I | re.S,
    ):
        try:
            data = json.loads(m.group(1))
        except Exception:
            continue

        def check(d):
            if not isinstance(d, dict):
                return False
            v = d.get("isAccessibleForFree")
            if isinstance(v, str):
                if v.strip().lower() in ("false", "0", "no"):
                    return True
            if v is False:
                return True
            # sometimes nested under hasPart
            hp = d.get("hasPart")
            if isinstance(hp, dict) and check(hp):
                return True
            if isinstance(hp, list) and any(check(x) for x in hp):
                return True
            return False

        if isinstance(data, dict) and check(data):
            return True
        if isinstance(data, list) and any(check(x) for x in data):
            return True
    return False

def is_probably_restricted(html_text: str) -> bool:
    # Fast heuristic combo
    return _has_paywall_hints(html_text) or _jsonld_is_restricted(html_text)

def attempt_fulltext(url: str, host: str) -> str | None:
    """Try to fetch + extract full text unless blocked/restricted. Returns text or None."""
    if not (fetch and extract_text):
        return None
    if host in DOMAIN_BLOCKLIST or host in RESTRICTED_HOSTS:
        return None
    try:
        html = fetch(url)
    except Exception:
        # network errors imply restriction or bot protection; mark host once
        RESTRICTED_HOSTS.add(host)
        return None

    # If page itself shows restriction, avoid future attempts this run
    try:
        if is_probably_restricted(html):
            RESTRICTED_HOSTS.add(host)
            return None
    except Exception:
        pass

    try:
        full = extract_text(html, url=url)
        if full and len(full) >= MIN_CONTENT_LEN:
            return full
    except Exception:
        pass
    # Very short bodies likely not worth keeping; mark host to save attempts later
    RESTRICTED_HOSTS.add(host)
    return None

# ---------- Google Trends (pytrends) ----------
def trends_related_queries(seeds: List[str], per_seed: int = None) -> List[str]:
    if not ENABLE_TRENDS:
        return []
    per_seed = per_seed or int(os.getenv("TRENDS_PER_SEED", "3"))
    try:
        from pytrends.request import TrendReq
        pt = TrendReq(hl="en-US", tz=0)
        outs = set()
        for kw in seeds:
            try:
                pt.build_payload([kw], timeframe="now 7-d")
                rq = pt.related_queries() or {}
                rising = (rq.get(kw, {}) or {}).get("rising")
                if rising is not None:
                    for q in rising["query"].head(per_seed):
                        outs.add(str(q).strip().lower())
                time.sleep(DELAY_BETWEEN_REQUESTS)
            except Exception:
                continue
        return list(outs)
    except Exception:
        return []

# ---------- NewsAPI ----------
def newsapi_items(keywords: List[str]) -> List[Dict]:
    if not ENABLE_NEWSAPI:
        return []
    API_KEY = os.getenv("NEWSAPI_KEY", "")
    if not API_KEY:
        return []
    try:
        from newsapi import NewsApiClient
    except Exception:
        return []

    newsapi = NewsApiClient(api_key=API_KEY)
    docs: List[Dict] = []

    for kw in keywords:
        try:
            res = newsapi.get_everything(
                q=kw, language="en", sort_by="publishedAt", page_size=MAX_RESULTS_PER_QUERY
            )
            for art in res.get("articles", []):
                url   = (art.get("url") or "").strip()
                if not url:
                    continue
                host  = urlparse(url).netloc.replace("www.", "").lower()
                title = (art.get("title") or "").strip()
                desc  = (art.get("description") or "").strip()

                content = desc
                if FETCH_FULLTEXT_NEWSAPI:
                    full = attempt_fulltext(url, host)
                    if full:
                        content = full

                docs.append({
                    "url": url,
                    "title": title,
                    "content": content,
                    "topic": kw.lower(),
                    "source": host,
                    "published_at": art.get("publishedAt"),
                    "source_type": "newsapi",
                })

                if len(docs) >= MAX_RESULTS_PER_QUERY:
                    break
            time.sleep(DELAY_BETWEEN_REQUESTS)
        except Exception:
            continue
    return docs

# ---------- Reddit API (PRAW) ----------
def reddit_api_items(subs: List[str], per_sub: int = 10) -> List[Dict]:
    if not ENABLE_REDDIT_API:
        return []
    cid    = os.getenv("REDDIT_CLIENT_ID", "")
    secret = os.getenv("REDDIT_CLIENT_SECRET", "")
    ua     = os.getenv("REDDIT_USER_AGENT", "InfluAIScraper/1.0")
    if not (cid and secret and ua):
        return []

    try:
        import praw
    except Exception:
        return []

    reddit = praw.Reddit(client_id=cid, client_secret=secret, user_agent=ua)
    try:
        reddit.read_only = True
    except Exception:
        pass

    docs: List[Dict] = []
    for sub in subs:
        try:
            for post in reddit.subreddit(sub).hot(limit=per_sub):
                url = (post.url or "").strip()
                if not url:
                    continue
                host = urlparse(url).netloc.replace("www.", "").lower()
                title = (post.title or "").strip()
                content = title

                if FETCH_FULLTEXT_REDDIT:
                    full = attempt_fulltext(url, host)
                    if full:
                        content = full

                docs.append({
                    "url": url,
                    "title": title,
                    "content": content,
                    "topic": sub.lower(),
                    "source": host,
                    "published_at": datetime.utcfromtimestamp(int(post.created_utc)).isoformat() + "Z",
                    "source_type": "reddit",
                })

                if len(docs) >= MAX_RESULTS_PER_QUERY:
                    break
            time.sleep(DELAY_BETWEEN_REQUESTS)
        except Exception:
            continue
    return docs

def parse_reddit_subs() -> List[str]:
    raw = os.getenv("REDDIT_SUBS", "marketing,Entrepreneur,Artificial,YouTubers")
    return [s.strip() for s in raw.split(",") if s.strip()]
