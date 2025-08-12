# utils/ingest.py
import os, json, requests, hashlib
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

BACKEND_URL = os.getenv("BACKEND_URL", "http://127.0.0.1:8000")
MODE        = os.getenv("INGEST_MODE", "http").lower()  # "http" or "mongo"

MONGO_URI   = os.getenv("MONGO_URI", "")
MONGO_DB    = os.getenv("MONGO_DB", "influAI")
MONGO_COL   = os.getenv("MONGO_COL", "scraped_data")

OUTBOX      = os.getenv("OUTBOX_PATH", "data/outbox.jsonl")

# optional notifier; safe fallback if notify.py isn't present
try:
    from notify import alert
except Exception:
    def alert(msg: str):  # no-op
        pass

# ----- Canonicalization (match backend logic) -----
# Keep lowercase for case-insensitive comparisons
TRACKING_PARAMS = {
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "gclid","fbclid","mc_cid","mc_eid","ref","ref_src","igshid"
}

def canonicalize_url(u: str) -> str:
    """
    Normalize URL for dedupe:
      - lowercase host, drop leading 'www.'
      - remove known tracking params (case-insensitive)
      - sort remaining query pairs for stability
      - drop fragments
      - normalize path (remove trailing slash except root)
    """
    try:
        parts = urlsplit((u or "").strip())
        host = (parts.hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]

        # drop tracking params (case-insensitive) and sort
        q_pairs = parse_qsl(parts.query, keep_blank_values=True)
        q_pairs = [(k, v) for (k, v) in q_pairs if k.lower() not in TRACKING_PARAMS]
        q_pairs.sort(key=lambda kv: (kv[0].lower(), kv[1]))

        # normalize path
        path = parts.path or "/"
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")

        new = parts._replace(
            netloc=host,
            path=path,
            query=urlencode(q_pairs, doseq=True),
            fragment=""
        )
        return urlunsplit(new)
    except Exception:
        return (u or "").strip()

def content_hash(text: str):
    t = (text or "").strip()
    if not t:
        return None
    return hashlib.sha256(t.encode("utf-8")).hexdigest()

# ----- Outbox helpers -----
def _ensure_outbox_dir():
    d = os.path.dirname(OUTBOX)
    if d:
        os.makedirs(d, exist_ok=True)

def _write_outbox(items):
    _ensure_outbox_dir()
    try:
        with open(OUTBOX, "a", encoding="utf-8") as f:
            for it in items:
                f.write(json.dumps(it, ensure_ascii=False) + "\n")
    except Exception:
        # last-resort: swallow file I/O errors to avoid crashing the caller
        pass

def _augment_for_dedupe(item: dict) -> dict:
    """Attach url_canon and content_hash for better dedupe downstream."""
    url = (item.get("url") or "").strip()
    if url:
        item["url_canon"] = canonicalize_url(url)
    h = content_hash(item.get("content", ""))
    if h:
        item["content_hash"] = h
    return item

# ----- Public API -----
def ingest_items(items):
    """Try to ingest; on failure, queue to outbox and alert."""
    if not items:
        return {"ok": True, "upserts": 0, "mode": MODE}

    # augment all items with canonical fields to help either backend or mongo mode
    items = [_augment_for_dedupe(dict(r)) for r in items]

    try:
        if MODE == "mongo":
            if not MONGO_URI:
                raise RuntimeError("MONGO_URI missing for direct ingest")
            col = MongoClient(MONGO_URI)[MONGO_DB][MONGO_COL]
            upserts = 0
            for r in items:
                url = (r.get("url") or "").strip()
                url_canon = (r.get("url_canon") or "").strip()
                r.setdefault("title", ""); r.setdefault("content", "")
                r.setdefault("topic", ""); r.setdefault("source", "")

                # choose best key: url_canon, then content_hash, then raw url
                key = None
                if url_canon:
                    key = {"url_canon": url_canon}
                else:
                    h = r.get("content_hash")
                    if h:
                        key = {"content_hash": h}
                    elif url:
                        key = {"url": url}

                if not key:
                    # nothing to upsert by; skip
                    continue

                col.update_one(
                    key,
                    {"$set": r, "$setOnInsert": {"created_at": True}},
                    upsert=True
                )
                upserts += 1
            return {"ok": True, "upserts": upserts, "mode": "mongo"}

        # default: HTTP to backend
        r = requests.post(f"{BACKEND_URL}/ingest", json=items, timeout=60)
        r.raise_for_status()
        return r.json()

    except Exception as e:
        _write_outbox(items)
        alert(f"🧺 Outbox queued {len(items)} docs (ingest failed). Error: {e}")
        return {"ok": False, "queued": len(items), "outbox": OUTBOX, "error": str(e)}
