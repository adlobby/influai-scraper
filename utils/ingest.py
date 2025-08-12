# utils/ingest.py
import os, json, requests, hashlib, math, time
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

BACKEND_URL = os.getenv("BACKEND_URL", "http://127.0.0.1:8000").rstrip("/")
MODE        = os.getenv("INGEST_MODE", "http").lower()  # "http" or "mongo"

MONGO_URI   = os.getenv("MONGO_URI", "")
MONGO_DB    = os.getenv("MONGO_DB", "influAI")
MONGO_COL   = os.getenv("MONGO_COL", "scraped_data")

OUTBOX      = os.getenv("OUTBOX_PATH", "data/outbox.jsonl")
INGEST_HTTP_TIMEOUT = int(os.getenv("INGEST_HTTP_TIMEOUT", "60"))


# batching + timeout
INGEST_BATCH_SIZE   = max(1, int(os.getenv("INGEST_BATCH_SIZE", "20")))
INGEST_HTTP_TIMEOUT = int(os.getenv("INGEST_HTTP_TIMEOUT", "120"))

# optional notifier; safe fallback if notify.py isn't present
try:
    from notify import alert
except Exception:
    def alert(msg: str):  # no-op
        pass

# ----- Canonicalization (match backend logic) -----
TRACKING_PARAMS = {
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "gclid","fbclid","mc_cid","mc_eid","ref","ref_src","igshid"
}

def canonicalize_url(u: str) -> str:
    try:
        parts = urlsplit((u or "").strip())
        host = (parts.hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        q = [(k, v) for (k, v) in parse_qsl(parts.query, keep_blank_values=True)
             if k.lower() not in TRACKING_PARAMS]
        q.sort(key=lambda kv: (kv[0].lower(), kv[1]))
        path = parts.path or "/"
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")
        new = parts._replace(netloc=host, path=path, query=urlencode(q, doseq=True), fragment="")
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
    with open(OUTBOX, "a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

# ----- HTTP helpers -----
def _warm_up_backend():
    if not BACKEND_URL:
        return
    try:
        requests.get(f"{BACKEND_URL}/health", timeout=8)
    except Exception:
        # best-effort only
        pass

def _post_batches(items):
    """POST /ingest in small batches with retries."""
    _warm_up_backend()
    total = len(items)
    if total == 0:
        return {"ok": True, "upserts": 0}

    upserts_total = 0
    skipped_total = 0
    sess = requests.Session()

    for i in range(0, total, INGEST_BATCH_SIZE):
        batch = items[i:i+INGEST_BATCH_SIZE]
        # retry up to 2 times on network failures
        last_err = None
        for attempt in range(2):
            try:
                r = requests.post(f"{BACKEND_URL}/ingest", json=items, timeout=INGEST_HTTP_TIMEOUT)
                r.raise_for_status()
                resp = r.json()
                upserts_total += int(resp.get("upserts", 0))
                skipped_total += int(resp.get("skipped", 0))
                break
            except Exception as e:
                last_err = e
                if attempt == 0:
                    time.sleep(2)
        else:
            # both attempts failed -> outbox remaining batches and alert
            remaining = items[i:]
            _write_outbox(remaining)
            alert(f"🧺 Outbox queued {len(remaining)} docs (HTTP ingest failed). Error: {last_err}")
            return {"ok": False, "queued": len(remaining), "outbox": OUTBOX, "error": str(last_err)}

    return {"ok": True, "upserts": upserts_total, "skipped": skipped_total}

# ----- Public API -----
def ingest_items(items):
    """Try to ingest; on failure, queue to outbox and alert."""
    if not items:
        return {"ok": True, "upserts": 0, "mode": MODE}

    try:
        if MODE == "mongo":
            if not MONGO_URI:
                raise RuntimeError("MONGO_URI missing for direct ingest")
            col = MongoClient(MONGO_URI)[MONGO_DB][MONGO_COL]
            upserts = 0
            for r in items:
                url = (r.get("url") or "").strip()
                url_canon = canonicalize_url(url) if url else ""
                r.setdefault("title",""); r.setdefault("content","")
                r.setdefault("topic",""); r.setdefault("source","")
                r["url"] = url
                r["url_canon"] = url_canon
                h = content_hash(r.get("content", ""))
                if h:
                    r["content_hash"] = h

                if url_canon:
                    col.update_one(
                        {"url_canon": url_canon},
                        {"$set": r, "$setOnInsert": {"created_at": True}},
                        upsert=True
                    )
                elif h:
                    col.update_one(
                        {"content_hash": h},
                        {"$set": r, "$setOnInsert": {"created_at": True}},
                        upsert=True
                    )
                else:
                    if not url:
                        continue
                    col.update_one(
                        {"url": url},
                        {"$set": r, "$setOnInsert": {"created_at": True}},
                        upsert=True
                    )
                upserts += 1
            return {"ok": True, "upserts": upserts, "mode": "mongo"}

        # default: HTTP (batched)
        return _post_batches(items)

    except Exception as e:
        _write_outbox(items)
        alert(f"🧺 Outbox queued {len(items)} docs (ingest failed). Error: {e}")
        return {"ok": False, "queued": len(items), "outbox": OUTBOX, "error": str(e)}
