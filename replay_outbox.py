# replay_outbox.py
import os, json, time, sys
from typing import List, Dict
from utils.ingest import ingest_items, OUTBOX

# Tunables via env (sane defaults)
CHUNK_SIZE   = int(os.getenv("OUTBOX_REPLAY_CHUNK", "200"))   # docs per batch
MAX_RETRIES  = int(os.getenv("OUTBOX_MAX_RETRIES", "3"))      # retries per batch
BACKOFF_SEC  = int(os.getenv("OUTBOX_BACKOFF_SEC", "5"))      # base backoff

def _iter_jsonl(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: 
                continue
            try:
                yield json.loads(line)
            except Exception:
                # skip malformed lines
                continue

def _dedupe_by_url(items: List[Dict]) -> List[Dict]:
    by_url = {}
    for r in items:
        url = (r.get("url") or "").strip()
        if url:
            by_url[url] = r
    return list(by_url.values())

def main() -> int:
    if not os.path.exists(OUTBOX):
        print("No outbox.")
        return 0

    tmp = OUTBOX + ".tmp"
    # atomic swap: moves OUTBOX -> tmp; new writes will create a fresh OUTBOX
    os.replace(OUTBOX, tmp)

    pending = list(_iter_jsonl(tmp))
    if not pending:
        print("Outbox empty.")
        try:
            os.remove(tmp)
        except FileNotFoundError:
            pass
        return 0

    pending = _dedupe_by_url(pending)
    print(f"Replaying {len(pending)} docs from outbox …")

    processed = 0
    failed: List[Dict] = []

    for i in range(0, len(pending), CHUNK_SIZE):
        chunk = pending[i:i+CHUNK_SIZE]
        ok = False
        err = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                res = ingest_items(chunk)
                ok = isinstance(res, dict) and bool(res.get("ok", True))
                if ok:
                    break
                err = res if isinstance(res, dict) else "unknown ingest error"
            except Exception as e:
                err = str(e)
                ok = False

            # backoff with jitter
            sleep_for = BACKOFF_SEC * attempt
            print(f"[retry {attempt}/{MAX_RETRIES}] chunk failed; backing off {sleep_for}s …")
            time.sleep(sleep_for)

        if ok:
            processed += len(chunk)
        else:
            print(f"[give-up] chunk still failing after {MAX_RETRIES} tries: {err}")
            failed.extend(chunk)

    # If some failed, requeue them back into OUTBOX so they aren't lost
    if failed:
        print(f"Requeuing {len(failed)} docs back to {OUTBOX}")
        try:
            with open(OUTBOX, "a", encoding="utf-8") as f:
                for it in failed:
                    f.write(json.dumps(it, ensure_ascii=False) + "\n")
        except Exception as e:
            print(f"[error] could not requeue failed docs: {e}")
            # keep tmp for manual inspection
            return 2

    # cleanup tmp (we’ve either processed or requeued)
    try:
        os.remove(tmp)
    except FileNotFoundError:
        pass

    print(f"Replay done. processed={processed} failed={len(failed)}")
    # Exit codes: 0=all good, 2=some still failed (will be retried next run)
    return 0 if not failed else 2

if __name__ == "__main__":
    sys.exit(main())
