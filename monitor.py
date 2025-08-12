# monitor.py
import os, time, json, socket, datetime, requests, pathlib, sys

URL   = os.getenv("BACKEND_URL", "http://127.0.0.1:8000").rstrip("/")
HOOK  = os.getenv("ALERT_WEBHOOK", "")  # Discord/Slack/Teams webhook
ATTEMPTS = int(os.getenv("MONITOR_ATTEMPTS", "3"))
TIMEOUT  = float(os.getenv("MONITOR_TIMEOUT_SEC", "5"))
COOLDOWN_MIN = int(os.getenv("MONITOR_MINUTES_BETWEEN_ALERTS", "30"))

STATE_FILE = pathlib.Path(os.getenv("MONITOR_STATE_FILE", "data/monitor_state.json"))
STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

def load_state():
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_state(state: dict):
    try:
        STATE_FILE.write_text(json.dumps(state), encoding="utf-8")
    except Exception:
        pass

def now_utc():
    return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")

def ping() -> tuple[int, float, str]:
    """Returns (successes, avg_latency_ms, last_error) across ATTEMPTS."""
    latencies, successes, last_err = [], 0, ""
    for _ in range(ATTEMPTS):
        t0 = time.time()
        try:
            r = requests.get(f"{URL}/health", timeout=TIMEOUT)
            latency = (time.time() - t0) * 1000
            if r.ok and (r.json().get("ok") is True):
                successes += 1
                latencies.append(latency)
            else:
                last_err = f"http {r.status_code} body={r.text[:200]}"
        except Exception as e:
            last_err = repr(e)
        time.sleep(0.2)
    avg = sum(latencies)/len(latencies) if latencies else 0.0
    return successes, avg, last_err

def send_alert(status: str, successes: int, avg_ms: float, last_err: str):
    if not HOOK:
        print("[monitor] No ALERT_WEBHOOK set; skipping alert.")
        return
    host = socket.gethostname()
    msg = (
        f"**InfluAI backend {status}**\n"
        f"- time: {now_utc()}\n"
        f"- host: `{host}`\n"
        f"- url: `{URL}`\n"
        f"- successes: {successes}/{ATTEMPTS}\n"
        f"- avg latency: {avg_ms:.1f} ms\n"
        f"- last error: `{(last_err or 'n/a')}`"
    )
    try:
        # Discord JSON shape; Slack/Teams usually accept plain {"text": "..."}
        requests.post(HOOK, json={"content": msg}, timeout=5)
    except Exception as e:
        print("[monitor] alert post failed:", e)

def main() -> int:
    successes, avg_ms, last_err = ping()
    # health thresholds
    if successes == ATTEMPTS:
        status, code = "healthy ✅", 0
    elif successes >= 1:
        status, code = "degraded ⚠️", 1
    else:
        status, code = "UNHEALTHY ❌", 2

    print(f"[monitor] {status} | successes={successes}/{ATTEMPTS} avg={avg_ms:.1f}ms err={last_err[:180]}")

    state = load_state()
    last_status = state.get("last_status", "")
    last_alert  = state.get("last_alert_ts", "1970-01-01T00:00:00Z")

    # only alert when status changed or cooldown elapsed
    should_alert = (status != last_status)
    if not should_alert:
        try:
            last_dt = datetime.datetime.strptime(last_alert, "%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            last_dt = datetime.datetime(1970,1,1)
        delta_min = (datetime.datetime.utcnow() - last_dt).total_seconds() / 60.0
        if delta_min >= COOLDOWN_MIN and code != 0:
            should_alert = True

    if should_alert and code != 0:
        send_alert(status, successes, avg_ms, last_err)
        state["last_alert_ts"] = now_utc().replace("Z","") + "Z"

    state["last_status"] = status
    save_state(state)
    return code

if __name__ == "__main__":
    sys.exit(main())
            