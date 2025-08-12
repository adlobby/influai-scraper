# forever_runner.py
import os, sys, time, random, subprocess, datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Configurable timing via env (defaults OK)
BASE_SLEEP = int(os.getenv("RUNNER_BASE_SLEEP_SEC", "3600"))   # 1 hour
JITTER     = int(os.getenv("RUNNER_JITTER_SEC", "90"))         # ±90s
MAX_BACKOFF= int(os.getenv("RUNNER_MAX_BACKOFF_SEC", "900"))   # 15 min

def log(msg: str):
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    line = f"[forever_runner] {ts} {msg}"
    print(line, flush=True)
    with open(os.path.join(LOG_DIR, "forever_runner.log"), "a", encoding="utf-8") as f:
        f.write(line + "\n")

def run_once() -> int:
    """Run hourly_runner.py once, stream output to a file, return exit code."""
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    out_path = os.path.join(LOG_DIR, "hourly_runner.out")
    with open(out_path, "a", encoding="utf-8") as out:
        out.write(f"\n--- run at {ts} UTC ---\n")
        proc = subprocess.run(
            [sys.executable, "hourly_runner.py"],
            cwd=SCRIPT_DIR,
            stdout=out,
            stderr=out,
        )
        return proc.returncode

fails = 0
while True:
    try:
        code = run_once()
        if code == 0:
            # Success: reset backoff, sleep ~1h with jitter
            fails = 0
            sleep_s = BASE_SLEEP + random.randint(-JITTER, JITTER)
            sleep_s = max(60, sleep_s)  # never less than 60s
            log(f"run OK (exit={code}); sleeping {sleep_s}s")
            time.sleep(sleep_s)
        else:
            # Failure: exponential backoff (30s, 60s, 120s … up to MAX_BACKOFF)
            fails += 1
            delay = min(MAX_BACKOFF, 30 * (2 ** (fails - 1)))
            log(f"run FAILED (exit={code}); backoff {delay}s (fail #{fails})")
            time.sleep(delay)
    except KeyboardInterrupt:
        log("received KeyboardInterrupt; exiting.")
        break
    except Exception as e:
        # Catch-all safeguard; treat as a failure with max backoff
        fails += 1
        log(f"unexpected error: {e!r}; backoff {MAX_BACKOFF}s (fail #{fails})")
        time.sleep(MAX_BACKOFF)
