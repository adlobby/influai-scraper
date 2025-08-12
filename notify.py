# notify.py
import os, socket, requests

HOOK = os.getenv("ALERT_WEBHOOK", "")
PLATFORM = os.getenv("ALERT_PLATFORM", "discord").lower()  # discord|slack|teams|generic
TIMEOUT = float(os.getenv("ALERT_TIMEOUT_SEC", "5"))
ENV = os.getenv("ENV_NAME", "local")
HOST = socket.gethostname()

def _payload(msg: str):
    msg = f"[{ENV}@{HOST}] {msg}"
    if PLATFORM == "slack":
        return {"text": msg[:4000]}
    if PLATFORM == "teams":
        return {"text": msg[:4000]}      # Teams simple connector cards accept {"text": "..."}
    if PLATFORM == "generic":
        return {"message": msg[:4000]}
    # default: Discord
    return {"content": msg[:1900]}

def alert(msg: str):
    if not HOOK:
        return
    try:
        requests.post(HOOK, json=_payload(msg), timeout=TIMEOUT)
    except Exception:
        pass
        