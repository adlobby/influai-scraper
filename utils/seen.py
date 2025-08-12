# utils/seen.py
import os, json, time

class SeenCache:
    def __init__(self, path: str = "data/seen.json", ttl_hours: int = 72):
        self.path = path
        self.ttl = ttl_hours * 3600
        self.data: dict[str, float] = {}
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
            except Exception:
                self.data = {}

    def save(self):
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self.data, f)
        except Exception:
            pass

    def _key(self, topic: str, url: str) -> str:
        return f"{(topic or '').strip().lower()}||{(url or '').strip()}"

    def recently_seen(self, topic: str, url: str) -> bool:
        k = self._key(topic, url)
        ts = self.data.get(k)
        if not ts:
            return False
        return (time.time() - ts) < self.ttl

    def mark(self, topic: str, url: str):
        self.data[self._key(topic, url)] = time.time()
