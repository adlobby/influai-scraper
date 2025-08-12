# youtube_source.py
import os, time
from typing import List, Dict
from urllib.parse import urlencode
import requests
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
YOUTUBE_MAX_RESULTS = int(os.getenv("YOUTUBE_MAX_RESULTS", "8"))
YOUTUBE_LANGS = [s.strip() for s in os.getenv("YOUTUBE_LANGS", "en,en-US").split(",") if s.strip()]
REQUEST_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SEC", "20"))
DELAY_BETWEEN_REQUESTS = float(os.getenv("DELAY_BETWEEN_REQUESTS", "1.0"))

def yt_search(query: str) -> List[Dict]:
    """Search YouTube for videos matching query (relevance)."""
    if not YOUTUBE_API_KEY:
        return []
    params = {
        "part": "snippet",
        "type": "video",
        "maxResults": min(50, YOUTUBE_MAX_RESULTS),
        "q": query,
        "order": "relevance",           # change to "date" if you want newest first
        "relevanceLanguage": "en",      # helps a bit with language targeting
        "key": YOUTUBE_API_KEY,
    }
    url = "https://www.googleapis.com/youtube/v3/search?" + urlencode(params)
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    items = r.json().get("items", [])
    out = []
    for it in items:
        vid = (it.get("id") or {}).get("videoId")
        if not vid:
            continue
        sn = it.get("snippet", {})
        out.append({
            "video_id": vid,
            "title": sn.get("title", ""),
            "channel": sn.get("channelTitle", ""),
            "published_at": sn.get("publishedAt", ""),
            "url": f"https://youtu.be/{vid}",
        })
    return out

def yt_transcript(video_id: str) -> str:
    """Fetch transcript text; returns empty string if unavailable."""
    try:
        trs = YouTubeTranscriptApi.get_transcript(video_id, languages=YOUTUBE_LANGS)
        text = " ".join(chunk["text"].strip() for chunk in trs if chunk.get("text", "").strip())
        return text[:20000]  # safety cap
    except (TranscriptsDisabled, NoTranscriptFound):
        return ""
    except Exception:
        return ""

def youtube_docs_for_keyword(keyword: str) -> List[Dict]:
    docs = []
    for v in yt_search(keyword):
        text = yt_transcript(v["video_id"])
        if len(text) < 200:   # skip short/missing transcripts
            continue
        docs.append({
            "url": v["url"],
            "title": v["title"],
            "content": text,
            "topic": keyword.lower(),
            "source": f"youtube/{v['channel']}",
            "published_at": v["published_at"],
            "media": {"type": "video", "provider": "youtube", "id": v["video_id"]},
            "source_type": "youtube",
        })
        time.sleep(DELAY_BETWEEN_REQUESTS)
    return docs
