# scraper.py
import os, time, re, html, logging, random
from urllib.parse import urlencode, urlparse
from io import BytesIO

import requests
from bs4 import BeautifulSoup, UnicodeDammit
from dotenv import load_dotenv
from utils.ingest import ingest_items

# Optional robust extractor fallback
try:
    import trafilatura
except Exception:
    trafilatura = None

# Optional PDF extractor
try:
    from pdfminer.high_level import extract_text as pdf_extract_text
except Exception:
    pdf_extract_text = None

load_dotenv()

API_KEY   = os.getenv("GOOGLE_API_KEY", "")
CSE_ID    = os.getenv("GOOGLE_CSE_ID", "")
KEYWORDS  = [k.strip() for k in os.getenv("SCRAPE_KEYWORDS", "ai marketing").split(",") if k.strip()]
MAX_PER   = int(os.getenv("MAX_RESULTS_PER_QUERY", "8"))
DELAY_S   = float(os.getenv("DELAY_BETWEEN_REQUESTS", "1.0"))
TIMEOUT   = int(os.getenv("HTTP_TIMEOUT_SEC", "20"))
MIN_LEN   = int(os.getenv("MIN_CONTENT_LEN", "150"))

# PDF limits
PDF_MAX_BYTES = int(os.getenv("PDF_MAX_BYTES", str(8 * 1024 * 1024)))  # 8 MB
PDF_MAX_PAGES = int(os.getenv("PDF_MAX_PAGES", "15"))

# Blocklist: hard-coded + optional env extension (comma-separated)
DEFAULT_BLOCKED = {
    "business.adobe.com", "sciencedirect.com", "tandfonline.com",
    "statista.com", "collabstr.com", "make.com"
}
EXTRA_BLOCKED = {h.strip().lower() for h in os.getenv("DOMAIN_BLOCKLIST", "").split(",") if h.strip()}
DOMAIN_BLOCKLIST = DEFAULT_BLOCKED | EXTRA_BLOCKED

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Reusable HTTP session with simple retry behavior
SESSION = requests.Session()
try:
    from urllib3.util.retry import Retry
    from requests.adapters import HTTPAdapter
    retries = Retry(
        total=3, backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    SESSION.mount("http://", HTTPAdapter(max_retries=retries))
    SESSION.mount("https://", HTTPAdapter(max_retries=retries))
except Exception:
    pass

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
]

def clean(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def google_cse(query: str):
    if not API_KEY or not CSE_ID:
        raise RuntimeError("Set GOOGLE_API_KEY and GOOGLE_CSE_ID in .env")
    params = {
        "key": API_KEY,
        "cx": CSE_ID,
        "q": query,
        "num": min(10, MAX_PER),
    }
    url = "https://www.googleapis.com/customsearch/v1?" + urlencode(params)
    r = SESSION.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    for it in r.json().get("items", []):
        yield it.get("title", ""), it.get("link", ""), it.get("snippet", "")

def extract_text(html_str: str, url: str | None = None) -> str:
    """Heuristic DOM extraction with BeautifulSoup, then fallback to trafilatura if too short."""
    soup = BeautifulSoup(html_str, "html.parser")
    for t in soup(["script", "style", "noscript", "header", "footer", "svg", "form"]):
        t.decompose()
    main = soup.find("article") or soup.find("main") or (soup.body or soup)
    text = ""
    if main:
        paras = [clean(p.get_text(" ")) for p in main.find_all(["p", "li", "blockquote"])]
        text = " ".join([p for p in paras if len(p.split()) > 4])

    # Fallback to trafilatura if available and BS4 was too short
    if len(text) < MIN_LEN and trafilatura:
        try:
            tx = trafilatura.extract(
                html_str, url=url, include_tables=False, include_comments=False
            )
            if tx and len(tx) > len(text):
                text = tx
        except Exception:
            pass

    return text[:8000]

def extract_from_url(url: str) -> tuple[str, bool]:
    """
    Returns (text, is_pdf). Detects PDFs by content-type or extension,
    streams bytes with a size cap, and extracts text via pdfminer.
    Falls back to HTML extraction for non-PDF.
    """
    headers = {"User-Agent": random.choice(UA_POOL)}
    # Stream to inspect headers and optionally cap PDF size
    r = SESSION.get(url, headers=headers, timeout=TIMEOUT, stream=True)
    ct = (r.headers.get("content-type") or "").lower()
    looks_pdf = ("application/pdf" in ct) or url.lower().endswith(".pdf")

    if looks_pdf:
        if pdf_extract_text is None:
            r.close()
            raise RuntimeError("PDF detected but pdfminer.six not installed")
        data = b""
        for chunk in r.iter_content(chunk_size=8192):
            if not chunk:
                break
            data += chunk
            if len(data) > PDF_MAX_BYTES:
                r.close()
                raise RuntimeError("PDF too large; exceeded PDF_MAX_BYTES cap")
        r.close()
        try:
            text = pdf_extract_text(BytesIO(data), maxpages=PDF_MAX_PAGES) or ""
        except Exception:
            text = ""
        return clean(text)[:8000], True

    # Not PDF → do a regular fetch for robust HTML decoding
    r.close()
    r = SESSION.get(url, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    dammit = UnicodeDammit(r.content)
    html_src = dammit.unicode_markup or r.text
    text = extract_text(html_src, url=url)
    return text, False

def run_for_keyword(kw: str):
    docs = []
    logging.info(f"Searching: {kw}")
    try:
        iterator = google_cse(kw)
    except Exception as e:
        logging.warning(f"Google CSE failed for '{kw}': {e}")
        return docs

    for title, link, snippet in iterator:
        try:
            if not link:
                continue
            host = urlparse(link).netloc.lower().replace("www.", "")
            if host in DOMAIN_BLOCKLIST:
                logging.info(f"Skip blocked host: {host}")
                continue

            content, is_pdf = extract_from_url(link)
            if len(content) < MIN_LEN:
                logging.info(f"Skip short: {link}")
                time.sleep(DELAY_S)
                continue

            docs.append({
                "url": link,
                "title": clean(title),
                "content": content,
                "topic": kw.lower(),
                "source": host,
                "snippet": clean(snippet),
                "source_type": "pdf" if is_pdf else "web",
            })

            time.sleep(DELAY_S)
            if len(docs) >= MAX_PER:
                break
        except Exception as e:
            logging.warning(f"Fail {link}: {e}")
    return docs

def main():
    all_docs = []
    for kw in KEYWORDS:
        all_docs += run_for_keyword(kw)

    if not all_docs:
        logging.info("No docs scraped.")
        return

    res = ingest_items(all_docs)
    logging.info(f"Ingest: {res}")

if __name__ == "__main__":
    main()
