"""
Jiji.ug Deal Scraper — v8
━━━━━━━━━━━━━━━━━━━━━━━━━
FIXES over v7 (timeout at 25 min):
  - Enrichment is now CONCURRENT (3 detail pages in parallel via threads)
    120 ads / 3 concurrent × 12s each = ~8 minutes total ✓
  - Only scrapes page 1 per query on first run to stay well within time limits
    (MAX_SEARCH_PAGES = 1). Subsequent runs skip already-stored URLs so
    later pages are rarely needed anyway.
  - FlareSolverr session reuse: open one session per batch instead of
    a new browser per request (saves ~2s per request).
  - Workflow timeout raised to 45 min as a safety net.

FlareSolverr runs as a GitHub Actions service container — no costs, no keys.

Required env vars:
  SUPABASE_URL       — Supabase project URL
  SUPABASE_KEY       — Supabase service_role key
  TELEGRAM_TOKEN     — Telegram bot token  (optional)
  TELEGRAM_CHAT_ID   — Telegram chat/channel ID  (optional)
  FLARESOLVERR_URL   — set by workflow (http://localhost:8191/v1)
"""

import os
import re
import json
import time
import logging
import urllib.request
import urllib.parse
import threading
from dataclasses import dataclass, field
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup
from supabase import create_client, Client

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL: str     = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str     = os.environ["SUPABASE_KEY"]
TELEGRAM_TOKEN: str   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.environ.get("TELEGRAM_CHAT_ID", "")
FLARESOLVERR_URL      = os.environ.get("FLARESOLVERR_URL", "http://localhost:8191/v1")

MAX_TIMEOUT       = 60_000  # ms per FlareSolverr request
MAX_SELLER_ADS    = 3
MAX_SEARCH_PAGES  = 1       # pages per query — keep low to stay under time limit
                            # increase to 2-3 once you have a paid Actions plan
                            # or self-hosted runner with no timeout concerns
ENRICH_WORKERS    = 3       # parallel detail-page fetches
RETRY_ATTEMPTS    = 2
PAGE_DELAY        = 2       # seconds between search pages

SEARCH_QUERIES = [
    {"query": "Toyota Harrier",  "category": "cars", "base_url": "https://jiji.ug/cars"},
    {"query": "Toyota Vanguard", "category": "cars", "base_url": "https://jiji.ug/cars"},
    {"query": "Subaru Forester", "category": "cars", "base_url": "https://jiji.ug/cars"},
    {"query": "Busiika",         "category": "land", "base_url": "https://jiji.ug/land-plots-for-sale"},
    {"query": "Namulonge",       "category": "land", "base_url": "https://jiji.ug/land-plots-for-sale"},
]

# Thread-safe print lock
_log_lock = threading.Lock()


# ── Data model ────────────────────────────────────────────────────────────────
@dataclass
class Ad:
    title:            str
    price:            Optional[int]
    location:         str
    image_url:        str
    ad_url:           str
    category:         str
    query:            str

    seller_name:       str           = ""
    seller_ad_count:   int           = 0
    market_price_low:  Optional[int] = None
    market_price_high: Optional[int] = None

    is_deal:     bool = field(default=False, init=False)
    deal_reason: str  = field(default="", init=False)

    def evaluate_deal(self) -> None:
        if (
            self.price is not None
            and self.market_price_low is not None
            and self.price < self.market_price_low
        ):
            pct = round((1 - self.price / self.market_price_low) * 100, 1)
            self.is_deal     = True
            self.deal_reason = (
                f"Price {pct}% below Jiji market floor "
                f"(floor = USh {self.market_price_low:,})"
            )


# ── URL cleaner ───────────────────────────────────────────────────────────────
def clean_jiji_url(raw: str) -> str:
    """Strip Jiji tracking params: /name-ID.html?page=2&pos=21... → /name-ID.html"""
    if not raw:
        return raw
    if raw.startswith("/"):
        raw = "https://jiji.ug" + raw
    m = re.match(r"(https://jiji\.ug/[^?#]+\.html)", raw)
    if m:
        return m.group(1)
    return raw.split("#")[0]


# ── Price helpers ─────────────────────────────────────────────────────────────
def parse_ugx(raw: str) -> Optional[int]:
    if not raw:
        return None
    text = (
        raw.upper()
           .replace(",", "").replace("\xa0", "").replace(" ", "")
           .replace("USH", "").replace("UGX", "")
    )
    m = re.search(r"([\d.]+)M", text)
    if m:
        return int(float(m.group(1)) * 1_000_000)
    m = re.search(r"(\d+)", text)
    if m:
        val = int(m.group(1))
        return val if val > 100_000 else None
    return None


def parse_market_range(text: str) -> tuple[Optional[int], Optional[int]]:
    if not text:
        return None, None
    t = text.upper().replace(",", "").replace("\xa0", "").replace(" ", "")
    parts = re.split(r"[~\-–—]", t)
    if len(parts) < 2:
        return None, None

    def _ex(s: str) -> Optional[int]:
        m = re.search(r"([\d.]+)M", s)
        if m:
            return int(float(m.group(1)) * 1_000_000)
        m = re.search(r"(\d{5,})", s)
        if m:
            return int(m.group(1))
        return None

    return _ex(parts[0]), _ex(parts[1])


# ── FlareSolverr ──────────────────────────────────────────────────────────────
# One persistent session ID per run — avoids re-solving Cloudflare each time
_fs_session_id: Optional[str] = None
_fs_session_lock = threading.Lock()


def _fs_create_session() -> Optional[str]:
    """Create a FlareSolverr browser session and return its ID."""
    payload = json.dumps({"cmd": "sessions.create"}).encode()
    try:
        req = urllib.request.Request(
            FLARESOLVERR_URL, data=payload,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
            if data.get("status") == "ok":
                sid = data.get("session")
                log.info("FlareSolverr session created: %s", sid)
                return sid
    except Exception as exc:
        log.warning("Could not create FlareSolverr session: %s", exc)
    return None


def _fs_destroy_session(session_id: str) -> None:
    payload = json.dumps({"cmd": "sessions.destroy", "session": session_id}).encode()
    try:
        req = urllib.request.Request(
            FLARESOLVERR_URL, data=payload,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass


def flare_get(target_url: str) -> Optional[str]:
    """Fetch target_url via FlareSolverr. Thread-safe. Returns HTML or None."""
    global _fs_session_id

    # Lazily create a shared session
    with _fs_session_lock:
        if _fs_session_id is None:
            _fs_session_id = _fs_create_session()

    body: dict = {
        "cmd":        "request.get",
        "url":        target_url,
        "maxTimeout": MAX_TIMEOUT,
    }
    if _fs_session_id:
        body["session"] = _fs_session_id

    payload = json.dumps(body).encode()

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            req = urllib.request.Request(
                FLARESOLVERR_URL, data=payload,
                headers={"Content-Type": "application/json"}, method="POST",
            )
            with urllib.request.urlopen(req, timeout=MAX_TIMEOUT // 1000 + 15) as resp:
                data = json.loads(resp.read())

            status   = data.get("status", "")
            solution = data.get("solution", {})
            html     = solution.get("response", "")
            http_st  = solution.get("status", 0)

            with _log_lock:
                log.info("  FS: status=%s HTTP=%s bytes=%d url=%.60s",
                         status, http_st, len(html), target_url)

            if status != "ok":
                log.warning("  FS error: %s", data.get("message", ""))
                if attempt < RETRY_ATTEMPTS:
                    time.sleep(5)
                continue

            if "Just a moment" in html or "Performing security verification" in html:
                log.warning("  CF challenge not solved (attempt %d/%d)", attempt, RETRY_ATTEMPTS)
                # Invalidate session so a fresh one is created next call
                with _fs_session_lock:
                    _fs_session_id = None
                if attempt < RETRY_ATTEMPTS:
                    time.sleep(10)
                continue

            return html

        except Exception as exc:
            log.error("  FS request error (attempt %d): %s", attempt, exc)
            if attempt < RETRY_ATTEMPTS:
                time.sleep(5)

    return None


def wait_for_flaresolverr(max_wait: int = 60) -> bool:
    log.info("Waiting for FlareSolverr (max %ds)...", max_wait)
    health = FLARESOLVERR_URL.replace("/v1", "/health")
    for i in range(max_wait):
        try:
            with urllib.request.urlopen(health, timeout=3) as resp:
                body = json.loads(resp.read())
                if body.get("status") == "ok":
                    log.info("FlareSolverr ready after %ds", i)
                    return True
        except Exception:
            pass
        time.sleep(1)
    log.error("FlareSolverr not ready after %ds", max_wait)
    return False


# ── Search-page parser ────────────────────────────────────────────────────────
def parse_search_html(html: str, category: str, query: str) -> list[Ad]:
    soup = BeautifulSoup(html, "lxml")
    ads:  list[Ad] = []

    log.info("  Page: '%s' | %d chars",
             (soup.title.string or "none") if soup.title else "none", len(html))

    # Primary card selectors
    cards = (
        soup.select("article.b-list-advert__item")
        or soup.select("div.b-list-advert__item")
        or soup.select("li.b-list-advert__item")
        or soup.select("article[class*='advert']")
        or soup.select("div[class*='advert-item']")
    )

    if cards:
        log.info("  Found %d card elements", len(cards))
        for card in cards:
            ad = _parse_card(card, category, query)
            if ad:
                ads.append(ad)
        return ads

    # Fallback: href scan for listing URLs
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        if not re.search(r"/[\w-]+/[\w-]+/[\w-]+-\w{10,}\.html", href):
            continue
        clean_url = clean_jiji_url(href)
        if clean_url in seen:
            continue
        seen.add(clean_url)
        title = a.get_text(strip=True)[:200] or "Untitled"
        ads.append(Ad(
            title=title, price=None, location="Uganda",
            image_url="", ad_url=clean_url,
            category=category, query=query,
        ))

    log.info("  Link-scan found %d stubs", len(ads))
    return ads


def _parse_card(card, category: str, query: str) -> Optional[Ad]:
    try:
        title_el = (
            card.select_one("span.b-advert-title-inner")
            or card.select_one("div.b-advert-title")
            or card.select_one("[class*='title']")
            or card.select_one("h3") or card.select_one("h2")
        )
        title = title_el.get_text(strip=True) if title_el else ""
        if not title:
            return None

        price_el = (
            card.select_one("span.b-advert-price__converted")
            or card.select_one("span.b-advert-price")
            or card.select_one("div.b-advert-price")
            or card.select_one("[class*='price']")
        )
        price = parse_ugx(price_el.get_text(strip=True) if price_el else "")

        loc_el = (
            card.select_one("span.b-list-advert__item-location__text")
            or card.select_one("[class*='location']")
            or card.select_one("[class*='region']")
        )
        location = loc_el.get_text(strip=True) if loc_el else "Uganda"

        img_el = card.select_one("img")
        image_url = ""
        if img_el:
            image_url = (
                img_el.get("data-src") or img_el.get("src")
                or img_el.get("data-lazy") or ""
            )

        link_el = card.select_one("a[href]")
        if not link_el:
            return None

        ad_url = clean_jiji_url(link_el["href"])
        return Ad(
            title=title, price=price, location=location,
            image_url=image_url, ad_url=ad_url,
            category=category, query=query,
        )
    except Exception as exc:
        log.debug("Card parse error: %s", exc)
        return None


# ── Detail-page enrichment ────────────────────────────────────────────────────
def enrich_ad(ad: Ad) -> Optional[Ad]:
    """Fetch detail page, apply broker filter, extract market price. Thread-safe."""
    with _log_lock:
        log.info("  Enriching: %.65s", ad.title)

    html = flare_get(ad.ad_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")

    # Broker filter
    count = _extract_seller_count(soup)
    ad.seller_ad_count = count
    if count > MAX_SELLER_ADS:
        with _log_lock:
            log.info("  SKIP broker (%d ads): %.55s", count, ad.title)
        return None

    # Seller name
    name_el = (
        soup.select_one("div.b-seller-block__name")
        or soup.select_one("span.b-advert-contact__name")
        or soup.select_one("div.b-user-info__name")
        or soup.select_one("[class*='seller'] [class*='name']")
    )
    ad.seller_name = name_el.get_text(strip=True) if name_el else ""

    # Price
    if not ad.price:
        price_el = (
            soup.select_one("span.b-advert-price__converted")
            or soup.select_one("div.b-advert-price")
            or soup.select_one("[class*='price']")
        )
        if price_el:
            ad.price = parse_ugx(price_el.get_text(strip=True))

    # Market price range
    market_text = None
    market_el = (
        soup.select_one("div.b-advert-price__market")
        or soup.select_one("span.b-advert-price__market")
        or soup.select_one("[class*='market-price']")
    )
    if market_el:
        market_text = market_el.get_text(" ", strip=True)

    if not market_text:
        for tag in soup.find_all(string=re.compile(r"[Mm]arket\s*price", re.I)):
            parent_text = tag.parent.get_text(" ", strip=True) if tag.parent else ""
            if any(sep in parent_text for sep in ("~", "–", "—")):
                market_text = parent_text
                break
            if tag.parent and tag.parent.parent:
                sib = tag.parent.parent.get_text(" ", strip=True)
                if "~" in sib:
                    market_text = sib
                    break

    if market_text:
        ad.market_price_low, ad.market_price_high = parse_market_range(market_text)
        if ad.market_price_low:
            with _log_lock:
                log.info("  Market: USh %s ~ %s | %.45s",
                         f"{ad.market_price_low:,}",
                         f"{ad.market_price_high:,}" if ad.market_price_high else "?",
                         ad.title)

    # Better image
    if not ad.image_url:
        og = soup.find("meta", property="og:image")
        if og:
            ad.image_url = og.get("content", "")

    # Better location
    if not ad.location or ad.location == "Uganda":
        loc_el = (
            soup.select_one("span.b-advert-details__item--region")
            or soup.select_one("[class*='location']")
        )
        if loc_el:
            ad.location = loc_el.get_text(strip=True)

    ad.evaluate_deal()
    return ad


def _extract_seller_count(soup: BeautifulSoup) -> int:
    for a in soup.select("a"):
        m = re.search(r"(?:all\s+ads?|see\s+all)\s*\((\d+)\)",
                      a.get_text(strip=True), re.I)
        if m:
            return int(m.group(1))
    for sel in ("div.b-seller-block", "div.b-user-info",
                "div.b-advert-contact", "div.seller-info"):
        for el in soup.select(sel):
            m = re.search(r"(\d+)\s+[Aa]ds?", el.get_text(" ", strip=True))
            if m:
                return int(m.group(1))
    el = soup.find(attrs={"data-ads-count": True})
    if el:
        try:
            return int(el["data-ads-count"])
        except (ValueError, TypeError):
            pass
    m = re.search(r"(\d+)\s+active\s+ads?", soup.get_text(" "), re.I)
    if m:
        return int(m.group(1))
    return 1


# ── Concurrent enrichment ─────────────────────────────────────────────────────
def enrich_all_concurrent(ads: list[Ad]) -> list[Ad]:
    """Enrich ads in parallel using ENRICH_WORKERS threads."""
    results: list[Ad] = []
    total = len(ads)
    completed = 0

    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as executor:
        futures = {executor.submit(enrich_ad, ad): ad for ad in ads}
        for future in as_completed(futures):
            completed += 1
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as exc:
                log.error("Enrich worker error: %s", exc)
            if completed % 10 == 0 or completed == total:
                log.info("  Progress: %d/%d enriched, %d kept so far",
                         completed, total, len(results))

    return results


# ── Database ──────────────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def upsert_ads(supabase: Client, ads: list[Ad]) -> tuple[int, int]:
    inserted = skipped = 0
    for ad in ads:
        try:
            existing = (
                supabase.table("jiji_deals")
                .select("id").eq("ad_url", ad.ad_url).execute()
            )
            if existing.data:
                skipped += 1
                continue
            supabase.table("jiji_deals").insert({
                "title":             ad.title,
                "price":             ad.price,
                "location":          ad.location,
                "image_url":         ad.image_url,
                "ad_url":            ad.ad_url,
                "category":          ad.category,
                "query":             ad.query,
                "seller_name":       ad.seller_name,
                "seller_ad_count":   ad.seller_ad_count,
                "market_price_low":  ad.market_price_low,
                "market_price_high": ad.market_price_high,
                "is_deal":           ad.is_deal,
                "deal_reason":       ad.deal_reason,
            }).execute()
            inserted += 1
        except Exception as exc:
            log.error("DB error for '%s': %s", ad.title, exc)
    return inserted, skipped


# ── Telegram ──────────────────────────────────────────────────────────────────
def send_telegram_alert(ad: Ad) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    price_str = f"USh {ad.price:,}" if ad.price else "Price not listed"
    mkt = ""
    if ad.market_price_low and ad.market_price_high:
        mkt = f"\n📊 Jiji range: USh {ad.market_price_low:,} ~ {ad.market_price_high:,}"
    text = (
        f"🔥 *DEAL ALERT — {ad.category.upper()}*\n\n"
        f"*{ad.title}*\n"
        f"💰 {price_str}{mkt}\n"
        f"👤 {ad.seller_name} ({ad.seller_ad_count} total ad(s))\n"
        f"📍 {ad.location}\n"
        f"📌 _{ad.deal_reason}_\n\n"
        f"[View on Jiji]({ad.ad_url})"
    )
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID, "text": text,
        "parse_mode": "Markdown", "disable_web_page_preview": False,
    }).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        data=payload, headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if not result.get("ok"):
                log.error("Telegram error: %s", result)
            else:
                log.info("Telegram alert: %.60s", ad.title)
    except Exception as exc:
        log.error("Telegram failed: %s", exc)


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    t_start = time.time()

    if not wait_for_flaresolverr(max_wait=60):
        raise RuntimeError("FlareSolverr did not start in time")

    supabase    = get_supabase()
    candidates: list[Ad] = []

    # ── 1. Scrape search pages ────────────────────────────────────────────────
    for item in SEARCH_QUERIES:
        q_enc = urllib.parse.quote(item["query"])
        for pnum in range(1, MAX_SEARCH_PAGES + 1):
            target = (
                f"{item['base_url']}?query={q_enc}"
                if pnum == 1
                else f"{item['base_url']}?query={q_enc}&page={pnum}"
            )
            log.info("Fetching '%s' page %d", item["query"], pnum)
            html = flare_get(target)

            if not html:
                log.warning("No HTML — skipping '%s' p%d", item["query"], pnum)
                break

            batch = parse_search_html(html, item["category"], item["query"])
            log.info("  → %d candidates", len(batch))

            if not batch:
                break

            candidates.extend(batch)
            if pnum < MAX_SEARCH_PAGES:
                time.sleep(PAGE_DELAY)

    log.info("Total candidates: %d", len(candidates))

    # ── 2. Filter already-stored URLs ─────────────────────────────────────────
    try:
        rows = supabase.table("jiji_deals").select("ad_url").execute()
        existing_urls = {r["ad_url"] for r in rows.data}
    except Exception as exc:
        log.warning("Could not fetch existing URLs: %s", exc)
        existing_urls = set()

    seen: set[str] = set()
    new_ads: list[Ad] = []
    for a in candidates:
        if a.ad_url not in existing_urls and a.ad_url not in seen:
            seen.add(a.ad_url)
            new_ads.append(a)
    log.info("New (not in DB): %d", len(new_ads))

    # ── 3. Concurrent enrichment + broker filter ──────────────────────────────
    log.info("Enriching %d ads with %d workers...", len(new_ads), ENRICH_WORKERS)
    enriched = enrich_all_concurrent(new_ads)

    log.info(
        "After broker filter (≤%d ads): kept %d / %d",
        MAX_SELLER_ADS, len(enriched), len(new_ads),
    )

    # ── 4. Persist ────────────────────────────────────────────────────────────
    inserted, skipped = upsert_ads(supabase, enriched)
    log.info("DB — inserted: %d, duplicate-skipped: %d", inserted, skipped)

    # ── 5. Telegram alerts ────────────────────────────────────────────────────
    deals = [a for a in enriched if a.is_deal]
    log.info("Deals this run: %d", len(deals))
    for ad in deals:
        send_telegram_alert(ad)

    # ── Cleanup FlareSolverr session ──────────────────────────────────────────
    global _fs_session_id
    if _fs_session_id:
        _fs_destroy_session(_fs_session_id)

    elapsed = round(time.time() - t_start)
    log.info("Done in %dm %ds", elapsed // 60, elapsed % 60)


if __name__ == "__main__":
    main()
