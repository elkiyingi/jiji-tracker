"""
Jiji.ug Deal Scraper — v9
━━━━━━━━━━━━━━━━━━━━━━━━━
WHAT CHANGED from v8:
  - Scrapes the general /cars page (all models, no query filter)
  - Hard price filter: 15,000,000 – 80,000,000 UGX only
  - Deal = price is ≥ 10% below Jiji's market floor
  - Broker heuristic: if seller name appears on ≥ 3 ads already in our DB
    → flag as likely broker and skip (no extra page fetches needed)
  - 2 search pages per run (~48 candidates)

FlareSolverr runs as a GitHub Actions service container — free, no API keys.

Required env vars:
  SUPABASE_URL      — Supabase project URL
  SUPABASE_KEY      — Supabase service_role key
  TELEGRAM_TOKEN    — Telegram bot token  (optional)
  TELEGRAM_CHAT_ID  — Telegram chat/channel ID  (optional)
  FLARESOLVERR_URL  — set by workflow (http://localhost:8191/v1)
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

# ── Deal / filter parameters ──────────────────────────────────────────────────
MIN_PRICE          = 15_000_000   # ignore anything cheaper
MAX_PRICE          = 80_000_000   # ignore anything more expensive
DEAL_THRESHOLD     = 0.10         # price must be ≥ 10% below Jiji market floor
BROKER_MIN_ADS     = 3            # seller flagged as broker if they have ≥ this
                                  # many ads already stored in our DB

# ── Scraper parameters ────────────────────────────────────────────────────────
MAX_TIMEOUT        = 60_000       # ms per FlareSolverr request
MAX_SEARCH_PAGES   = 2            # pages of /cars to fetch per run (~24 ads each)
ENRICH_WORKERS     = 3            # parallel detail-page fetches
RETRY_ATTEMPTS     = 2
PAGE_DELAY         = 2            # seconds between search pages

SEARCH_QUERIES = [
    {
        "query":    "",            # empty = browse all cars, no model filter
        "category": "cars",
        "base_url": "https://jiji.ug/cars",
    },
    {
        "query":    "",
        "category": "land",
        "base_url": "https://jiji.ug/land-plots-for-sale",
    },
]

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
    query:            str          # "" for general browse

    seller_name:       str           = ""
    seller_ad_count:   int           = 0   # filled from DB heuristic
    is_likely_broker:  bool          = False
    market_price_low:  Optional[int] = None
    market_price_high: Optional[int] = None

    is_deal:     bool = field(default=False, init=False)
    deal_reason: str  = field(default="", init=False)

    def evaluate_deal(self) -> None:
        """
        Flag as deal if:
          - Jiji market floor exists AND
          - price < floor × (1 - DEAL_THRESHOLD)
          i.e. at least 10% below the low end of Jiji's range.
        """
        if (
            self.price is not None
            and self.market_price_low is not None
            and self.price < self.market_price_low * (1 - DEAL_THRESHOLD)
        ):
            pct = round((1 - self.price / self.market_price_low) * 100, 1)
            self.is_deal     = True
            self.deal_reason = (
                f"{pct}% below Jiji market floor "
                f"(floor USh {self.market_price_low:,})"
            )


# ── URL cleaner ───────────────────────────────────────────────────────────────
def clean_jiji_url(raw: str) -> str:
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


def extract_price_from_stub(raw: str) -> Optional[int]:
    """
    Extract the LISTING price from link-scan anchor text.
    Skips any USh value that is part of a market range (has ~ nearby).

    Examples:
      "5+ years on JijiUSh 38,000,000Toyota Harrier" → 38_000_000
      "USh 47.5 M ~ 48 MSubaru Forester 2014"        → None (range only)
      "USh 23,500,000Market price: USh 17 M ~ 19 M"  → 23_500_000
    """
    for m in re.finditer(r"US[Hh]\s*([\d,]+(?:\.[\d]+)?)\s*(M\b)?", raw, re.I):
        # Skip if ~ is within 15 chars after this value (low end of range)
        after = raw[m.end():m.end()+15]
        if re.search(r"[~\u007e\uff5e]", after):
            continue
        # Skip if ~ is within 15 chars before this value (high end of range)
        before = raw[max(0, m.start()-15):m.start()]
        if re.search(r"[~\u007e\uff5e]", before):
            continue
        val_str = m.group(1).replace(",", "")
        try:
            val = float(val_str)
            if m.group(2):
                return int(val * 1_000_000)
            if val < 10_000:
                return int(val * 1_000_000)
            v = int(val)
            return v if v > 100_000 else None
        except ValueError:
            pass
    return None


def parse_market_range(text: str) -> tuple[Optional[int], Optional[int]]:
    """
    Parse Jiji market price strings in all observed formats:
      "Market price: USh 56.36 M ~ 57.1 M"   <- space before M
      "Market price: USh 74.5 M ~ 79.2 M"
      "Market price: USh 74,500,000 ~ 79,200,000"
      "USh 37,550,000 ~ 40,600,000"
    """
    if not text:
        return None, None
    # Normalise: strip commas and non-breaking spaces
    t = text.replace(",", "").replace("\xa0", "").strip()
    # Primary: two values each followed by M (with optional space before M)
    m = re.search(
        r"([\d]+(?:\.[\d]+)?)\s*M[\s~\-\u2013\u2014]+([\d]+(?:\.[\d]+)?)\s*M",
        t, re.I,
    )
    if m:
        return (
            int(float(m.group(1)) * 1_000_000),
            int(float(m.group(2)) * 1_000_000),
        )
    # Fallback: two large integers (>=6 digits) separated by ~ or dash
    m2 = re.search(r"(\d{6,})\s*[~\-\u2013\u2014]\s*(\d{6,})", t)
    if m2:
        return int(m2.group(1)), int(m2.group(2))
    return None, None


def clean_stub_title(raw: str) -> str:
    """Strip Jiji badge noise from link-scan anchor text."""
    cleaned = re.sub(
        r"(Verified\s*ID|Quick\s*reply|ENTERPRISE|\d+\+?\s*years?\s*on\s*Jiji"
        r"|US[Hh][\s\d,\.]+M?|UGX[\s\d,\.]+M?)\s*",
        "", raw, flags=re.I,
    ).strip()
    cleaned = re.sub(r"^[\-–—•|\s]+", "", cleaned).strip()
    for sep in ["\n", " - ", " – "]:
        if sep in cleaned:
            cleaned = cleaned[:cleaned.index(sep)].strip()
    return cleaned[:120] if cleaned else raw[:120]


# ── Broker heuristic (DB-based, no extra page fetches) ───────────────────────
def build_broker_set(supabase: Client) -> set[str]:
    """
    Return a set of seller names that appear on ≥ BROKER_MIN_ADS records
    already in the DB. These are treated as brokers and skipped.
    Names are lowercased + stripped for fuzzy matching.
    """
    try:
        rows = (
            supabase.table("jiji_deals")
            .select("seller_name")
            .neq("seller_name", "")
            .execute()
        )
        from collections import Counter
        counts = Counter(
            r["seller_name"].strip().lower()
            for r in rows.data
            if r.get("seller_name", "").strip()
        )
        brokers = {name for name, cnt in counts.items() if cnt >= BROKER_MIN_ADS}
        log.info("Broker heuristic: %d known brokers in DB", len(brokers))
        return brokers
    except Exception as exc:
        log.warning("Could not build broker set: %s", exc)
        return set()


# Keywords that strongly indicate a commercial dealer rather than a private seller
BROKER_KEYWORDS = {
    # Company structure words
    "limited", "ltd", "company", "co.", "corp", "inc",
    # Car dealer words
    "motors", "auto", "automotive", "vehicles", "cars", "car",
    "dealers", "dealership", "garage", "imports", "exports",
    "sales", "solutions",
    # Shop/store words (catches "Seven Starr Gadgets", "Tech Shop" etc.)
    "gadgets", "shop", "store", "electronics", "phones", "hardware",
    "enterprise", "enterprises", "trading", "traders",
    # Geographic business suffixes common in Uganda
    "ug", "uganda", "kampala",
    # Service words
    "centre", "center", "services", "group",
}

def is_broker(seller_name: str, broker_set: set[str]) -> bool:
    """
    Two-layer broker check:
    1. DB heuristic — seller already has ≥ BROKER_MIN_ADS in our DB
    2. Keyword heuristic — seller name contains commercial dealer keywords
       (catches new brokers on their first appearance)
    """
    if not seller_name:
        return False
    name_lower = seller_name.strip().lower()
    # DB heuristic
    if name_lower in broker_set:
        return True
    # Keyword heuristic — any word in the name matches a broker keyword
    name_words = set(re.split(r"[\s\-_&/]+", name_lower))
    if name_words & BROKER_KEYWORDS:
        return True
    return False


# ── FlareSolverr ──────────────────────────────────────────────────────────────
_fs_session_id: Optional[str] = None
_fs_session_lock = threading.Lock()


def _fs_create_session() -> Optional[str]:
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
                log.info("FlareSolverr session: %s", sid)
                return sid
    except Exception as exc:
        log.warning("Could not create FS session: %s", exc)
    return None


def _fs_destroy_session(sid: str) -> None:
    try:
        payload = json.dumps({"cmd": "sessions.destroy", "session": sid}).encode()
        req = urllib.request.Request(
            FLARESOLVERR_URL, data=payload,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass


def flare_get(target_url: str) -> Optional[str]:
    global _fs_session_id
    with _fs_session_lock:
        if _fs_session_id is None:
            _fs_session_id = _fs_create_session()

    body: dict = {
        "cmd": "request.get",
        "url": target_url,
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
                log.info("  FS: %s HTTP=%s %d bytes %.55s",
                         status, http_st, len(html), target_url)

            if status != "ok":
                log.warning("  FS error: %s", data.get("message", ""))
                if attempt < RETRY_ATTEMPTS:
                    time.sleep(5)
                continue

            if "Just a moment" in html or "Performing security verification" in html:
                log.warning("  CF not solved (attempt %d/%d)", attempt, RETRY_ATTEMPTS)
                with _fs_session_lock:
                    _fs_session_id = None
                if attempt < RETRY_ATTEMPTS:
                    time.sleep(10)
                continue

            return html

        except Exception as exc:
            log.error("  FS error (attempt %d): %s", attempt, exc)
            if attempt < RETRY_ATTEMPTS:
                time.sleep(5)

    return None


def wait_for_flaresolverr(max_wait: int = 60) -> bool:
    log.info("Waiting for FlareSolverr (max %ds)...", max_wait)
    health = FLARESOLVERR_URL.replace("/v1", "/health")
    for i in range(max_wait):
        try:
            with urllib.request.urlopen(health, timeout=3) as resp:
                if json.loads(resp.read()).get("status") == "ok":
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
             soup.title.string if soup.title else "none", len(html))

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
            if ad and _in_price_range(ad.price):
                ads.append(ad)
        log.info("  %d cards in price range %s–%s",
                 len(ads),
                 f"USh {MIN_PRICE:,}", f"USh {MAX_PRICE:,}")
        return ads

    # Fallback: href scan
    log.info("  No cards — trying link scan")
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        if not re.search(r"/[\w-]+/[\w-]+/[\w-]+-\w{10,}\.html", href):
            continue
        clean_url = clean_jiji_url(href)
        if clean_url in seen:
            continue
        seen.add(clean_url)

        raw_text  = a.get_text(" ", strip=True)
        stub_price = extract_price_from_stub(raw_text)

        # Hard price filter at search stage — skip obvious out-of-range
        if stub_price and not _in_price_range(stub_price):
            continue

        title_el = (
            a.select_one("span[class*='title']")
            or a.select_one("div[class*='title']")
            or a.select_one("h3") or a.select_one("h2")
        )
        title = (title_el.get_text(strip=True)
                 if title_el else clean_stub_title(raw_text)) or "Untitled"

        img_el = a.find("img") or (a.parent and a.parent.find("img"))
        image_url = ""
        if img_el:
            src = (img_el.get("data-src") or img_el.get("src")
                   or img_el.get("data-lazy") or "")
            if src and not src.startswith("data:"):
                image_url = src

        ads.append(Ad(
            title=title, price=stub_price, location="Uganda",
            image_url=image_url, ad_url=clean_url,
            category=category, query=query,
        ))

    log.info("  Link-scan: %d stubs in price range", len(ads))
    return ads


def _in_price_range(price: Optional[int]) -> bool:
    """True if price is within our budget range, or if price is unknown (check later)."""
    if price is None:
        return True   # unknown price — let detail page confirm
    return MIN_PRICE <= price <= MAX_PRICE


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
            if image_url.startswith("data:"):
                image_url = ""

        link_el = card.select_one("a[href]")
        if not link_el:
            return None

        return Ad(
            title=title, price=price, location=location,
            image_url=image_url, ad_url=clean_jiji_url(link_el["href"]),
            category=category, query=query,
        )
    except Exception as exc:
        log.debug("Card parse error: %s", exc)
        return None


# ── Detail-page enrichment ────────────────────────────────────────────────────
def enrich_ad(ad: Ad, broker_set: set[str]) -> Optional[Ad]:
    """
    Fetch detail page to get:
      - Confirmed price (filter out-of-range)
      - Seller name (for broker heuristic)
      - Market price range (for deal detection)
      - Better image / location
    """
    with _log_lock:
        log.info("  Enriching: %.65s", ad.title)

    html = flare_get(ad.ad_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")

    # ── Seller name ───────────────────────────────────────────────────────────
    name_el = (
        soup.select_one("div.b-seller-block__name")
        or soup.select_one("span.b-advert-contact__name")
        or soup.select_one("div.b-user-info__name")
        or soup.select_one("[class*='seller'] [class*='name']")
    )
    ad.seller_name = name_el.get_text(strip=True) if name_el else ""

    # ── Broker heuristic ──────────────────────────────────────────────────────
    if is_broker(ad.seller_name, broker_set):
        with _log_lock:
            log.info("  SKIP broker (in DB heuristic): %s", ad.seller_name)
        ad.is_likely_broker = True
        return None

    # ── Confirmed price from detail page (always overwrites stub price) ─────
    # Be specific: target the main price element, NOT the market range element.
    # Jiji's main price is in a dedicated heading/span, not near "Market price:"
    confirmed = None
    for sel in [
        "span.b-advert-price__converted",
        "h3.b-advert-price",
        "div.b-advert-price > span",
        "span[class*='price-value']",
        "h2[class*='price']",
    ]:
        el = soup.select_one(sel)
        if el:
            v = parse_ugx(el.get_text(strip=True))
            if v and v > 100_000:
                confirmed = v
                break

    # Fallback: find the first USh value that is NOT part of a market range
    if not confirmed:
        for tag in soup.find_all(string=lambda t: t and "USh" in t or "UGX" in t):
            txt = str(tag)
            if "market" in txt.lower() or "~" in txt:
                continue
            v = parse_ugx(txt)
            if v and v > 100_000:
                confirmed = v
                break

    if confirmed:
        ad.price = confirmed  # always trust the detail page over the stub

    # Hard price filter on confirmed price
    if not _in_price_range(ad.price):
        with _log_lock:
            log.info("  SKIP out of range (USh %s): %.45s",
                     f"{ad.price:,}" if ad.price else "?", ad.title)
        return None

    # ── Market price range ────────────────────────────────────────────────────
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
                log.info("  Market USh %s ~ %s | %.40s",
                         f"{ad.market_price_low:,}",
                         f"{ad.market_price_high:,}" if ad.market_price_high else "?",
                         ad.title)

    # ── Require market price — skip ads without it ────────────────────────────
    if not ad.market_price_low:
        with _log_lock:
            log.info("  SKIP no market price: %.55s", ad.title)
        return None

    # ── Better image ──────────────────────────────────────────────────────────
    if not ad.image_url:
        og = soup.find("meta", property="og:image")
        if og:
            ad.image_url = og.get("content", "")

    # ── Better location ───────────────────────────────────────────────────────
    if not ad.location or ad.location == "Uganda":
        loc_el = (
            soup.select_one("span.b-advert-details__item--region")
            or soup.select_one("[class*='location']")
        )
        if loc_el:
            ad.location = loc_el.get_text(strip=True)

    ad.evaluate_deal()
    return ad


# ── Concurrent enrichment ─────────────────────────────────────────────────────
def enrich_all_concurrent(ads: list[Ad], broker_set: set[str]) -> list[Ad]:
    results: list[Ad] = []
    total = len(ads)
    done  = 0

    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as ex:
        futures = {ex.submit(enrich_ad, ad, broker_set): ad for ad in ads}
        for future in as_completed(futures):
            done += 1
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as exc:
                log.error("Worker error: %s", exc)
            if done % 10 == 0 or done == total:
                log.info("  Progress: %d/%d done, %d kept", done, total, len(results))

    return results


# ── Database ──────────────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def upsert_ads(supabase: Client, ads: list[Ad]) -> tuple[int, int]:
    inserted = skipped = 0
    for ad in ads:
        try:
            if supabase.table("jiji_deals").select("id").eq("ad_url", ad.ad_url).execute().data:
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
                "seller_ad_count":   0,
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
    price_str = f"USh {ad.price:,}" if ad.price else "?"
    mkt = ""
    if ad.market_price_low and ad.market_price_high:
        mkt = f"\n📊 Jiji range: USh {ad.market_price_low:,} ~ {ad.market_price_high:,}"
    text = (
        f"🔥 *DEAL — {ad.category.upper()}*\n\n"
        f"*{ad.title}*\n"
        f"💰 {price_str}{mkt}\n"
        f"👤 {ad.seller_name}\n"
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
            if not json.loads(resp.read()).get("ok"):
                log.error("Telegram error")
            else:
                log.info("Telegram: %.60s", ad.title)
    except Exception as exc:
        log.error("Telegram failed: %s", exc)


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    t_start = time.time()

    if not wait_for_flaresolverr(max_wait=60):
        raise RuntimeError("FlareSolverr not ready")

    supabase = get_supabase()

    # Build broker set from DB before scraping
    broker_set = build_broker_set(supabase)

    # Fetch existing URLs to skip
    try:
        existing_urls = {r["ad_url"] for r in
                         supabase.table("jiji_deals").select("ad_url").execute().data}
    except Exception as exc:
        log.warning("Could not fetch existing URLs: %s", exc)
        existing_urls = set()

    candidates: list[Ad] = []

    # ── 1. Scrape search pages ────────────────────────────────────────────────
    for item in SEARCH_QUERIES:
        for pnum in range(1, MAX_SEARCH_PAGES + 1):
            if item["query"]:
                q_enc = urllib.parse.quote(item["query"])
                target = (f"{item['base_url']}?query={q_enc}"
                          if pnum == 1
                          else f"{item['base_url']}?query={q_enc}&page={pnum}")
            else:
                target = (item["base_url"]
                          if pnum == 1
                          else f"{item['base_url']}?page={pnum}")

            log.info("Fetching %s page %d → %s",
                     item["category"], pnum, target)
            html = flare_get(target)
            if not html:
                log.warning("No HTML — skipping")
                break

            batch = parse_search_html(html, item["category"], item["query"])
            log.info("  → %d in-range candidates", len(batch))
            if not batch:
                break

            candidates.extend(batch)
            if pnum < MAX_SEARCH_PAGES:
                time.sleep(PAGE_DELAY)

    log.info("Total candidates: %d", len(candidates))

    # ── 2. Deduplicate against DB ─────────────────────────────────────────────
    seen: set[str] = set()
    new_ads: list[Ad] = []
    for a in candidates:
        if a.ad_url not in existing_urls and a.ad_url not in seen:
            seen.add(a.ad_url)
            new_ads.append(a)
    log.info("New (not in DB): %d", len(new_ads))

    # ── 3. Enrich concurrently ────────────────────────────────────────────────
    log.info("Enriching %d ads (%d workers)...", len(new_ads), ENRICH_WORKERS)
    enriched = enrich_all_concurrent(new_ads, broker_set)
    log.info("After all filters: kept %d / %d", len(enriched), len(new_ads))

    # ── 4. Persist ────────────────────────────────────────────────────────────
    inserted, skipped = upsert_ads(supabase, enriched)
    log.info("DB — inserted: %d, skipped: %d", inserted, skipped)

    # ── 5. Alerts ─────────────────────────────────────────────────────────────
    deals = [a for a in enriched if a.is_deal]
    log.info("Deals this run: %d", len(deals))
    for ad in deals:
        send_telegram_alert(ad)

    # Cleanup
    global _fs_session_id
    if _fs_session_id:
        _fs_destroy_session(_fs_session_id)

    elapsed = round(time.time() - t_start)
    log.info("Done in %dm %ds", elapsed // 60, elapsed % 60)


if __name__ == "__main__":
    main()