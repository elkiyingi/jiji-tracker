"""
Jiji.ug Deal Scraper — v6
━━━━━━━━━━━━━━━━━━━━━━━━━
CLOUDFLARE BYPASS: FlareSolverr
  FlareSolverr runs as a local Docker service inside GitHub Actions.
  It spins up a real Chrome browser with undetected-chromedriver,
  solves Cloudflare challenges, and returns the rendered HTML via
  a simple HTTP API on localhost:8191.

  No API keys. No costs. Completely free and open source.
  https://github.com/FlareSolverr/FlareSolverr

HOW IT WORKS:
  We POST to http://localhost:8191/v1 with:
    { "cmd": "request.get", "url": "https://jiji.ug/cars?query=...", "maxTimeout": 60000 }
  FlareSolverr returns:
    { "solution": { "response": "<html>...</html>", "status": 200 } }

Required env vars:
  SUPABASE_URL       — Supabase project URL
  SUPABASE_KEY       — Supabase service_role key
  TELEGRAM_TOKEN     — Telegram bot token  (optional)
  TELEGRAM_CHAT_ID   — Telegram chat/channel ID  (optional)

FlareSolverr is started automatically by the GitHub Actions workflow
as a service container — no setup needed beyond the workflow file.
"""

import os
import re
import json
import time
import logging
import urllib.request
import urllib.parse
from dataclasses import dataclass, field
from typing import Optional

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

FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL", "http://localhost:8191/v1")
MAX_TIMEOUT      = 60_000   # ms — passed to FlareSolverr per request
MAX_SELLER_ADS   = 3
MAX_PAGES        = 3
RETRY_ATTEMPTS   = 2
DELAY_BETWEEN    = 3        # seconds between requests

SEARCH_QUERIES = [
    {
        "query":    "Toyota Harrier",
        "category": "cars",
        "base_url": "https://jiji.ug/cars",
    },
    {
        "query":    "Toyota Vanguard",
        "category": "cars",
        "base_url": "https://jiji.ug/cars",
    },
    {
        "query":    "Subaru Forester",
        "category": "cars",
        "base_url": "https://jiji.ug/cars",
    },
    {
        "query":    "Busiika",
        "category": "land",
        "base_url": "https://jiji.ug/land-plots-for-sale",
    },
    {
        "query":    "Namulonge",
        "category": "land",
        "base_url": "https://jiji.ug/land-plots-for-sale",
    },
]


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


# ── Price helpers ─────────────────────────────────────────────────────────────
def parse_ugx(raw: str) -> Optional[int]:
    if not raw:
        return None
    text = (
        raw.upper()
           .replace(",", "")
           .replace("\xa0", "")
           .replace(" ", "")
           .replace("USH", "")
           .replace("UGX", "")
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

    def _extract(s: str) -> Optional[int]:
        m = re.search(r"([\d.]+)M", s)
        if m:
            return int(float(m.group(1)) * 1_000_000)
        m = re.search(r"(\d{5,})", s)
        if m:
            return int(m.group(1))
        return None

    return _extract(parts[0]), _extract(parts[1])


# ── FlareSolverr fetch ────────────────────────────────────────────────────────
def flare_get(target_url: str) -> Optional[str]:
    """
    Ask FlareSolverr to fetch target_url through a real Chrome browser.
    Returns the rendered HTML string, or None on failure.
    """
    payload = json.dumps({
        "cmd":        "request.get",
        "url":        target_url,
        "maxTimeout": MAX_TIMEOUT,
    }).encode()

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            req = urllib.request.Request(
                FLARESOLVERR_URL,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=MAX_TIMEOUT // 1000 + 10) as resp:
                data = json.loads(resp.read())

            status  = data.get("status", "")
            solution = data.get("solution", {})
            html    = solution.get("response", "")
            http_status = solution.get("status", 0)

            log.info(
                "  FlareSolverr status=%s HTTP=%s bytes=%d",
                status, http_status, len(html),
            )

            if status != "ok":
                log.warning("  FlareSolverr returned status=%s: %s", status, data.get("message", ""))
                if attempt < RETRY_ATTEMPTS:
                    time.sleep(5)
                continue

            # Check we actually got past Cloudflare
            if "Just a moment" in html or "Performing security verification" in html:
                log.warning(
                    "  Still got Cloudflare challenge page (attempt %d/%d)",
                    attempt, RETRY_ATTEMPTS,
                )
                if attempt < RETRY_ATTEMPTS:
                    time.sleep(8)
                continue

            return html

        except Exception as exc:
            log.error("  FlareSolverr request error (attempt %d): %s", attempt, exc)
            if attempt < RETRY_ATTEMPTS:
                time.sleep(5)

    return None


def wait_for_flaresolverr(max_wait: int = 30) -> bool:
    """Poll FlareSolverr health endpoint until it's ready."""
    log.info("Waiting for FlareSolverr to be ready...")
    health_url = FLARESOLVERR_URL.replace("/v1", "/health")
    # fallback: just hit /v1 with a dummy payload
    for i in range(max_wait):
        try:
            req = urllib.request.Request(
                health_url,
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=3) as resp:
                body = json.loads(resp.read())
                if body.get("status") == "ok" or resp.status == 200:
                    log.info("FlareSolverr ready after %ds", i)
                    return True
        except Exception:
            pass
        time.sleep(1)
    log.error("FlareSolverr did not become ready within %ds", max_wait)
    return False


# ── Search-page parser ────────────────────────────────────────────────────────
def parse_search_html(html: str, category: str, query: str) -> list["Ad"]:
    soup = BeautifulSoup(html, "lxml")
    ads:  list[Ad] = []

    page_title = soup.title.string if soup.title else "none"
    log.info(
        "  Page title='%s' | size=%d | has_advert=%s",
        page_title, len(html), "advert" in html.lower(),
    )

    # Primary selectors
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

    # Fallback: scan all anchors for listing URLs
    log.info("  No card elements — trying link scan")
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        if not re.search(
            r"/(cars|land|vehicles|property|land-plots)/[\w-]+-\w{10,}", href
        ):
            continue
        full_url = href if href.startswith("http") else f"https://jiji.ug{href}"
        if full_url in seen:
            continue
        seen.add(full_url)
        title = a.get_text(strip=True)[:200] or "Untitled"
        ads.append(Ad(
            title=title, price=None, location="Uganda",
            image_url="", ad_url=full_url,
            category=category, query=query,
        ))

    if ads:
        log.info("  Link-scan found %d stubs", len(ads))
    return ads


def _parse_card(card, category: str, query: str) -> Optional[Ad]:
    try:
        title_el = (
            card.select_one("span.b-advert-title-inner")
            or card.select_one("div.b-advert-title")
            or card.select_one("[class*='title']")
            or card.select_one("h3")
            or card.select_one("h2")
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
                img_el.get("data-src")
                or img_el.get("src")
                or img_el.get("data-lazy")
                or ""
            )

        link_el = card.select_one("a[href]")
        if not link_el:
            return None
        href = link_el["href"]
        ad_url = href if href.startswith("http") else f"https://jiji.ug{href}"

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
    log.info("  Enriching: %.60s", ad.title)
    html = flare_get(ad.ad_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")

    # Seller count
    count = _extract_seller_count(soup)
    ad.seller_ad_count = count
    if count > MAX_SELLER_ADS:
        log.info("  SKIP broker (%d ads): %.60s", count, ad.title)
        return None

    # Seller name
    name_el = (
        soup.select_one("div.b-seller-block__name")
        or soup.select_one("span.b-advert-contact__name")
        or soup.select_one("div.b-user-info__name")
        or soup.select_one("[class*='seller'] [class*='name']")
    )
    ad.seller_name = name_el.get_text(strip=True) if name_el else ""

    # Price (detail page is more reliable)
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
            log.info(
                "  Market: USh %s ~ %s",
                f"{ad.market_price_low:,}",
                f"{ad.market_price_high:,}" if ad.market_price_high else "?",
            )

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
        m = re.search(
            r"(?:all\s+ads?|see\s+all)\s*\((\d+)\)",
            a.get_text(strip=True), re.I,
        )
        if m:
            return int(m.group(1))
    for sel in (
        "div.b-seller-block", "div.b-user-info",
        "div.b-advert-contact", "div.seller-info",
    ):
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


# ── Database ──────────────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def upsert_ads(supabase: Client, ads: list[Ad]) -> tuple[int, int]:
    inserted = skipped = 0
    for ad in ads:
        try:
            existing = (
                supabase.table("jiji_deals")
                .select("id")
                .eq("ad_url", ad.ad_url)
                .execute()
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
                log.info("Telegram alert sent: %.60s", ad.title)
    except Exception as exc:
        log.error("Telegram request failed: %s", exc)


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    # Wait for FlareSolverr service to be ready
    if not wait_for_flaresolverr(max_wait=60):
        raise RuntimeError("FlareSolverr did not start in time")

    supabase    = get_supabase()
    candidates: list[Ad] = []

    # ── 1. Scrape search pages ────────────────────────────────────────────────
    for item in SEARCH_QUERIES:
        q_enc = urllib.parse.quote(item["query"])
        for pnum in range(1, MAX_PAGES + 1):
            target = (
                f"{item['base_url']}?query={q_enc}"
                if pnum == 1
                else f"{item['base_url']}?query={q_enc}&page={pnum}"
            )
            log.info("Fetching '%s' page %d → %s", item["query"], pnum, target)
            html = flare_get(target)

            if not html:
                log.warning("No HTML — skipping '%s' page %d", item["query"], pnum)
                break

            batch = parse_search_html(html, item["category"], item["query"])
            log.info("  → %d candidates", len(batch))

            if not batch:
                log.info("  Empty — stopping pagination for '%s'", item["query"])
                break

            candidates.extend(batch)
            time.sleep(DELAY_BETWEEN)

    log.info("Total candidates: %d", len(candidates))

    # ── 2. Filter known URLs ──────────────────────────────────────────────────
    try:
        rows = supabase.table("jiji_deals").select("ad_url").execute()
        existing_urls = {r["ad_url"] for r in rows.data}
    except Exception as exc:
        log.warning("Could not fetch existing URLs: %s", exc)
        existing_urls = set()

    seen_this_run: set[str] = set()
    new_ads: list[Ad] = []
    for a in candidates:
        if a.ad_url not in existing_urls and a.ad_url not in seen_this_run:
            seen_this_run.add(a.ad_url)
            new_ads.append(a)
    log.info("New (not in DB): %d", len(new_ads))

    # ── 3. Enrich + broker filter ─────────────────────────────────────────────
    enriched: list[Ad] = []
    for ad in new_ads:
        result = enrich_ad(ad)
        if result:
            enriched.append(result)
        time.sleep(2)

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


if __name__ == "__main__":
    main()
