"""
Jiji.ug Deal Scraper — v2
━━━━━━━━━━━━━━━━━━━━━━━━
Logic:
  1. For each search query, scrape listing cards from search results.
  2. For every NEW candidate ad, open the individual listing page to:
       a. Extract Jiji's own "Market price: USh X ~ Y" range.
       b. Read the seller's total ad count shown on the page.
  3. SKIP ads where seller_ad_count > 3 (broker filter).
  4. Flag as a deal if price < market_price_low (below Jiji's range floor).
  5. Upsert new ads to Supabase; send Telegram alert for deals.

Required env vars:
  SUPABASE_URL       — Supabase project URL
  SUPABASE_KEY       — Supabase service_role key (write access)
  TELEGRAM_TOKEN     — Telegram bot token  (optional)
  TELEGRAM_CHAT_ID   — Telegram chat / channel ID  (optional)
"""

import asyncio
import os
import re
import logging
from dataclasses import dataclass, field
from typing import Optional

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
from supabase import create_client, Client

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL: str       = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str       = os.environ["SUPABASE_KEY"]
TELEGRAM_TOKEN: str     = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID: str   = os.environ.get("TELEGRAM_CHAT_ID", "")

MAX_SELLER_ADS  = 3          # sellers with more ads than this are considered brokers
MAX_PAGES       = 3          # search-result pages per query
PAGE_TIMEOUT    = 30_000     # ms — search page load
DETAIL_TIMEOUT  = 20_000     # ms — individual listing page load
CONCURRENCY     = 3          # parallel detail-page fetches

SEARCH_QUERIES = [
    {"query": "Toyota Harrier",  "category": "cars"},
    {"query": "Toyota Vanguard", "category": "cars"},
    {"query": "Subaru Impreza",  "category": "cars"},
    {"query": "Busiika",         "category": "land"},
    {"query": "Namulonge",       "category": "land"},
]

JIJI_SEARCH_URL = "https://jiji.ug/search?query={query}&page={page}"


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

    # enriched from detail page
    seller_name:       str           = ""
    seller_ad_count:   int           = 0
    market_price_low:  Optional[int] = None
    market_price_high: Optional[int] = None

    # computed
    is_deal:     bool = field(default=False, init=False)
    deal_reason: str  = field(default="", init=False)

    def evaluate_deal(self) -> None:
        reasons: list[str] = []
        if (
            self.price is not None
            and self.market_price_low is not None
            and self.price < self.market_price_low
        ):
            pct = round((1 - self.price / self.market_price_low) * 100, 1)
            reasons.append(
                f"Price {pct}% below Jiji market floor "
                f"(floor = USh {self.market_price_low:,})"
            )
        if reasons:
            self.is_deal    = True
            self.deal_reason = "; ".join(reasons)


# ── Price helpers ─────────────────────────────────────────────────────────────
def parse_ugx(raw: str) -> Optional[int]:
    """'USh 78,000,000' | '45.5M' | 'UGX 45000000'  →  int"""
    if not raw:
        return None
    text = (
        raw.upper()
        .replace(",", "")
        .replace(" ", "")
        .replace("USH", "")
        .replace("UGX", "")
    )
    m = re.search(r"([\d.]+)M", text)
    if m:
        return int(float(m.group(1)) * 1_000_000)
    m = re.search(r"(\d+)", text)
    if m:
        return int(m.group(1))
    return None


def parse_market_range(text: str) -> tuple[Optional[int], Optional[int]]:
    """
    'Market price: USh 74.5 M ~ 79.2 M'  →  (74_500_000, 79_200_000)
    'USh 74,500,000 ~ 79,200,000'         →  same
    """
    if not text:
        return None, None
    t = text.upper().replace(",", "").replace(" ", "")
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


# ── Search-page scraper ───────────────────────────────────────────────────────
async def scrape_search_page(
    page, url: str, category: str, query: str
) -> list[Ad]:
    ads: list[Ad] = []
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
        await page.wait_for_selector(
            "article.b-list-advert__item, div.b-list-advert-base",
            timeout=PAGE_TIMEOUT,
        )
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1.2)
    except PlaywrightTimeout:
        log.warning("Timeout on search page: %s", url)
        return ads

    soup = BeautifulSoup(await page.content(), "html.parser")
    cards = (
        soup.select("article.b-list-advert__item")
        or soup.select("div.b-list-advert-base__item")
    )
    for card in cards:
        ad = _parse_search_card(card, category, query)
        if ad:
            ads.append(ad)
    return ads


def _parse_search_card(card, category: str, query: str) -> Optional[Ad]:
    try:
        title_el = (
            card.select_one("span.b-advert-title-inner")
            or card.select_one("div.b-advert-title")
            or card.select_one("h3")
        )
        title = title_el.get_text(strip=True) if title_el else ""
        if not title:
            return None

        price_el = (
            card.select_one("span.b-advert-price__converted")
            or card.select_one("span.b-advert-price")
            or card.select_one("div.b-advert-price")
        )
        price = parse_ugx(price_el.get_text(strip=True) if price_el else "")

        loc_el = (
            card.select_one("span.b-list-advert__item-location__text")
            or card.select_one("div.b-advert-location")
        )
        location = loc_el.get_text(strip=True) if loc_el else "Uganda"

        img_el = card.select_one("img")
        image_url = ""
        if img_el:
            image_url = img_el.get("data-src") or img_el.get("src") or ""

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
async def enrich_ad(context, ad: Ad) -> Optional[Ad]:
    """
    Visit individual listing page.
    Returns None if seller has > MAX_SELLER_ADS (broker) or on hard failure.
    """
    page = await context.new_page()
    try:
        await page.goto(ad.ad_url, wait_until="domcontentloaded", timeout=DETAIL_TIMEOUT)
        await asyncio.sleep(0.8)
        soup = BeautifulSoup(await page.content(), "html.parser")

        # ── Seller ad count ───────────────────────────────────────────────────
        count = _extract_seller_count(soup)
        ad.seller_ad_count = count
        if count > MAX_SELLER_ADS:
            log.info("  SKIP broker (%d ads): %s", count, ad.title[:60])
            return None

        # ── Seller name ───────────────────────────────────────────────────────
        name_el = (
            soup.select_one("div.b-seller-block__name")
            or soup.select_one("span.b-advert-contact__name")
            or soup.select_one("div.b-user-info__name")
        )
        ad.seller_name = name_el.get_text(strip=True) if name_el else ""

        # ── Market price range ────────────────────────────────────────────────
        # Jiji renders "Market price: USh 74.5 M ~ 79.2 M" near the price block
        market_el = (
            soup.select_one("div.b-advert-price__market")
            or soup.select_one("span.b-advert-price__market")
            or soup.find(string=re.compile(r"[Mm]arket\s+price", re.I))
        )
        if market_el:
            raw = market_el if isinstance(market_el, str) else market_el.get_text()
            ad.market_price_low, ad.market_price_high = parse_market_range(raw)
            log.debug(
                "  Market range '%s': %s ~ %s",
                ad.title[:40], ad.market_price_low, ad.market_price_high,
            )

        # ── Better image ──────────────────────────────────────────────────────
        if not ad.image_url:
            og = soup.find("meta", property="og:image")
            if og:
                ad.image_url = og.get("content", "")

        ad.evaluate_deal()
        return ad

    except PlaywrightTimeout:
        log.warning("Timeout enriching: %s", ad.ad_url)
        return None
    except Exception as exc:
        log.error("Enrich error for %s: %s", ad.ad_url, exc)
        return None
    finally:
        await page.close()


def _extract_seller_count(soup: BeautifulSoup) -> int:
    """
    Multiple CSS/text patterns to detect total ads from this seller.
    Defaults to 1 (private seller) if undetectable.
    """
    # Pattern 1 — "See all ads (N)" or "All ads (N)"
    for a in soup.select("a"):
        m = re.search(r"[Aa]ll\s+ads?\s*\((\d+)\)", a.get_text(strip=True))
        if m:
            return int(m.group(1))

    # Pattern 2 — seller block text like "12 ads"
    for el in soup.select(
        "div.b-seller-block, div.b-user-info, div.b-advert-contact, div.seller-info"
    ):
        m = re.search(r"(\d+)\s+[Aa]ds?", el.get_text(" ", strip=True))
        if m:
            return int(m.group(1))

    # Pattern 3 — data attribute
    el = soup.find(attrs={"data-ads-count": True})
    if el:
        try:
            return int(el["data-ads-count"])
        except (ValueError, TypeError):
            pass

    log.debug("Seller ad count not found; defaulting to 1")
    return 1


# ── Semaphore-bounded batch enrichment ───────────────────────────────────────
async def enrich_all(context, ads: list[Ad]) -> list[Ad]:
    sem = asyncio.Semaphore(CONCURRENCY)

    async def _bounded(ad):
        async with sem:
            return await enrich_ad(context, ad)

    results = await asyncio.gather(*[_bounded(ad) for ad in ads])
    return [r for r in results if r is not None]


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
async def send_telegram_alert(ad: Ad) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials missing — skipping alert.")
        return

    price_str = f"USh {ad.price:,}" if ad.price else "Price not listed"
    mkt = ""
    if ad.market_price_low and ad.market_price_high:
        mkt = f"\n📊 Jiji range: USh {ad.market_price_low:,} ~ {ad.market_price_high:,}"
    elif ad.market_price_low:
        mkt = f"\n📊 Jiji floor: USh {ad.market_price_low:,}"

    text = (
        f"🔥 *DEAL ALERT — {ad.category.upper()}*\n\n"
        f"*{ad.title}*\n"
        f"💰 {price_str}{mkt}\n"
        f"👤 Seller: {ad.seller_name} ({ad.seller_ad_count} total ad(s))\n"
        f"📍 {ad.location}\n"
        f"📌 _{ad.deal_reason}_\n\n"
        f"[View on Jiji]({ad.ad_url})"
    )

    import urllib.request, json

    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": False,
    }).encode()

    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if not result.get("ok"):
                log.error("Telegram error: %s", result)
            else:
                log.info("Telegram alert sent: %s", ad.title)
    except Exception as exc:
        log.error("Telegram request failed: %s", exc)


# ── Entry point ───────────────────────────────────────────────────────────────
async def main() -> None:
    supabase = get_supabase()
    candidates: list[Ad] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
        )

        # ── 1. Collect candidates from search pages ───────────────────────────
        search_page = await context.new_page()
        for item in SEARCH_QUERIES:
            for pnum in range(1, MAX_PAGES + 1):
                url = JIJI_SEARCH_URL.format(
                    query=item["query"].replace(" ", "+"), page=pnum
                )
                log.info("Search '%s' page %d → %s", item["query"], pnum, url)
                batch = await scrape_search_page(
                    search_page, url, item["category"], item["query"]
                )
                if not batch:
                    log.info("  No results, stopping pagination for '%s'", item["query"])
                    break
                candidates.extend(batch)
        await search_page.close()
        log.info("Total candidates from search: %d", len(candidates))

        # ── 2. Skip URLs already in DB ────────────────────────────────────────
        try:
            existing = supabase.table("jiji_deals").select("ad_url").execute()
            existing_urls = {r["ad_url"] for r in existing.data}
        except Exception as exc:
            log.warning("Could not fetch existing URLs: %s", exc)
            existing_urls = set()

        new_ads = [a for a in candidates if a.ad_url not in existing_urls]
        log.info("New (not yet in DB): %d", len(new_ads))

        # ── 3. Enrich: visit detail page, filter brokers ──────────────────────
        enriched = await enrich_all(context, new_ads)
        await browser.close()

    log.info(
        "After broker filter (≤%d ads): kept %d / %d",
        MAX_SELLER_ADS, len(enriched), len(new_ads),
    )

    # ── 4. Save to Supabase ───────────────────────────────────────────────────
    inserted, skipped = upsert_ads(supabase, enriched)
    log.info("DB — inserted: %d, duplicate-skipped: %d", inserted, skipped)

    # ── 5. Telegram alerts for deals ─────────────────────────────────────────
    deals = [a for a in enriched if a.is_deal]
    log.info("Deals this run: %d", len(deals))
    for ad in deals:
        await send_telegram_alert(ad)


if __name__ == "__main__":
    asyncio.run(main())
