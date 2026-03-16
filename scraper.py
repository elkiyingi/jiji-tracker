"""
Jiji.ug Deal Scraper — v3
━━━━━━━━━━━━━━━━━━━━━━━━━
Key fixes over v2:
  - Uses correct Jiji category URLs (jiji.ug/cars, jiji.ug/land-plots-for-sale)
    with ?query= param instead of the broken /search endpoint.
  - Waits for page to be fully rendered before scraping, with multiple fallback
    selectors and a link-scan fallback if no card selector matches.
  - Increased timeouts (45s search, 30s detail).
  - Extracts seller ad count and Jiji market price range from the detail page.
  - Broker filter: skip any seller with > MAX_SELLER_ADS total ads.
  - Deal flag: price < Jiji market floor triggers Telegram alert.

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
SUPABASE_URL: str     = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str     = os.environ["SUPABASE_KEY"]
TELEGRAM_TOKEN: str   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.environ.get("TELEGRAM_CHAT_ID", "")

MAX_SELLER_ADS  = 3
MAX_PAGES       = 3
PAGE_TIMEOUT    = 45_000   # ms — Jiji can be slow on GitHub Actions
DETAIL_TIMEOUT  = 30_000
CONCURRENCY     = 2        # keep low to avoid rate-limiting

# ── Search query definitions ──────────────────────────────────────────────────
# Uses correct Jiji category base URLs rather than the broken /search endpoint.
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


# ── Robust page loader ────────────────────────────────────────────────────────
async def load_page_robust(page, url: str, timeout: int) -> Optional[str]:
    """Navigate, wait for JS to render, return HTML. Returns None on failure."""
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
    except PlaywrightTimeout:
        log.warning("domcontentloaded timeout: %s", url)
        return None
    except Exception as exc:
        log.warning("Navigation error for %s: %s", url, exc)
        return None

    # Try a series of selectors that should exist once Jiji renders its cards
    content_selectors = [
        "article.b-list-advert__item",
        "div.b-list-advert__item",
        "div[class*='advert']",
        "a[href*='/uganda/']",
        "div.masonry-item",
        "section.b-list-advert",
    ]
    for sel in content_selectors:
        try:
            await page.wait_for_selector(sel, timeout=8_000)
            break
        except PlaywrightTimeout:
            continue

    # Scroll to bottom to trigger lazy-loaded images/cards
    try:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1.5)
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(0.5)
    except Exception:
        pass

    return await page.content()


# ── Search-page parser ────────────────────────────────────────────────────────
def parse_search_html(html: str, category: str, query: str) -> list[Ad]:
    soup = BeautifulSoup(html, "lxml")
    ads:  list[Ad] = []

    # Primary: article/div card elements
    cards = (
        soup.select("article.b-list-advert__item")
        or soup.select("div.b-list-advert__item")
        or soup.select("li.b-list-advert__item")
    )

    if cards:
        for card in cards:
            ad = _parse_card(card, category, query)
            if ad:
                ads.append(ad)
        return ads

    # Fallback: scan all anchor tags for Jiji listing URLs
    log.debug("No card elements found — falling back to href scan")
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        # Jiji listing URLs contain a slug + short alphanumeric ID at the end
        if re.search(r"/[a-z-]+-\w{6,}$", href) and "/uganda/" not in href:
            # Also accept /kampala/ or /central-division/ paths
            if not re.search(r"/(uganda|kampala|central|wakiso|mukono)/", href):
                continue
        elif "/uganda/" not in href and not re.search(r"/(kampala|central)/", href):
            continue

        full_url = href if href.startswith("http") else f"https://jiji.ug{href}"
        if full_url in seen or "jiji.ug" not in full_url:
            continue
        seen.add(full_url)

        # Grab the closest text we can find as a title
        title_candidate = a.get_text(strip=True)[:200]
        if len(title_candidate) < 5:
            # Try parent container
            title_candidate = (a.parent or a).get_text(" ", strip=True)[:200].strip()

        ads.append(Ad(
            title=title_candidate or "Untitled",
            price=None, location="Uganda", image_url="",
            ad_url=full_url, category=category, query=query,
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
async def enrich_ad(context, ad: Ad) -> Optional[Ad]:
    page = await context.new_page()
    try:
        html = await load_page_robust(page, ad.ad_url, DETAIL_TIMEOUT)
        if not html:
            return None

        soup = BeautifulSoup(html, "lxml")

        # ── Seller ad count ───────────────────────────────────────────────────
        count = _extract_seller_count(soup)
        ad.seller_ad_count = count
        if count > MAX_SELLER_ADS:
            log.info("  SKIP broker (%d ads): %.60s", count, ad.title)
            return None

        # ── Seller name ───────────────────────────────────────────────────────
        name_el = (
            soup.select_one("div.b-seller-block__name")
            or soup.select_one("span.b-advert-contact__name")
            or soup.select_one("div.b-user-info__name")
            or soup.select_one("[class*='seller'] [class*='name']")
        )
        ad.seller_name = name_el.get_text(strip=True) if name_el else ""

        # ── Price (more reliable from detail page) ────────────────────────────
        if not ad.price:
            price_el = (
                soup.select_one("span.b-advert-price__converted")
                or soup.select_one("div.b-advert-price")
                or soup.select_one("[class*='price']")
            )
            if price_el:
                ad.price = parse_ugx(price_el.get_text(strip=True))

        # ── Market price range ────────────────────────────────────────────────
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
                    sibling_text = tag.parent.parent.get_text(" ", strip=True)
                    if "~" in sibling_text:
                        market_text = sibling_text
                        break

        if market_text:
            ad.market_price_low, ad.market_price_high = parse_market_range(market_text)
            if ad.market_price_low:
                log.info(
                    "  Market %s ~ %s for: %.45s",
                    f"USh {ad.market_price_low:,}",
                    f"USh {ad.market_price_high:,}" if ad.market_price_high else "?",
                    ad.title,
                )

        # ── Better image ──────────────────────────────────────────────────────
        if not ad.image_url:
            og = soup.find("meta", property="og:image")
            if og:
                ad.image_url = og.get("content", "")

        # ── Better location ───────────────────────────────────────────────────
        if not ad.location or ad.location == "Uganda":
            loc_el = (
                soup.select_one("span.b-advert-details__item--region")
                or soup.select_one("[class*='location']")
                or soup.select_one("[class*='region']")
            )
            if loc_el:
                ad.location = loc_el.get_text(strip=True)

        ad.evaluate_deal()
        return ad

    except Exception as exc:
        log.error("Enrich error for %s: %s", ad.ad_url, exc)
        return None
    finally:
        await page.close()


def _extract_seller_count(soup: BeautifulSoup) -> int:
    # Pattern 1 — anchor text like "See all ads (12)"
    for a in soup.select("a"):
        m = re.search(r"(?:all\s+ads?|see\s+all)\s*\((\d+)\)", a.get_text(strip=True), re.I)
        if m:
            return int(m.group(1))

    # Pattern 2 — seller block text
    for sel in ("div.b-seller-block", "div.b-user-info", "div.b-advert-contact", "div.seller-info"):
        for el in soup.select(sel):
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

    # Pattern 4 — broad page text
    m = re.search(r"(\d+)\s+active\s+ads?", soup.get_text(" "), re.I)
    if m:
        return int(m.group(1))

    log.debug("Seller ad count not found — defaulting to 1")
    return 1


# ── Batch enrichment ─────────────────────────────────────────────────────────
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
        f"👤 {ad.seller_name} ({ad.seller_ad_count} total ad(s))\n"
        f"📍 {ad.location}\n"
        f"📌 _{ad.deal_reason}_\n\n"
        f"[View on Jiji]({ad.ad_url})"
    )

    import urllib.request, json
    payload = json.dumps({
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       text,
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
                log.info("Telegram alert sent: %.60s", ad.title)
    except Exception as exc:
        log.error("Telegram request failed: %s", exc)


# ── Entry point ───────────────────────────────────────────────────────────────
async def main() -> None:
    supabase    = get_supabase()
    candidates: list[Ad] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="en-GB",
        )

        # ── 1. Scrape search/category pages ───────────────────────────────────
        search_page = await context.new_page()

        for item in SEARCH_QUERIES:
            q_enc = item["query"].replace(" ", "+")
            for pnum in range(1, MAX_PAGES + 1):
                url = (
                    f"{item['base_url']}?query={q_enc}"
                    if pnum == 1
                    else f"{item['base_url']}?query={q_enc}&page={pnum}"
                )
                log.info("Fetching '%s' page %d → %s", item["query"], pnum, url)
                html = await load_page_robust(search_page, url, PAGE_TIMEOUT)

                if not html:
                    log.warning("No HTML — skipping page %d for '%s'", pnum, item["query"])
                    break

                batch = parse_search_html(html, item["category"], item["query"])
                log.info("  → %d candidates", len(batch))

                if not batch:
                    log.info("  Empty — stopping pagination for '%s'", item["query"])
                    break

                candidates.extend(batch)
                await asyncio.sleep(2)

        await search_page.close()
        log.info("Total candidates: %d", len(candidates))

        # ── 2. Filter already-known URLs ──────────────────────────────────────
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

        log.info("New (not yet in DB): %d", len(new_ads))

        # ── 3. Enrich + broker filter ─────────────────────────────────────────
        enriched = await enrich_all(context, new_ads)
        await browser.close()

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
        await send_telegram_alert(ad)


if __name__ == "__main__":
    asyncio.run(main())
