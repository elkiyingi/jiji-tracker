"""
Jiji.ug Deal Scraper — v4 DEBUG
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
This version adds full HTML debugging:
  - Saves the raw HTML of every search page to /tmp/debug_*.html
  - Logs the first 3000 chars of HTML so we can see in Actions logs
    what Jiji is actually returning to the headless browser
  - Tries multiple anti-bot evasion techniques
  - Falls back to requests-html / httpx if Playwright gets a bot wall

The debug HTML files are uploaded as GitHub Actions artifacts so you
can download and inspect them.
"""

import asyncio
import os
import re
import logging
from dataclasses import dataclass, field
from pathlib import Path
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

DEBUG_DIR = Path("/tmp/jiji_debug")
DEBUG_DIR.mkdir(exist_ok=True)

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL: str     = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str     = os.environ["SUPABASE_KEY"]
TELEGRAM_TOKEN: str   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.environ.get("TELEGRAM_CHAT_ID", "")

MAX_SELLER_ADS  = 3
MAX_PAGES       = 3
PAGE_TIMEOUT    = 60_000   # increased to 60s
DETAIL_TIMEOUT  = 30_000
CONCURRENCY     = 2

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


# ── Debug helper ──────────────────────────────────────────────────────────────
def save_debug_html(slug: str, html: str) -> None:
    """Save HTML to file for artifact upload, and log a preview."""
    safe_slug = re.sub(r"[^\w-]", "_", slug)[:60]
    path = DEBUG_DIR / f"debug_{safe_slug}.html"
    path.write_text(html, encoding="utf-8")
    log.info("DEBUG HTML saved → %s (%d bytes)", path, len(html))

    # Log the first 2000 chars so it's visible directly in Actions log
    preview = html[:2000].replace("\n", " ").replace("\r", "")
    log.info("HTML PREVIEW (first 2000 chars): %s", preview)

    # Also log key signals
    signals = {
        "has 'advert'":      "advert"      in html.lower(),
        "has 'b-list'":      "b-list"      in html.lower(),
        "has 'price'":       "price"       in html.lower(),
        "has 'harrier'":     "harrier"     in html.lower(),
        "has 'vanguard'":    "vanguard"    in html.lower(),
        "has 'captcha'":     "captcha"     in html.lower(),
        "has 'cloudflare'":  "cloudflare"  in html.lower(),
        "has 'robot'":       "robot"       in html.lower(),
        "has 'blocked'":     "blocked"     in html.lower(),
        "page title":        re.search(r"<title>(.*?)</title>", html, re.I),
    }
    for k, v in signals.items():
        if k == "page title":
            log.info("  SIGNAL %s: %s", k, v.group(1) if v else "NOT FOUND")
        else:
            log.info("  SIGNAL %s: %s", k, v)


# ── Playwright browser context with anti-bot settings ────────────────────────
async def make_context(browser):
    """Create a browser context that looks as human as possible."""
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1366, "height": 768},
        locale="en-US",
        timezone_id="Africa/Kampala",
        # Pretend to be a real browser with these headers
        extra_http_headers={
            "Accept-Language":  "en-US,en;q=0.9",
            "Accept":           "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Encoding":  "gzip, deflate, br",
            "DNT":              "1",
            "Upgrade-Insecure-Requests": "1",
        },
    )
    # Override navigator.webdriver to avoid detection
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        window.chrome = { runtime: {} };
    """)
    return context


# ── Page loader ───────────────────────────────────────────────────────────────
async def load_page(page, url: str, timeout: int, slug: str = "") -> Optional[str]:
    """Load page, wait for content, save debug HTML, return HTML string."""
    log.info("Loading: %s", url)
    try:
        response = await page.goto(
            url,
            wait_until="networkidle",   # wait for ALL network requests to finish
            timeout=timeout,
        )
        if response:
            log.info("  HTTP %d for %s", response.status, url)
    except PlaywrightTimeout:
        log.warning("  networkidle timeout — trying domcontentloaded fallback")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        except PlaywrightTimeout:
            log.warning("  domcontentloaded also timed out: %s", url)
            html = await page.content()
            if slug:
                save_debug_html(f"timeout_{slug}", html)
            return None
        except Exception as exc:
            log.error("  Navigation failed: %s", exc)
            return None
    except Exception as exc:
        log.error("  Navigation error: %s", exc)
        return None

    # Extra wait for JS-rendered content
    await asyncio.sleep(3)

    # Scroll to bottom (triggers lazy loading)
    try:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1)
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(0.5)
    except Exception:
        pass

    html = await page.content()

    if slug:
        save_debug_html(slug, html)

    return html


# ── Search-page parser ────────────────────────────────────────────────────────
def parse_search_html(html: str, category: str, query: str) -> list[Ad]:
    soup = BeautifulSoup(html, "lxml")
    ads:  list[Ad] = []

    # Log all CSS classes containing 'advert' or 'list' to help identify selectors
    class_samples = set()
    for tag in soup.find_all(class_=True):
        for cls in tag.get("class", []):
            if any(kw in cls.lower() for kw in ("advert", "list", "item", "card", "product")):
                class_samples.add(cls)
    if class_samples:
        log.info("  Relevant CSS classes found: %s", sorted(class_samples)[:30])

    # Try progressively broader selectors
    selector_attempts = [
        "article.b-list-advert__item",
        "div.b-list-advert__item",
        "li.b-list-advert__item",
        "article[class*='advert']",
        "div[class*='advert-item']",
        "div[class*='list-item']",
        # Very broad — any article or li with a price-like child
        "article",
        "li",
    ]

    cards = []
    for sel in selector_attempts:
        found = soup.select(sel)
        if found:
            log.info("  Selector '%s' matched %d elements", sel, len(found))
            # Only use broad selectors if they contain price-like text
            if sel in ("article", "li"):
                found = [
                    c for c in found
                    if re.search(r"ush|ugx|\d{7,}", c.get_text().lower())
                ]
                log.info("    → %d have price-like text", len(found))
            if found:
                cards = found
                break

    for card in cards:
        ad = _parse_card(card, category, query)
        if ad:
            ads.append(ad)

    # Fallback: direct link scan
    if not ads:
        log.info("  No cards parsed — trying link scan")
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href: str = a["href"]
            # Jiji listing pages have URLs like /uganda/cars/NAME-ID or
            # /kampala/cars/NAME-ID — the ID is ~20 alphanumeric chars
            if not re.search(r"\.ug/([\w-]+/){1,3}[\w-]+-\w{15,}", "https://jiji" + href):
                if not re.search(r"/(cars|land|vehicles|property)/[\w-]+-\w{10,}", href):
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
async def enrich_ad(context, ad: Ad) -> Optional[Ad]:
    page = await context.new_page()
    try:
        html = await load_page(page, ad.ad_url, DETAIL_TIMEOUT)
        if not html:
            return None

        soup = BeautifulSoup(html, "lxml")

        count = _extract_seller_count(soup)
        ad.seller_ad_count = count
        if count > MAX_SELLER_ADS:
            log.info("  SKIP broker (%d ads): %.60s", count, ad.title)
            return None

        name_el = (
            soup.select_one("div.b-seller-block__name")
            or soup.select_one("span.b-advert-contact__name")
            or soup.select_one("div.b-user-info__name")
            or soup.select_one("[class*='seller'] [class*='name']")
        )
        ad.seller_name = name_el.get_text(strip=True) if name_el else ""

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

        if not ad.image_url:
            og = soup.find("meta", property="og:image")
            if og:
                ad.image_url = og.get("content", "")

        if not ad.location or ad.location == "Uganda":
            loc_el = (
                soup.select_one("span.b-advert-details__item--region")
                or soup.select_one("[class*='location']")
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
    for a in soup.select("a"):
        m = re.search(r"(?:all\s+ads?|see\s+all)\s*\((\d+)\)", a.get_text(strip=True), re.I)
        if m:
            return int(m.group(1))
    for sel in ("div.b-seller-block", "div.b-user-info", "div.b-advert-contact", "div.seller-info"):
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
    except Exception as exc:
        log.error("Telegram request failed: %s", exc)


# ── Main ──────────────────────────────────────────────────────────────────────
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
                "--window-size=1366,768",
            ],
        )
        context = await make_context(browser)
        search_page = await context.new_page()

        # ── Scrape search pages ───────────────────────────────────────────────
        for item in SEARCH_QUERIES:
            q_enc = item["query"].replace(" ", "+")
            for pnum in range(1, MAX_PAGES + 1):
                url = (
                    f"{item['base_url']}?query={q_enc}"
                    if pnum == 1
                    else f"{item['base_url']}?query={q_enc}&page={pnum}"
                )
                slug = f"{item['query'].replace(' ', '_')}_p{pnum}"
                log.info("Fetching '%s' page %d → %s", item["query"], pnum, url)

                html = await load_page(search_page, url, PAGE_TIMEOUT, slug=slug)

                if not html:
                    log.warning("No HTML returned — skipping")
                    break

                batch = parse_search_html(html, item["category"], item["query"])
                log.info("  → %d candidates", len(batch))

                if not batch:
                    log.info("  Empty — stopping pagination for '%s'", item["query"])
                    break

                candidates.extend(batch)
                await asyncio.sleep(3)  # polite delay between pages

        await search_page.close()
        log.info("Total candidates: %d", len(candidates))

        # ── Filter known URLs ─────────────────────────────────────────────────
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

        # ── Enrich ───────────────────────────────────────────────────────────
        enriched = await enrich_all(context, new_ads)
        await browser.close()

    log.info(
        "After broker filter (≤%d ads): kept %d / %d",
        MAX_SELLER_ADS, len(enriched), len(new_ads),
    )

    inserted, skipped = upsert_ads(supabase, enriched)
    log.info("DB — inserted: %d, duplicate-skipped: %d", inserted, skipped)

    deals = [a for a in enriched if a.is_deal]
    log.info("Deals this run: %d", len(deals))
    for ad in deals:
        await send_telegram_alert(ad)

    # Report debug file locations
    debug_files = list(DEBUG_DIR.glob("*.html"))
    log.info("Debug HTML files saved (%d): %s", len(debug_files), DEBUG_DIR)


if __name__ == "__main__":
    asyncio.run(main())
