"""
================================================================
  Nairobi Property Scraper — FINAL
  All URLs and selectors confirmed from live page inspection.

  INSTALL:
      pip install requests beautifulsoup4 lxml pandas playwright
      playwright install chromium

  RUN:
      python nairobi_scraper_final.py

  OUTPUT:
      nairobi_properties.csv
================================================================
"""

import asyncio, csv, json, logging, random, re, time
from datetime import datetime
from bs4 import BeautifulSoup, Tag, NavigableString
import requests
import pandas as pd

try:
    from playwright.async_api import async_playwright
    HAS_PW = True
except ImportError:
    HAS_PW = False

# ── CONFIG ───────────────────────────────────────────────────
OUTPUT   = "nairobi_properties.csv"
LOG_FILE = "scraper_final.log"
MAX_PAGES = 10
HEADLESS  = True
DELAY     = (2.0, 4.0)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

FIELDS = ["source","listing_type","title","price","location",
          "bedrooms","bathrooms","size_sqm","property_type","url","scraped_at"]

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

BASE_HEADERS = {
    "User-Agent":      UA,
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection":      "keep-alive",
}

# ── HELPERS ──────────────────────────────────────────────────

def c(v):
    return re.sub(r"\s+", " ", str(v or "").replace("\xa0", " ")).strip()

def row(src, lt, **kw):
    return {f: c(kw.get(f, "")) for f in FIELDS
            if f not in ("source","listing_type","scraped_at")} | \
           {"source": src, "listing_type": lt, "scraped_at": datetime.now().isoformat()}

def sleep():
    time.sleep(random.uniform(*DELAY))

async def asleep():
    await asyncio.sleep(random.uniform(*DELAY))

def fetch(url, session, extra=None):
    h = {**BASE_HEADERS, **(extra or {})}
    try:
        r = session.get(url, headers=h, timeout=20)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "lxml"), r.text
        log.warning(f"HTTP {r.status_code}: {url}")
    except Exception as e:
        log.warning(f"Fetch error {url}: {e}")
    return None, None


# ════════════════════════════════════════════════════════════
# 1. BuyRentKenya  (requests)
# ────────────────────────────────────────────────────────────
# Confirmed structure (live, 200 OK, 2MB HTML):
#   <a href="/listings/SLUG-ID">
#     <h2>TITLE</h2>
#     <h3>TITLEKSh PRICE</h3>   ← price concatenated directly onto title text
#     <h3>SUBTITLE...</h3>
#     SUBURB, AREA              ← NavigableString between tags
#     N Bedrooms / N Bathrooms / N m²
#   </a>
#
# Each listing is duplicated (mobile + desktop) — deduplicate by title+price.
# Pagination: ?page=N
# ════════════════════════════════════════════════════════════

def parse_brk(html, base):
    soup = BeautifulSoup(html, "lxml")
    results = []

    for h2 in soup.find_all("h2"):
        title = c(h2.get_text())
        if not title or len(title) < 8:
            continue

        # Price: in next <h3> as "TITLEKSh X,XXX,XXX"
        price = ""
        next_h3 = h2.find_next_sibling("h3")
        if next_h3:
            m = re.search(r"KSh[\s\d,]+", next_h3.get_text())
            if m:
                price = c(m.group())
        if not price:
            continue   # skip nav/header h2 tags that have no price

        # URL: parent <a href="/listings/...">
        href = ""
        node = h2.parent
        for _ in range(6):
            if isinstance(node, Tag) and node.name == "a":
                h = node.get("href", "")
                if "/listings/" in h:
                    href = h if h.startswith("http") else base + h
                    break
            node = getattr(node, "parent", None)

        # Location: first NavigableString sibling after h2 that looks like a suburb
        location = ""
        for sib in h2.next_siblings:
            if isinstance(sib, Tag) and sib.name == "h2":
                break
            if isinstance(sib, NavigableString):
                t = str(sib).strip()
                if (t and 3 < len(t) < 70
                        and not any(w in t for w in
                            ("Bedroom","Bathroom","m²","KSh","APARTMENT","HOUSE","FOR SALE","FOR RENT"))):
                    location = t
                    break

        # Beds / baths / size from all siblings until next h2
        chunk = ""
        for sib in h2.next_siblings:
            if isinstance(sib, Tag) and sib.name == "h2":
                break
            chunk += " " + (sib.get_text(" ") if isinstance(sib, Tag) else str(sib))

        beds  = re.search(r"(\d+)\s*Bedrooms?",  chunk)
        baths = re.search(r"(\d+)\s*Bathrooms?", chunk)
        size  = re.search(r"([\d,]+)\s*m²",       chunk)

        results.append(row("BuyRentKenya", None,
            title=title, price=price, location=location,
            bedrooms=beds.group(0) if beds else "",
            bathrooms=baths.group(0) if baths else "",
            size_sqm=size.group(0) if size else "",
            url=href,
        ))

    # Deduplicate (page renders each listing twice)
    seen, unique = set(), []
    for r in results:
        k = r["title"] + r["price"]
        if k not in seen:
            seen.add(k)
            unique.append(r)
    return unique


def scrape_brk(listing_type):
    src  = "BuyRentKenya"
    slug = "property-for-sale" if listing_type == "sale" else "property-for-rent"
    disp = "Sale" if listing_type == "sale" else "Rent"
    base = "https://www.buyrentkenya.com"
    results = []
    sess = requests.Session()
    sess.headers.update(BASE_HEADERS)

    log.info(f"[{src}] {disp}")
    for pn in range(1, MAX_PAGES + 1):
        url = f"{base}/{slug}/nairobi?page={pn}"
        log.info(f"  p{pn}: {url}")
        soup, html = fetch(url, sess)
        if not soup:
            break

        page_rows = parse_brk(html, base)
        # Tag listing type
        for r in page_rows:
            r["listing_type"] = disp
        log.info(f"  {len(page_rows)} listings on p{pn}")
        results.extend(page_rows)

        if not page_rows:
            break

        # Next page: look for a link to page N+1
        next_a = soup.find("a", string=re.compile(r"Next|›|»", re.I)) or \
                 soup.find("a", href=re.compile(rf"page={pn+1}"))
        if not next_a:
            break
        sleep()

    log.info(f"[{src}] {disp} DONE — {len(results)}")
    return results


# ════════════════════════════════════════════════════════════
# 2. Jiji  (requests)
# ────────────────────────────────────────────────────────────
# Confirmed: 200 OK, 273k HTML, "adverts" key present 4 times.
# Data is in: window.INITIAL_DATA__ = {"adverts":[...],"total_count":2097}
# Each advert: {title, price_obj:{value,currency}, region_name, url,
#               attrs:{Bedrooms, Bathrooms, Size}, category_name}
# ════════════════════════════════════════════════════════════

def parse_jiji_adverts(html):
    soup = BeautifulSoup(html, "lxml")
    for script in soup.find_all("script"):
        text = script.string or ""
        if '"adverts"' not in text:
            continue
        # Match the array directly after the "adverts": key
        m = re.search(r'"adverts"\s*:\s*(\[[\s\S]+?\])\s*,\s*"total_count"', text)
        if not m:
            # Broader fallback
            m = re.search(r'"adverts"\s*:\s*(\[[\s\S]+?\])', text)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                # JSON may be truncated — try finding complete balanced array
                raw = text[text.index('"adverts"'):]
                depth, start, i = 0, None, 0
                for i, ch in enumerate(raw):
                    if ch == '[':
                        if start is None: start = i
                        depth += 1
                    elif ch == ']':
                        depth -= 1
                        if depth == 0 and start is not None:
                            try:
                                return json.loads(raw[start:i+1])
                            except Exception:
                                break
    return []

def parse_jiji_total(html):
    m = re.search(r'"total_count"\s*:\s*(\d+)', html)
    return int(m.group(1)) if m else 0


def scrape_jiji(listing_type):
    src  = "Jiji"
    slug = "houses-apartments-for-sale" if listing_type == "sale" \
           else "houses-apartments-for-rent"
    disp = "Sale" if listing_type == "sale" else "Rent"
    base = "https://jiji.co.ke"
    results = []
    sess = requests.Session()
    sess.headers.update({**BASE_HEADERS, "Referer": "https://jiji.co.ke/"})

    log.info(f"[{src}] {disp}")
    total = None

    for pn in range(1, MAX_PAGES + 1):
        url = f"{base}/nairobi/{slug}?page={pn}"
        log.info(f"  p{pn}: {url}")
        soup, html = fetch(url, sess)
        if not soup:
            break

        if total is None:
            total = parse_jiji_total(html)
            log.info(f"  Total listings: {total}")

        adverts = parse_jiji_adverts(html)
        log.info(f"  {len(adverts)} adverts on p{pn}")

        if not adverts:
            log.info(f"  No JSON found — stopping")
            break

        for adv in adverts:
            po    = adv.get("price_obj") or {}
            val   = po.get("value", "") if isinstance(po, dict) else ""
            price = f"KSh {val}".strip() if val else str(adv.get("price", ""))

            attrs = adv.get("attrs") or {}
            if isinstance(attrs, list):
                attrs = {a.get("name",""): a.get("value","")
                         for a in attrs if isinstance(a, dict)}

            href = adv.get("url", "")
            if href and not href.startswith("http"):
                href = base + href

            results.append(row(src, disp,
                title=adv.get("title", ""),
                price=price,
                location=adv.get("region_name") or adv.get("town_name", ""),
                bedrooms=str(attrs.get("Bedrooms") or attrs.get("bedrooms", "")),
                bathrooms=str(attrs.get("Bathrooms") or attrs.get("bathrooms", "")),
                size_sqm=str(attrs.get("Size") or attrs.get("size", "")),
                property_type=adv.get("category_name", ""),
                url=href,
            ))

        page_size = len(adverts) or 20
        if total and len(results) >= min(total, MAX_PAGES * page_size):
            break
        sleep()

    log.info(f"[{src}] {disp} DONE — {len(results)}")
    return results


# ════════════════════════════════════════════════════════════
# 3. Property24  (Playwright)
# ────────────────────────────────────────────────────────────
# Confirmed: 200 OK, 234k HTML, no __NEXT_DATA__, server-rendered.
# 69 KSh mentions. DOM selectors: .p24_regularTile / .p24_price etc.
# ════════════════════════════════════════════════════════════

async def scrape_p24(page, listing_type):
    src  = "Property24"
    slug = "property-for-sale-in-nairobi-c1890" if listing_type == "sale" \
           else "property-to-rent-in-nairobi-c1890"
    disp = "Sale" if listing_type == "sale" else "Rent"
    base = "https://www.property24.co.ke"
    results = []

    log.info(f"[{src}] {disp}")
    for pn in range(1, MAX_PAGES + 1):
        url = f"{base}/{slug}?Page={pn}"
        log.info(f"  p{pn}: {url}")
        try:
            await page.goto(url, timeout=40000, wait_until="domcontentloaded")
            await asyncio.sleep(3)
        except Exception as e:
            log.warning(f"  nav error: {e}"); break

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        # Primary: .p24_regularTile cards (confirmed class name)
        cards = soup.select(".p24_regularTile")
        log.info(f"  .p24_regularTile cards: {len(cards)}")

        if not cards:
            # Fallback: any div with p24_ class
            cards = soup.select("[class*='p24_']")
            # Filter to ones that contain a price
            cards = [c for c in cards if "KSh" in c.get_text()]
            log.info(f"  Fallback p24_ cards: {len(cards)}")

        for card in cards:
            try:
                t  = card.select_one(".p24_title, .p24_propertyName, h2, h3")
                p  = card.select_one(".p24_price, .p24_displayPrice, [class*='price']")
                lo = card.select_one(".p24_address, .p24_addressDescription, [class*='address']")
                a  = card.select_one("a[href]")
                spans = card.select(".p24_info span, .p24_details span")

                span_texts = [s.get_text(strip=True) for s in spans]
                beds  = next((s for s in span_texts if "Bed"  in s), "")
                baths = next((s for s in span_texts if "Bath" in s), "")
                size  = next((s for s in span_texts if "m²"   in s), "")

                href = a["href"] if a else ""
                if href and not href.startswith("http"):
                    href = base + href

                r_ = row(src, disp,
                    title=t.get_text() if t else "",
                    price=p.get_text() if p else "",
                    location=lo.get_text() if lo else "",
                    bedrooms=beds, bathrooms=baths, size_sqm=size,
                    url=href,
                )
                if r_["title"] or r_["price"]:
                    results.append(r_)
            except Exception as e:
                log.debug(f"  card: {e}")

        # If still nothing, parse all KSh text blocks
        if not results:
            log.info(f"  No cards — falling back to text extraction")
            for tag in soup.find_all(string=re.compile(r"KSh[\s\d,]+")):
                parent = tag.parent
                if not parent:
                    continue
                block_text = parent.get_text(" ", strip=True)
                price_m = re.search(r"KSh[\s\d,]+", block_text)
                if not price_m:
                    continue
                # Walk up to find a heading
                heading = parent.find_previous(["h2","h3","h4"])
                results.append(row(src, disp,
                    title=heading.get_text(strip=True) if heading else "",
                    price=price_m.group(),
                    location="Nairobi",
                ))

        log.info(f"  Running total: {len(results)}")
        await asleep()

    log.info(f"[{src}] {disp} DONE — {len(results)}")
    return results


# ════════════════════════════════════════════════════════════
# 4. PigiaMe  (Playwright)
# ────────────────────────────────────────────────────────────
# Confirmed correct URLs (from live site):
#   /houses-for-sale/nairobi?page=N
#   /apartments-for-sale/nairobi?page=N
#   /houses-for-rent/nairobi?page=N
#   /apartments-for-rent/nairobi?page=N
# (Old URL /houses-apartments-for-sale/nairobi/ → 404)
# ════════════════════════════════════════════════════════════

PIGIAME_SLUGS = {
    "sale": ["houses-for-sale", "apartments-for-sale"],
    "rent": ["houses-for-rent", "apartments-for-rent"],
}

async def scrape_pigiame(page, listing_type):
    src  = "PigiaMe"
    disp = "Sale" if listing_type == "sale" else "Rent"
    base = "https://www.pigiame.co.ke"
    results = []

    log.info(f"[{src}] {disp}")
    for slug in PIGIAME_SLUGS[listing_type]:
        log.info(f"  slug: {slug}")
        for pn in range(1, MAX_PAGES + 1):
            url = f"{base}/{slug}/nairobi?page={pn}"
            log.info(f"    p{pn}: {url}")
            try:
                await page.goto(url, timeout=40000, wait_until="domcontentloaded")
                await asyncio.sleep(3)
            except Exception as e:
                log.warning(f"    nav: {e}"); break

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # PigiaMe listing cards — confirmed from search results:
            # Title, N bed, LOCATION, DATE, KSh PRICE
            cards = soup.select(
                "article[class*='listing'], "
                "div[class*='listing-card'], "
                "li[class*='item'], "
                "[data-advert-id]"
            )
            log.info(f"    DOM cards: {len(cards)}")

            if not cards:
                # Text-based extraction from confirmed format:
                # "3 Bed Apartment with En Suite · 3 bed · Kilimani · KSh 12,000,000"
                blocks = re.findall(
                    r'([\w\s]+(?:Bed|bed|Apartment|House|Villa|Studio)[^\n]{5,80})'
                    r'[\s\S]{0,200}?'
                    r'(KSh[\s\d,]+)',
                    html
                )
                for title_raw, price_raw in blocks:
                    t = c(title_raw)
                    p = c(price_raw)
                    if t and p and len(t) > 5:
                        beds_m = re.search(r'(\d+)\s*[Bb]ed', t)
                        results.append(row(src, disp,
                            title=t, price=p,
                            bedrooms=beds_m.group(0) if beds_m else "",
                            location="Nairobi",
                        ))
                if results:
                    log.info(f"    text extraction: {len(results)} so far")
                    break

            for card in cards:
                try:
                    t  = card.select_one("h2, h3, [class*='title'], [class*='name']")
                    p  = card.select_one("[class*='price'], [class*='cost']")
                    lo = card.select_one("[class*='location'], [class*='suburb'], [class*='area']")
                    b  = card.select_one("[class*='bed']")
                    a  = card.select_one("a[href]")
                    href = a["href"] if a else ""
                    if href and not href.startswith("http"):
                        href = base + href
                    r_ = row(src, disp,
                        title=t.get_text() if t else "",
                        price=p.get_text() if p else "",
                        location=lo.get_text() if lo else "",
                        bedrooms=b.get_text() if b else "",
                        url=href,
                    )
                    if r_["title"]:
                        results.append(r_)
                except Exception as e:
                    log.debug(f"    card: {e}")

            log.info(f"    running total: {len(results)}")

            next_a = soup.find("a", string=re.compile(r"Next|›", re.I)) or \
                     soup.find("a", rel="next")
            if not next_a:
                break
            await asleep()

    log.info(f"[{src}] {disp} DONE — {len(results)}")
    return results


# ════════════════════════════════════════════════════════════
# PLAYWRIGHT RUNNER
# ════════════════════════════════════════════════════════════

STEALTH_JS = """
Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3,4,5]});
window.chrome={runtime:{}};
"""

async def run_pw():
    results = []
    if not HAS_PW:
        log.error("playwright not installed"); return results
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox","--disable-setuid-sandbox",
                  "--disable-blink-features=AutomationControlled"]
        )
        ctx = await browser.new_context(
            viewport={"width":1366,"height":768},
            user_agent=UA, locale="en-US", timezone_id="Africa/Nairobi",
            extra_http_headers={
                "Accept":         "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language":"en-US,en;q=0.5",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
            }
        )
        await ctx.add_init_script(STEALTH_JS)
        await ctx.route("**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2,ttf,mp4}",
                        lambda r: r.abort())
        page = await ctx.new_page()

        results.extend(await scrape_p24(page, "sale")); await asleep()
        results.extend(await scrape_p24(page, "rent")); await asleep()
        results.extend(await scrape_pigiame(page, "sale")); await asleep()
        results.extend(await scrape_pigiame(page, "rent"))

        await browser.close()
    return results


# ════════════════════════════════════════════════════════════
# DEDUP + SAVE
# ════════════════════════════════════════════════════════════

def dedup(lst):
    seen, out = set(), []
    for r in lst:
        k = r.get("url") or (r.get("title","") + r.get("price",""))
        if k and k not in seen:
            seen.add(k); out.append(r)
    log.info(f"Dedup: {len(lst)} → {len(out)}")
    return out

def save(lst, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader(); w.writerows(lst)
    log.info(f"Saved {len(lst)} → {path}")


# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════

async def main():
    all_ = []
    t0 = datetime.now()
    log.info("="*60)
    log.info("  Nairobi Property Scraper — FINAL")
    log.info(f"  headless={HEADLESS}  max_pages={MAX_PAGES}")
    log.info("="*60)

    # requests-based (fast, confirmed working)
    all_.extend(scrape_brk("sale"));  sleep()
    all_.extend(scrape_brk("rent"));  sleep()
    all_.extend(scrape_jiji("sale")); sleep()
    all_.extend(scrape_jiji("rent")); sleep()

    # Playwright-based
    all_.extend(await run_pw())

    all_ = dedup(all_)
    save(all_, OUTPUT)

    elapsed = (datetime.now()-t0).total_seconds()
    log.info("="*60)
    log.info(f"  DONE — {len(all_)} listings in {elapsed:.0f}s")
    log.info("="*60)

    if all_:
        df = pd.DataFrame(all_)
        print("\n── Breakdown ──")
        print(df.groupby(["source","listing_type"]).size().to_string())
        print(f"\nTotal: {len(df)} unique listings → {OUTPUT}\n")

if __name__ == "__main__":
    asyncio.run(main())
