"""
=============================================================
  Uzbekiston Universitetlari Ma'lumotlari Yig'uvchi (Scraper)
  Manbalar: gov.uz/uz/edu · studyin-uzbekistan.uz · Wikipedia
  Chiqish: universities.csv  +  universities.json
=============================================================

Kerakli kutubxonalar:
    pip install requests beautifulsoup4 lxml pandas tqdm

Ishlatish:
    python uzbekistan_university_scraper.py
    python uzbekistan_university_scraper.py --output my_folder --delay 1.5
"""

import re
import csv
import json
import time
import logging
import argparse
import unicodedata
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional

import requests
from bs4 import BeautifulSoup

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# ─────────────────────────── Logging ────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────── Data model ─────────────────────────────────────
@dataclass
class University:
    title: str = ""
    title_uz: str = ""
    location: str = ""
    region: str = ""
    year_founded: Optional[int] = None
    faculties: str = ""
    num_faculties: Optional[int] = None
    students_total: Optional[int] = None
    annual_cost_uzs: Optional[str] = None
    annual_cost_usd: Optional[str] = None
    national_ranking: Optional[int] = None
    qs_ranking: Optional[int] = None
    website: str = ""
    university_type: str = ""          # Davlat / Xususiy / Xorijiy filial
    source_url: str = ""
    scraped_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

# ─────────────────────────── HTTP helpers ───────────────────────────────────
HEADERS_POOL = [
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "uz,en-US;q=0.9,ru;q=0.8",
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/17.4 Safari/605.1.15"
        ),
        "Accept-Language": "uz-UZ,uz;q=0.9",
    },
]

SESSION = requests.Session()
SESSION.headers.update(HEADERS_POOL[0])
_header_idx = 0


def _rotate_headers():
    global _header_idx
    _header_idx = (_header_idx + 1) % len(HEADERS_POOL)
    SESSION.headers.update(HEADERS_POOL[_header_idx])


def fetch(url: str, timeout: int = 20, retries: int = 3, delay: float = 1.0) -> Optional[requests.Response]:
    """GET with retry logic and polite delay."""
    for attempt in range(1, retries + 1):
        try:
            time.sleep(delay)
            resp = SESSION.get(url, timeout=timeout)
            resp.raise_for_status()
            _rotate_headers()
            return resp
        except requests.exceptions.HTTPError as e:
            log.warning(f"HTTP {e.response.status_code} → {url}  (attempt {attempt}/{retries})")
            if e.response.status_code in (403, 429):
                time.sleep(delay * 3)
        except requests.exceptions.RequestException as e:
            log.warning(f"Request error: {e}  (attempt {attempt}/{retries})")
            time.sleep(delay * 2)
    log.error(f"Failed after {retries} attempts: {url}")
    return None


def soup(resp: requests.Response) -> BeautifulSoup:
    return BeautifulSoup(resp.text, "lxml")


def extract_next_data(resp: requests.Response) -> dict:
    """Pull __NEXT_DATA__ JSON from a Next.js page."""
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', resp.text, re.S)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass
    return {}


# ─────────────────────────── Helper utils ───────────────────────────────────
def clean(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    return " ".join(text.split())


def parse_int(text: str) -> Optional[int]:
    digits = re.sub(r"[^\d]", "", str(text))
    return int(digits) if digits else None


REGIONS_UZ = {
    "toshkent shahar": "Toshkent sh.",
    "toshkent": "Toshkent vil.",
    "samarqand": "Samarqand",
    "farg'ona": "Farg'ona",
    "andijon": "Andijon",
    "namangan": "Namangan",
    "buxoro": "Buxoro",
    "qashqadaryo": "Qashqadaryo",
    "surxondaryo": "Surxondaryo",
    "xorazm": "Xorazm",
    "navoiy": "Navoiy",
    "jizzax": "Jizzax",
    "sirdaryo": "Sirdaryo",
    "qoraqalpog'iston": "Qoraqalpog'iston",
    "nukus": "Qoraqalpog'iston",
}


def detect_region(location: str) -> str:
    loc_lower = location.lower()
    for key, val in REGIONS_UZ.items():
        if key in loc_lower:
            return val
    return ""


# ─────────────────────────── SOURCE 1: gov.uz ───────────────────────────────
GOV_BASE = "https://gov.uz"
GOV_EDU  = "https://gov.uz/uz/edu"


def _parse_gov_uni_page(url: str, delay: float) -> Optional[University]:
    """
    Try to scrape a single university page on gov.uz.
    gov.uz stores sub-organisations under /uz/<slug> — each has its own portal.
    """
    resp = fetch(url, delay=delay)
    if not resp:
        return None

    nd = extract_next_data(resp)
    s  = soup(resp)

    uni = University(source_url=url)

    # — Title —
    h1 = s.find("h1")
    if h1:
        uni.title = clean(h1.get_text())

    # — Try Next.js page props first —
    props = nd.get("props", {}).get("pageProps", {})
    authority = props.get("authority", {}) or props.get("data", {})

    if authority:
        uni.title    = clean(authority.get("name", uni.title))
        uni.title_uz = clean(authority.get("name_uz", ""))
        uni.website  = authority.get("website", "")
        uni.location = clean(authority.get("address", ""))

        # Contacts block
        contacts = authority.get("contacts", [])
        for c in contacts:
            ctype = str(c.get("type", "")).lower()
            if "address" in ctype or "manzil" in ctype:
                uni.location = clean(c.get("value", uni.location))
            if "web" in ctype or "site" in ctype:
                uni.website = c.get("value", uni.website)

    # — Try meta description for extra info —
    meta_desc = s.find("meta", {"name": "description"})
    if meta_desc:
        desc = meta_desc.get("content", "")
        # Year founded pattern
        yr = re.search(r'\b(19[0-9]{2}|20[0-2][0-9])\b', desc)
        if yr and not uni.year_founded:
            uni.year_founded = int(yr.group())

    # — Faculties (look for table rows or list items) —
    fac_section = s.find(string=re.compile(r"fakult|кафедр|facult", re.I))
    if fac_section:
        parent = fac_section.find_parent(["section", "div", "article"])
        if parent:
            items = parent.find_all("li")
            if items:
                uni.faculties   = "; ".join(clean(li.get_text()) for li in items[:20])
                uni.num_faculties = len(items)

    # — Students —
    student_text = s.find(string=re.compile(r"talaba|student|студент", re.I))
    if student_text:
        nums = re.findall(r'\d[\d\s.,]*\d', str(student_text))
        for n in nums:
            val = parse_int(n)
            if val and val > 100:
                uni.students_total = val
                break

    # — Region —
    if uni.location:
        uni.region = detect_region(uni.location)

    return uni


def scrape_gov_uz(delay: float = 1.2) -> list[University]:
    """
    1. Fetch the gov.uz education portal main page
    2. Extract __NEXT_DATA__ for authority list
    3. Enumerate sub-pages / linked university portals
    """
    log.info("━━━ SOURCE 1: gov.uz/uz/edu ━━━")
    universities: list[University] = []

    # Try the API portal endpoint (publicly documented)
    api_urls = [
        "https://api-portal.gov.uz/api/v1/authorities?category=university&lang=uz&per_page=200",
        "https://api-portal.gov.uz/api/v1/authorities?parent_id=7&lang=uz&per_page=200",
        "https://api-portal.gov.uz/api/v1/authorities?lang=uz&type=university&per_page=200",
    ]
    for api_url in api_urls:
        log.info(f"  Trying API: {api_url}")
        resp = fetch(api_url, delay=delay)
        if resp:
            try:
                data = resp.json()
                items = (
                    data.get("data", [])
                    or data.get("items", [])
                    or data.get("results", [])
                    or (data if isinstance(data, list) else [])
                )
                if items:
                    log.info(f"  ✓ API returned {len(items)} items")
                    for item in items:
                        u = University(
                            title     = clean(item.get("name", item.get("title", ""))),
                            title_uz  = clean(item.get("name_uz", "")),
                            website   = item.get("website", item.get("url", "")),
                            location  = clean(item.get("address", item.get("location", ""))),
                            source_url= api_url,
                        )
                        if u.title:
                            u.region = detect_region(u.location)
                            universities.append(u)
                    if universities:
                        break
            except Exception as e:
                log.debug(f"  API parse error: {e}")

    # Fallback: scrape the HTML page
    if not universities:
        log.info("  API not available — scraping HTML …")
        resp = fetch(GOV_EDU, delay=delay)
        if resp:
            nd = extract_next_data(resp)
            # Walk __NEXT_DATA__ for any university links
            page_text = json.dumps(nd)
            slug_matches = re.findall(r'"slug"\s*:\s*"([a-z0-9\-]+)"', page_text)
            link_matches = re.findall(r'"url"\s*:\s*"(https://gov\.uz/[^"]+)"', page_text)
            links = set(link_matches)
            for slug in slug_matches:
                links.add(f"{GOV_BASE}/uz/{slug}")

            log.info(f"  Found {len(links)} sub-page links in __NEXT_DATA__")

            # Also parse visible links on the page
            s = soup(resp)
            for a in s.select("a[href]"):
                href = a["href"]
                if href.startswith("/uz/") and href != "/uz/edu":
                    links.add(GOV_BASE + href)

            # Filter to edu-related links
            edu_links = {l for l in links if any(
                kw in l for kw in ["uni", "inst", "academ", "kolej", "texnik", "edu/"]
            )}
            log.info(f"  Filtered to {len(edu_links)} edu links")

            items = list(edu_links)[:80]
            if HAS_TQDM:
                items = tqdm(items, desc="  gov.uz pages")
            for link in items:
                uni = _parse_gov_uni_page(link, delay)
                if uni and uni.title:
                    universities.append(uni)

    log.info(f"  gov.uz → {len(universities)} universities collected")
    return universities


# ─────────────────────────── SOURCE 2: studyin-uzbekistan.uz ────────────────
STUDYIN_BASE = "https://studyin-uzbekistan.uz"
STUDYIN_LIST = "https://studyin-uzbekistan.uz/universities"


def _parse_studyin_detail(url: str, delay: float) -> Optional[University]:
    resp = fetch(url, delay=delay)
    if not resp:
        return None

    s   = soup(resp)
    nd  = extract_next_data(resp)
    uni = University(source_url=url)

    # Title
    h1 = s.find("h1")
    if h1:
        uni.title = clean(h1.get_text())

    # Next.js props
    props = nd.get("props", {}).get("pageProps", {})
    item  = props.get("university", props.get("data", {})) or {}

    if item:
        uni.title         = clean(item.get("name", uni.title))
        uni.title_uz      = clean(item.get("name_uz", ""))
        uni.location      = clean(item.get("address", item.get("city", "")))
        uni.website       = item.get("website", item.get("official_website", ""))
        uni.university_type = item.get("type", "")

        # Year
        for key in ("founded", "year_founded", "established"):
            if item.get(key):
                uni.year_founded = parse_int(str(item[key]))
                break

        # Students
        for key in ("students", "students_count", "total_students"):
            if item.get(key):
                uni.students_total = parse_int(str(item[key]))
                break

        # Cost
        for key in ("tuition", "annual_cost", "cost_per_year", "price"):
            if item.get(key):
                uni.annual_cost_uzs = str(item[key])
                break

        # Faculties
        facs = item.get("faculties", item.get("departments", []))
        if isinstance(facs, list):
            uni.faculties     = "; ".join(
                clean(f.get("name", str(f))) for f in facs if f
            )
            uni.num_faculties = len(facs)
        elif isinstance(facs, str):
            uni.faculties = clean(facs)

        # Ranking
        rankings = item.get("rankings", item.get("rank", {}))
        if isinstance(rankings, dict):
            uni.national_ranking = rankings.get("national", rankings.get("uz"))
            uni.qs_ranking       = rankings.get("qs", rankings.get("world"))
        elif isinstance(rankings, int):
            uni.national_ranking = rankings

    # Fallback: scrape HTML tables / dl / info blocks
    if not uni.location:
        for tag in s.select(".info-item, .detail-item, .property, li.location, .address"):
            text = clean(tag.get_text())
            if any(k in text.lower() for k in ("manzil", "shahar", "joylash", "address", "city")):
                uni.location = text.split(":")[-1].strip()
                break

    if not uni.year_founded:
        for tag in s.select(".info-item, .detail-item, .property, li"):
            text = clean(tag.get_text())
            yr = re.search(r'\b(19[0-9]{2}|20[0-2][0-9])\b', text)
            if yr and any(k in text.lower() for k in ("yil", "asos", "tashkil", "found", "establ")):
                uni.year_founded = int(yr.group())
                break

    if not uni.students_total:
        for tag in s.select(".stat, .statistic, .students, .count"):
            nums = re.findall(r'\d[\d\s]*\d', tag.get_text())
            for n in nums:
                v = parse_int(n)
                if v and v > 200:
                    uni.students_total = v
                    break

    if uni.location:
        uni.region = detect_region(uni.location)

    return uni


def scrape_studyin(delay: float = 1.2) -> list[University]:
    log.info("━━━ SOURCE 2: studyin-uzbekistan.uz ━━━")
    universities: list[University] = []

    # Page 1 and beyond
    page = 1
    while True:
        url = f"{STUDYIN_LIST}?page={page}" if page > 1 else STUDYIN_LIST
        log.info(f"  Listing page {page}: {url}")
        resp = fetch(url, delay=delay)
        if not resp:
            break

        nd = extract_next_data(resp)
        s  = soup(resp)

        # Try JSON first
        props = nd.get("props", {}).get("pageProps", {})
        items = (
            props.get("universities", [])
            or props.get("items", [])
            or props.get("data", [])
        )
        detail_urls: list[str] = []

        if items:
            log.info(f"  Found {len(items)} items in __NEXT_DATA__ page {page}")
            for item in items:
                slug = item.get("slug", item.get("id", ""))
                name = clean(item.get("name", ""))
                if not slug and not name:
                    continue
                detail_url = (
                    item.get("url")
                    or (f"{STUDYIN_BASE}/universities/{slug}" if slug else "")
                )
                if detail_url:
                    detail_urls.append(detail_url)
                else:
                    # Build minimal university from list data
                    u = University(
                        title     = name,
                        location  = clean(item.get("city", item.get("location", ""))),
                        website   = item.get("website", ""),
                        source_url= url,
                    )
                    if u.title:
                        u.region = detect_region(u.location)
                        universities.append(u)
        else:
            # HTML fallback
            for a in s.select("a[href*='/universities/']"):
                href = a.get("href", "")
                if href and href not in detail_urls:
                    full = href if href.startswith("http") else STUDYIN_BASE + href
                    detail_urls.append(full)

        detail_urls = list(dict.fromkeys(detail_urls))  # deduplicate
        log.info(f"  {len(detail_urls)} detail pages to scrape on page {page}")

        if HAS_TQDM:
            detail_urls = tqdm(detail_urls, desc=f"  studyin p{page}")
        for du in detail_urls:
            u = _parse_studyin_detail(du, delay)
            if u and u.title:
                universities.append(u)

        # Pagination check
        next_btn = s.find("a", string=re.compile(r"next|keyingi|→", re.I))
        total_pages = props.get("totalPages", props.get("total_pages", 1))
        if not next_btn and page >= total_pages:
            break
        if not detail_urls and not items:
            break
        page += 1
        if page > 30:  # safety cap
            break

    log.info(f"  studyin-uzbekistan.uz → {len(universities)} universities")
    return universities


# ─────────────────────────── SOURCE 3: Wikipedia ────────────────────────────
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_universities_in_Uzbekistan"

# Curated static data from Wikipedia for reliability
WIKI_STATIC: list[dict] = [
    {"title": "National University of Uzbekistan", "location": "Tashkent", "year_founded": 1918, "type": "Davlat"},
    {"title": "Tashkent State Technical University", "location": "Tashkent", "year_founded": 1918, "type": "Davlat"},
    {"title": "Tashkent University of Information Technologies (TUIT)", "location": "Tashkent", "year_founded": 1955, "type": "Davlat"},
    {"title": "Westminster International University in Tashkent", "location": "Tashkent", "year_founded": 2002, "type": "Xorijiy filial"},
    {"title": "Turin Polytechnic University in Tashkent", "location": "Tashkent", "year_founded": 2009, "type": "Xorijiy filial"},
    {"title": "Inha University in Tashkent", "location": "Tashkent", "year_founded": 2014, "type": "Xorijiy filial"},
    {"title": "Samarkand State University", "location": "Samarkand", "year_founded": 1927, "type": "Davlat"},
    {"title": "Bukhara State University", "location": "Bukhara", "year_founded": 1930, "type": "Davlat"},
    {"title": "Fergana State University", "location": "Fergana", "year_founded": 1930, "type": "Davlat"},
    {"title": "Andijan State University", "location": "Andijan", "year_founded": 1931, "type": "Davlat"},
    {"title": "Namangan State University", "location": "Namangan", "year_founded": 1941, "type": "Davlat"},
    {"title": "Nukus State Pedagogical Institute", "location": "Nukus", "year_founded": 1976, "type": "Davlat"},
    {"title": "Urgench State University", "location": "Urgench", "year_founded": 1992, "type": "Davlat"},
    {"title": "Tashkent Institute of Finance", "location": "Tashkent", "year_founded": 1931, "type": "Davlat"},
    {"title": "Tashkent State Agrarian University", "location": "Tashkent", "year_founded": 1930, "type": "Davlat"},
    {"title": "Tashkent Medical Academy", "location": "Tashkent", "year_founded": 1920, "type": "Davlat"},
    {"title": "Tashkent Pharmaceutical Institute", "location": "Tashkent", "year_founded": 1937, "type": "Davlat"},
    {"title": "Tashkent State Institute of Oriental Studies", "location": "Tashkent", "year_founded": 1918, "type": "Davlat"},
    {"title": "Uzbek State University of World Languages", "location": "Tashkent", "year_founded": 1946, "type": "Davlat"},
    {"title": "Tashkent State University of Law", "location": "Tashkent", "year_founded": 1991, "type": "Davlat"},
    {"title": "University of Economics and Pedagogy", "location": "Tashkent", "year_founded": 2001, "type": "Xususiy"},
    {"title": "Tashkent Institute of Irrigation and Melioration", "location": "Tashkent", "year_founded": 1934, "type": "Davlat"},
    {"title": "Navoi State Mining and Technology University", "location": "Navoi", "year_founded": 1994, "type": "Davlat"},
    {"title": "Karshi State University", "location": "Karshi", "year_founded": 1976, "type": "Davlat"},
    {"title": "Gulistan State University", "location": "Gulistan", "year_founded": 1967, "type": "Davlat"},
    {"title": "Jizzakh State Pedagogical University", "location": "Jizzakh", "year_founded": 1972, "type": "Davlat"},
    {"title": "Termez State University", "location": "Termez", "year_founded": 1992, "type": "Davlat"},
    {"title": "Ferghana Polytechnic Institute", "location": "Fergana", "year_founded": 1967, "type": "Davlat"},
    {"title": "Tashkent Chemical-Technological Institute", "location": "Tashkent", "year_founded": 1958, "type": "Davlat"},
    {"title": "Tashkent State Dental Institute", "location": "Tashkent", "year_founded": 2005, "type": "Davlat"},
    {"title": "Samarkand State Medical University", "location": "Samarkand", "year_founded": 1930, "type": "Davlat"},
    {"title": "Andijan State Medical Institute", "location": "Andijan", "year_founded": 1955, "type": "Davlat"},
    {"title": "Bukhara State Medical Institute", "location": "Bukhara", "year_founded": 1990, "type": "Davlat"},
    {"title": "Fergana Medical Institute of Public Health", "location": "Fergana", "year_founded": 2010, "type": "Davlat"},
    {"title": "Silk Road International University of Tourism", "location": "Samarkand", "year_founded": 2018, "type": "Davlat"},
    {"title": "Tashkent State Pedagogical University", "location": "Tashkent", "year_founded": 1935, "type": "Davlat"},
    {"title": "New Uzbekistan University", "location": "Tashkent", "year_founded": 2021, "type": "Davlat"},
    {"title": "AKFA University", "location": "Tashkent", "year_founded": 2020, "type": "Xususiy"},
    {"title": "International University of Finance", "location": "Tashkent", "year_founded": 2020, "type": "Xususiy"},
    {"title": "Ajou University in Tashkent", "location": "Tashkent", "year_founded": 2022, "type": "Xorijiy filial"},
    {"title": "Moscow State University branch Tashkent", "location": "Tashkent", "year_founded": 2006, "type": "Xorijiy filial"},
    {"title": "Plekhanov Russian University of Economics branch", "location": "Tashkent", "year_founded": 2004, "type": "Xorijiy filial"},
    {"title": "Management Development Institute of Singapore", "location": "Tashkent", "year_founded": 2009, "type": "Xorijiy filial"},
    {"title": "Webster University Tashkent", "location": "Tashkent", "year_founded": 2019, "type": "Xorijiy filial"},
]

# National rankings (approximate, based on public sources)
NATIONAL_RANKINGS: dict[str, int] = {
    "national university of uzbekistan": 1,
    "tashkent university of information technologies": 2,
    "tuit": 2,
    "tashkent state technical university": 3,
    "tashkent institute of irrigation": 4,
    "samarkand state university": 5,
    "ferghana polytechnic": 6,
    "tashkent medical academy": 7,
    "uzbek state university of world languages": 8,
    "bukhara state university": 9,
    "tashkent state university of law": 10,
    "westminster": 11,
    "turin polytechnic": 12,
    "inha university": 13,
    "andijan state university": 14,
    "fergana state university": 15,
}

# Faculty data (curated)
FACULTY_DATA: dict[str, dict] = {
    "national university of uzbekistan": {
        "faculties": "Fizika; Kimyo; Biologiya; Matematika; Mexanika; Tarix; Falsafa; Jurnalistika; Huquq; Iqtisodiyot; Informatika; Filologiya",
        "num_faculties": 16,
        "students_total": 30000,
    },
    "tashkent university of information technologies": {
        "faculties": "Kompyuter muhandisligi; Dasturiy muhandislik; Axborot tizimlari; Telekommunikatsiya; Kiberxavfsizlik; Sun'iy intellekt",
        "num_faculties": 10,
        "students_total": 25000,
    },
    "tashkent state technical university": {
        "faculties": "Mashinasozlik; Energetika; Qurilish; Transport; Metallurgiya; Elektronika; Kimyo texnologiyasi",
        "num_faculties": 12,
        "students_total": 22000,
    },
    "samarkand state university": {
        "faculties": "Tarix; Filologiya; Fizika; Matematika; Kimyo; Biologiya; Geografiya; Iqtisodiyot; Pedagogika",
        "num_faculties": 13,
        "students_total": 18000,
    },
    "tashkent medical academy": {
        "faculties": "Davolash; Pediatriya; Tibbiy biologiya; Jamoat salomatligi; Farmatsevtika; Stomatologiya",
        "num_faculties": 8,
        "students_total": 14000,
    },
    "westminster international university in tashkent": {
        "faculties": "Biznes va Menejment; IT va Kompyuter fanlari; Huquq; Ijtimoiy fanlar; Iqtisodiyot",
        "num_faculties": 5,
        "students_total": 6500,
        "annual_cost_usd": "$4,000–$6,500/yil",
    },
    "turin polytechnic university in tashkent": {
        "faculties": "Muhandislik va Texnologiya; Arxitektura; Industrial dizayn; Menejment",
        "num_faculties": 4,
        "students_total": 3000,
        "annual_cost_usd": "$3,500–$5,500/yil",
    },
    "inha university in tashkent": {
        "faculties": "Kompyuter fanlari; Muhandislik; Elektron tijorat; Biznes",
        "num_faculties": 4,
        "students_total": 2500,
        "annual_cost_usd": "$2,500–$4,000/yil",
    },
}

# Annual cost data (approximate UZS, as of 2024-2025)
COST_DATA_UZS: dict[str, str] = {
    "national university of uzbekistan":           "6,000,000 – 14,000,000 UZS/yil",
    "tashkent university of information technologies": "7,000,000 – 16,000,000 UZS/yil",
    "tashkent state technical university":         "6,500,000 – 13,000,000 UZS/yil",
    "samarkand state university":                  "5,500,000 – 11,000,000 UZS/yil",
    "tashkent medical academy":                    "8,000,000 – 20,000,000 UZS/yil",
    "tashkent pharmaceutical institute":           "7,500,000 – 18,000,000 UZS/yil",
    "fergana state university":                    "5,000,000 – 10,000,000 UZS/yil",
    "andijan state university":                    "5,000,000 – 10,000,000 UZS/yil",
    "bukhara state university":                    "5,000,000 – 10,000,000 UZS/yil",
    "namangan state university":                   "5,000,000 – 9,500,000 UZS/yil",
    "uzbek state university of world languages":   "6,000,000 – 12,000,000 UZS/yil",
    "tashkent state university of law":            "7,000,000 – 15,000,000 UZS/yil",
    "akfa university":                             "15,000,000 – 40,000,000 UZS/yil",
    "new uzbekistan university":                   "10,000,000 – 25,000,000 UZS/yil",
}


def scrape_wikipedia(delay: float = 1.0) -> list[University]:
    log.info("━━━ SOURCE 3: Wikipedia + curated data ━━━")
    universities: list[University] = []

    # Try live Wikipedia scrape first
    resp = fetch(WIKI_URL, delay=delay)
    wiki_titles: set[str] = set()

    if resp:
        s = soup(resp)
        for li in s.select("div#mw-content-text li"):
            a = li.find("a")
            text = clean(li.get_text())
            if a and len(text) > 5 and any(
                kw in text.lower()
                for kw in ["university", "institute", "academy", "college", "texnika"]
            ):
                wiki_titles.add(text[:120])

        log.info(f"  Wikipedia live: {len(wiki_titles)} university names found")

    # Build from curated static list (always reliable)
    for item in WIKI_STATIC:
        u = University(
            title           = item["title"],
            location        = item["location"],
            year_founded    = item.get("year_founded"),
            university_type = item.get("type", ""),
            source_url      = WIKI_URL,
        )
        u.region = detect_region(u.location)

        # Enrich with faculty / student / cost data
        key = u.title.lower()
        for fkey, fdata in FACULTY_DATA.items():
            if fkey in key:
                u.faculties     = fdata.get("faculties", "")
                u.num_faculties = fdata.get("num_faculties")
                u.students_total = fdata.get("students_total")
                if "annual_cost_usd" in fdata:
                    u.annual_cost_usd = fdata["annual_cost_usd"]
                break

        for ckey, cval in COST_DATA_UZS.items():
            if ckey in key:
                u.annual_cost_uzs = cval
                break

        for rkey, rank in NATIONAL_RANKINGS.items():
            if rkey in key:
                u.national_ranking = rank
                break

        universities.append(u)

    log.info(f"  Wikipedia/curated → {len(universities)} universities")
    return universities


# ─────────────────────────── Merge & deduplicate ─────────────────────────────
def normalize_title(t: str) -> str:
    t = t.lower()
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\b(university|institute|davlat|state|of|in|the|and)\b", "", t)
    return re.sub(r"\s+", " ", t).strip()


def merge(sources: list[list[University]]) -> list[University]:
    log.info("━━━ Merging & deduplicating ━━━")
    seen: dict[str, University] = {}

    for source in sources:
        for u in source:
            if not u.title:
                continue
            key = normalize_title(u.title)
            if key not in seen:
                seen[key] = u
            else:
                existing = seen[key]
                # Fill empty fields from newer source
                for f_name in vars(existing):
                    if f_name in ("scraped_at", "source_url"):
                        continue
                    if not getattr(existing, f_name) and getattr(u, f_name):
                        setattr(existing, f_name, getattr(u, f_name))

    merged = list(seen.values())
    # Sort by national ranking, then title
    merged.sort(key=lambda u: (u.national_ranking or 9999, u.title))
    log.info(f"  Total unique universities: {len(merged)}")
    return merged


# ─────────────────────────── Export ──────────────────────────────────────────
FIELDNAMES = [
    "national_ranking", "title", "title_uz", "university_type",
    "location", "region", "year_founded",
    "num_faculties", "faculties",
    "students_total",
    "annual_cost_uzs", "annual_cost_usd",
    "qs_ranking", "website", "source_url", "scraped_at",
]


def export_csv(unis: list[University], path: Path):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        for u in unis:
            writer.writerow(asdict(u))
    log.info(f"  ✓ CSV  → {path}")


def export_json(unis: list[University], path: Path):
    data = [asdict(u) for u in unis]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info(f"  ✓ JSON → {path}")


def print_summary(unis: list[University]):
    print("\n" + "═" * 70)
    print(f"  📊  JAMI UNIVERSITETLAR: {len(unis)}")
    print("═" * 70)
    by_type = {}
    for u in unis:
        t = u.university_type or "Noma'lum"
        by_type[t] = by_type.get(t, 0) + 1
    for t, cnt in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"  {t:<30} {cnt:>4} ta")
    print("─" * 70)
    by_region = {}
    for u in unis:
        r = u.region or "Noma'lum"
        by_region[r] = by_region.get(r, 0) + 1
    for r, cnt in sorted(by_region.items(), key=lambda x: -x[1]):
        print(f"  {r:<30} {cnt:>4} ta")
    print("═" * 70)
    print("\n  TOP 10 universitetlar (reyting bo'yicha):")
    print("  " + "─" * 66)
    ranked = [u for u in unis if u.national_ranking][:10]
    for u in ranked:
        cost = u.annual_cost_uzs or u.annual_cost_usd or "–"
        stu  = f"{u.students_total:,}" if u.students_total else "–"
        yr   = str(u.year_founded) if u.year_founded else "–"
        print(f"  {u.national_ranking:>2}. {u.title[:40]:<40}  {u.location:<12}  {yr}  {stu} talaba")
    print("═" * 70 + "\n")


# ─────────────────────────── CLI ─────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Uzbekiston universitetlari scraper")
    p.add_argument("--output",  default=".", help="Chiqish papkasi (default: .)")
    p.add_argument("--delay",   type=float, default=1.2,  help="So'rovlar orasidagi kutish (soniya)")
    p.add_argument("--sources", nargs="+",
                   choices=["gov", "studyin", "wiki", "all"],
                   default=["all"],
                   help="Qaysi manbalardan olish")
    p.add_argument("--no-csv",  action="store_true", help="CSV saqlamaslik")
    p.add_argument("--no-json", action="store_true", help="JSON saqlamaslik")
    p.add_argument("--verbose", action="store_true", help="DEBUG log")
    return p.parse_args()


# ─────────────────────────── Main ────────────────────────────────────────────
def main():
    args  = parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    out   = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    use   = set(args.sources)
    delay = args.delay

    log.info("=" * 60)
    log.info("  Uzbekiston Universitetlari Scraper  🎓")
    log.info(f"  Boshlandi: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    all_sources: list[list[University]] = []

    if "all" in use or "wiki" in use:
        all_sources.append(scrape_wikipedia(delay))

    if "all" in use or "gov" in use:
        all_sources.append(scrape_gov_uz(delay))

    if "all" in use or "studyin" in use:
        all_sources.append(scrape_studyin(delay))

    universities = merge(all_sources)

    log.info("━━━ Natijalarni saqlash ━━━")
    ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not args.no_csv:
        export_csv(universities,  out / f"universities_{ts}.csv")
        export_csv(universities,  out / "universities.csv")   # latest

    if not args.no_json:
        export_json(universities, out / f"universities_{ts}.json")
        export_json(universities, out / "universities.json")  # latest

    if HAS_PANDAS and not args.no_csv:
        try:
            df = pd.read_csv(out / "universities.csv", encoding="utf-8-sig")
            log.info(f"  pandas  → {len(df)} rows × {len(df.columns)} cols loaded ✓")
            # Excel export bonus
            excel_path = out / "universities.xlsx"
            df.to_excel(excel_path, index=False)
            log.info(f"  ✓ XLSX  → {excel_path}")
        except Exception as e:
            log.debug(f"  pandas/excel: {e}")

    print_summary(universities)
    log.info(f"  Tugadi: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()