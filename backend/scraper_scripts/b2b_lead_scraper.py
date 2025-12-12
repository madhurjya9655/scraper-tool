#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
b2b_lead_scraper.py  (ENV-safe, Serper POST, per-run batch export, generic-ready)

Async, directory-safe B2B scraper for India (can be made generic with SCRAPER_STRICT=0).
- Free-tier SERP APIs (SerpAPI + Serper; optional)
- Dedupe (domain/name/email/phone/title/linkedin) within a run
- Shallow site crawl with strict filters (can relax by env)
- **Per-run batch_id** so exports contain only rows from the current run
- Output schema (13 columns):
  ID, Company Name, Contact Person, Email, Phone, Website, Industry,
  Location, Company Type, Source, Date, LinkedIn URL, Enriched Emails
"""

import os, re, csv, json, sys, time, math, logging, asyncio, urllib.parse, sqlite3, random, uuid
from datetime import datetime, timezone
from typing import Dict, Optional, List, Tuple, Any, Set
from functools import lru_cache
from difflib import SequenceMatcher
from html import unescape

try:
    import aiohttp
except ImportError:
    print("Please install aiohttp: pip install aiohttp", file=sys.stderr); sys.exit(1)

APP_NAME = "b2b_lead_scraper_async"

# ---------------------------- Env loader ----------------------------
def _load_env(path: str):
    if not os.path.isfile(path): return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s: continue
            k, v = s.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
# load backend/.env and project-root/.env if present
_load_env(os.path.join(BASE_DIR, ".env"))
_load_env(os.path.join(os.path.dirname(BASE_DIR), ".env"))

# ---------------------------- Config ----------------------------
DEFAULT_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "12"))
MIN_REQUEST_INTERVAL = float(os.environ.get("SCRAPER_RATE", "0.6"))
MAX_WORKERS = int(os.environ.get("SCRAPER_WORKERS", "24"))
CSV_MAX_ROWS = 100000

SERPAPI_KEY = os.environ.get("SERPAPI_KEY") or ""
SERPER_KEY  = os.environ.get("SERPER_API_KEY") or ""

UA = os.environ.get("SCRAPER_USER_AGENT", "Mozilla/5.0 (compatible; B2BLeadScraperAsync/1.1)")
ALANG = os.environ.get("SCRAPER_ACCEPT_LANGUAGE", "en-IN,en;q=0.9")

# NEW: strict mode switch (1 = industrial-biased filtering, 0 = generic)
STRICT_MODE = os.environ.get("SCRAPER_STRICT", "1") == "1"

TARGET_INDUSTRIES = [
    "Automotive","Automotive Components","Mechanical Engineering","Industrial Machinery",
    "Metals","Forging","Machine Manufacturing","Heavy Engineering","Construction","EPC",
    "Oil & Gas","Industrial Fabrication","Aerospace","Defence","Steel Procurement",
    "Alloy Steel Buyers","Steel Suppliers"
]

TARGET_COMPANY_TYPES = [
    "Forging Company","Closed Die Forging","Hot Forging Manufacturer","Auto Components Manufacturer",
    "Automotive Parts Supplier","Precision Machined Components","CNC Machining Company","Gear Manufacturer",
    "Transmission Parts Manufacturer","Crankshaft Manufacturer","Shafts Manufacturer","Flanges Manufacturer",
    "Hydraulic Cylinder Manufacturer","Pump & Valve Manufacturer","Industrial Machinery Parts",
    "Heavy Engineering Components","Alloy Steel Components","Round Bar Buyers","Steel Forging Supplier",
    "Tier 1 Auto Supplier","Industrial Gearbox Manufacturer"
]

DIRECTORY_DOMAINS: Set[str] = {
    "indiamart.com","tradeindia.com","justdial.com","tiimg.com","lens.indiamart.com",
    "3dcondl.com","exportersindia.com","yellowpages.com","yellowpages.in","99corporates.com",
    "dial4trade.com","aajjo.com","zoominfo.com","dnb.com","dunandbradstreet.com"
}
SERP_NEVER: Set[str] = DIRECTORY_DOMAINS.union({
    "facebook.com","youtube.com","pinterest.com","pinterest.co.in","crunchbase.com",
    "linkedin.com","zaubacorp.com","instagram.com","x.com","twitter.com",
    "bseindia.com","nseindia.com","gem.gov.in","gov.in","nic.in"
})

FREE_MAIL: Set[str] = {
    "gmail.com","yahoo.com","outlook.com","hotmail.com","rediffmail.com","live.com","icloud.com",
    "aol.com","proton.me","protonmail.com","yandex.com","zoho.com","gmx.com"
}

# Broadened candidate paths for generic sites, while keeping the industrial-friendly ones
CANDIDATE_PATHS = [
    "/","/contact","/contact-us","/about","/about-us","/team","/people","/reach-us",
    "/contactus","/contacts","/company","/aboutus","/who-we-are","/impressum"
]

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?\d{1,3}[-\s]?)?\d[\d\-\s]{8,}\d")  # keep broad; normalize later if needed
TITLE_SPLIT_RE = re.compile(r"[-|–|:|•|,]")

TITLE_BLACKLIST = {
    "home","jobs","account suspended","login","sign in","register","instagram","trader","catalog","marketplace"
}
PATH_BLACKLIST_SUBSTR = ("/login","/signin","/register","/account","/careers","/jobs","/blog/","/news","/events","/investor","/privacy","/terms")

# Industrial-biased keywords retained, but optional depending on STRICT_MODE
MUST_HAVE_KEYWORDS = ("manufactur","forg","machin","cnc","hydraul","gear","flange","crankshaft","valve","pump","auto component")

# ---------------------------- Utils ----------------------------
def iso_now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()

def safe_mkdir(p: str): os.makedirs(p, exist_ok=True)

def domain_of(url: str) -> Optional[str]:
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    except: return None

def is_valid_url(u: str) -> bool:
    try:
        p = urllib.parse.urlparse(u)
        return p.scheme in ("http","https") and "." in (p.netloc or "")
    except: return False

def is_dir_domain(host: Optional[str]) -> bool:
    if not host: return False
    h = host.lower()
    if any(h.endswith(d) for d in DIRECTORY_DOMAINS): return True
    if h.endswith((".gov.in",".nic.in",".ac.in",".edu",".edu.in")): return True
    return False

def normalize_phone(raw: str) -> Optional[str]:
    if not raw: return None
    digits = re.sub(r"\D","", raw)
    # Keep as generic as possible: accept 10–15 digits; prefer last 10 if >15
    if len(digits) > 15:
        digits = digits[-15:]
    return digits if 10 <= len(digits) <= 15 else None

def is_valid_email(e: str) -> bool:
    if not e: return False
    m = re.match(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$", e.strip())
    return bool(m)

def rank_email(email: str, site_dom: Optional[str]) -> int:
    if not is_valid_email(email): return 0
    dom = email.split("@")[-1].lower()
    if site_dom and dom == site_dom: return 3
    if dom in FREE_MAIL: return 1
    return 2

def fuzzy_sim(a: str, b: str) -> float:
    a = " ".join(sorted(a.lower().split()))
    b = " ".join(sorted(b.lower().split()))
    return SequenceMatcher(None, a, b).ratio()

def title_head(s: str) -> str:
    if not s: return ""
    t = TITLE_SPLIT_RE.split(s)[0].strip()
    t = re.sub(r"\b(Indiamart|IndiaMART|Justdial|TradeIndia)\b.*$", "", t, flags=re.I).strip()
    return t

def best_company_from_url_or_title(url: str, title: str) -> Optional[str]:
    t = title_head(title)
    if 2 <= len(t) <= 120: return t
    d = domain_of(url) or ""
    name = (d.split(".")[0] if d else "").replace("-", " ").strip()
    return name.title() if len(name) >= 3 else None

def match_company_type(text: str) -> Optional[str]:
    if not text: return None
    for kw in TARGET_COMPANY_TYPES:
        if re.search(re.escape(kw), text, re.I): return kw
    return None

def industry_for_kw(kw: str) -> str:
    for i in TARGET_INDUSTRIES:
        if i.split()[0].lower() in (kw or "").lower(): return i
    return "General"  # neutral default for generic scraping

def title_looks_generic(t: str) -> bool:
    t = (t or "").strip().lower()
    if any(t.startswith(p) for p in ("find ","buy ","best ","top ","price","prices")): return True
    words = re.findall(r"[a-zA-Z]+", t)
    if len(words) <= 1: return True
    generic_nouns = {"home","jobs","trader","products","services","contact","about","catalog","marketplace"}
    if len(words) <= 3 and all(w in generic_nouns for w in words): return True
    return False

def looks_like_company_site(url: str, title: str, page_snippet: str) -> bool:
    if not is_valid_url(url): return False
    host = domain_of(url) or ""
    if is_dir_domain(host): return False
    path = (urllib.parse.urlparse(url).path or "").lower()
    if any(s in path for s in PATH_BLACKLIST_SUBSTR): return False
    t = (title or "").strip().lower()
    if any(bad in t for bad in TITLE_BLACKLIST): return False
    if title_looks_generic(t): return False

    # If STRICT_MODE, require industrial-ish hints; otherwise allow any decent-looking company page
    if STRICT_MODE:
        body = (page_snippet or "").lower()
        hit = any(k in t for k in MUST_HAVE_KEYWORDS) or any(k in body for k in MUST_HAVE_KEYWORDS)
        return bool(hit)
    else:
        return True

# ---------------------------- Logging ----------------------------
def setup_logger(log_dir: str) -> logging.Logger:
    safe_mkdir(log_dir)
    logger = logging.getLogger(APP_NAME)
    logger.setLevel(logging.INFO)
    if logger.handlers: return logger
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler(os.path.join(log_dir, "lead_scraper_async.log"), encoding="utf-8")
    ch = logging.StreamHandler(sys.stdout)
    fh.setFormatter(fmt); ch.setFormatter(fmt)
    logger.addHandler(fh); logger.addHandler(ch)
    return logger

# ---------------------------- DB ----------------------------
class LeadsDB:
    def __init__(self, path: str, logger: logging.Logger):
        self.path = path; self.logger = logger
        safe_mkdir(os.path.dirname(path))
        self.conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self._schema()

    def _schema(self):
        c = self.conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name TEXT NOT NULL,
            contact_person TEXT,
            email TEXT,
            phone TEXT,
            website TEXT,
            industry TEXT,
            location TEXT,
            company_type TEXT,
            source TEXT,
            scraped_date TEXT,
            linkedin_url TEXT,
            enriched_emails TEXT,
            batch_id TEXT DEFAULT ''
        );""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_leads_domain ON leads(website);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_leads_name ON leads(company_name);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_leads_batch ON leads(batch_id);")
        # backfill column for pre-existing DBs
        try:
            c.execute("ALTER TABLE leads ADD COLUMN batch_id TEXT DEFAULT ''")
            self.conn.commit()
        except Exception:
            pass

    def upsert(self, row: Dict[str, Any], batch_id: str) -> Optional[int]:
        cols = ["company_name","contact_person","email","phone","website","industry","location",
                "company_type","source","scraped_date","linkedin_url","enriched_emails","batch_id"]
        vals = [row.get(k,"") for k in cols[:-1]] + [batch_id]
        try:
            cur = self.conn.cursor()
            cur.execute(f"""
                INSERT INTO leads ({",".join(cols)})
                VALUES ({",".join(["?"]*len(cols))});
            """, vals)
            self.conn.commit()
            return cur.lastrowid
        except Exception as e:
            self.logger.info(f"DB insert skipped/failed: {e}")
            return None

    def fetch_batch(self, batch_id: str) -> List[Tuple]:
        cur = self.conn.cursor()
        cur.execute("""SELECT id, company_name, contact_person, email, phone, website,
                       industry, location, company_type, source, scraped_date, linkedin_url, enriched_emails
                       FROM leads WHERE batch_id = ?""", (batch_id,))
        return cur.fetchall()

def export_csv_xlsx(rows: List[Tuple], out_dir: str) -> Tuple[str,str]:
    safe_mkdir(out_dir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(out_dir, f"b2b_leads_{ts}.csv")
    xlsx_path = os.path.join(out_dir, f"b2b_leads_{ts}.xlsx")
    headers = ["ID","Company Name","Contact Person","Email","Phone","Website","Industry",
               "Location","Company Type","Source","Date","LinkedIn URL","Enriched Emails"]
    with open(csv_path,"w",newline="",encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(headers)
        n=0
        for r in rows:
            (cid,cname,cp,em,ph,web,ind,loc,ctype,src,dt,li,ee) = r
            w.writerow([cid, cname or "", cp or "", em or "", ph or "", web or "", ind or "", loc or "",
                        ctype or "", src or "", dt or "", li or "", ee or ""])
            n+=1
            if n>=CSV_MAX_ROWS: break
    try:
        from openpyxl import Workbook
        wb=Workbook(); ws=wb.active; ws.title="Leads"
        ws.append(headers)
        with open(csv_path,"r",encoding="utf-8") as f:
            next(f)
            for row in csv.reader(f): ws.append(row)
        wb.save(xlsx_path)
    except Exception:
        xlsx_path=""
    return csv_path, xlsx_path

# ---------------------------- Async HTTP ----------------------------
class Http:
    def __init__(self, logger: logging.Logger, timeout: int = DEFAULT_TIMEOUT):
        self.logger=logger
        self.timeout=timeout
        self._session: Optional[aiohttp.ClientSession] = None
        self._host_last: Dict[str, float] = {}
    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout),
            headers={"User-Agent": UA, "Accept-Language": ALANG, "Connection":"close"}
        )
        return self
    async def __aexit__(self, *exc):
        if self._session: await self._session.close()
    async def _pace(self, host: str):
        last = self._host_last.get(host, 0.0)
        wait = max(0.0, MIN_REQUEST_INTERVAL - (time.time()-last))
        if wait>0: await asyncio.sleep(wait)
        self._host_last[host] = time.time()
    async def get(self, url: str, headers: Optional[Dict[str,str]]=None, retries: int=3) -> Optional[bytes]:
        if not self._session: raise RuntimeError("session not started")
        host = urllib.parse.urlparse(url).netloc.lower()
        for attempt in range(1, retries+1):
            try:
                await self._pace(host)
                async with self._session.get(url, headers=headers) as resp:
                    if resp.status in (429, 500, 502, 503, 504) and attempt<retries:
                        await asyncio.sleep(1.5*attempt + random.uniform(0,1)); continue
                    if resp.status>=400:
                        if resp.status not in (403,404):
                            self.logger.warning(f"HTTP {resp.status} {url}")
                        return None
                    return await resp.read()
            except Exception as e:
                if attempt<retries:
                    await asyncio.sleep(1.0 + random.uniform(0,1)); continue
                self.logger.info(f"GET fail {url}: {e}")
                return None
    async def post_json(self, url: str, payload: Dict[str,Any], headers: Optional[Dict[str,str]]=None, retries: int=3) -> Optional[bytes]:
        if not self._session: raise RuntimeError("session not started")
        host = urllib.parse.urlparse(url).netloc.lower()
        for attempt in range(1, retries+1):
            try:
                await self._pace(host)
                async with self._session.post(url, json=payload, headers=headers) as resp:
                    if resp.status in (429, 500, 502, 503, 504) and attempt<retries:
                        await asyncio.sleep(1.5*attempt + random.uniform(0,1)); continue
                    if resp.status>=400:
                        if resp.status not in (403,404):
                            self.logger.warning(f"HTTP {resp.status} {url}")
                        return None
                    return await resp.read()
            except Exception as e:
                if attempt<retries:
                    await asyncio.sleep(1.0 + random.uniform(0,1)); continue
                self.logger.info(f"POST fail {url}: {e}")
                return None

# ---------------------------- SERP providers ----------------------------
class SerpAPI:
    @staticmethod
    async def search(http: Http, q: str, location: str, num: int=10) -> List[str]:
        if not SERPAPI_KEY: return []
        params = {
            "engine":"google","q":q,"location":f"{location}, India","num":str(num),
            "hl":"en","gl":"in","api_key":SERPAPI_KEY
        }
        url = "https://serpapi.com/search.json?" + urllib.parse.urlencode(params)
        data = await http.get(url, headers={"Accept":"application/json"})
        if not data: return []
        try:
            js = json.loads(data.decode("utf-8","ignore"))
            out=[]
            for it in js.get("organic_results",[]) or []:
                link = it.get("link")
                if link and is_valid_url(link):
                    h = domain_of(link) or ""
                    if any(h.endswith(b) for b in SERP_NEVER): continue
                    out.append(link)
            return out
        except: return []

class Serper:
    @staticmethod
    async def search(http: Http, q: str, location: str, num: int=10) -> List[str]:
        if not SERPER_KEY: return []
        url = "https://google.serper.dev/search"
        payload = {"q": q, "gl":"in", "hl":"en", "num": num, "location": f"{location}, India"}
        data = await http.post_json(url, payload, headers={
            "X-API-KEY": SERPER_KEY, "Accept":"application/json", "Content-Type":"application/json"
        })
        if not data: return []
        try:
            js = json.loads(data.decode("utf-8","ignore"))
            out=[]
            for it in js.get("organic",[]) or []:
                link = it.get("link")
                if link and is_valid_url(link):
                    h = domain_of(link) or ""
                    if any(h.endswith(b) for b in SERP_NEVER): continue
                    out.append(link)
            return out
        except: return []

QUERY_TEMPLATES = [
    '{kw} manufacturers in {city} India -indiamart -tradeindia -justdial -yellowpages -exportersindia',
    'site:.in {kw} {city} supplier -indiamart -tradeindia -justdial -facebook -youtube -pinterest',
    '{kw} {city} factory address -indiamart -tradeindia -justdial -exportersindia',
    '{kw} near {city} company website -indiamart -tradeindia -justdial',
    '{kw} {city} oem tier 1 -indiamart -tradeindia -justdial',
    'inurl:about OR inurl:contact "{kw}" "{city}" -indiamart -tradeindia -justdial -exportersindia',
    'site:.in "{kw}" "{city}" -indiamart -tradeindia -justdial'
]

def build_queries(kw: str, city: str) -> List[str]:
    return [t.format(kw=kw, city=city) for t in QUERY_TEMPLATES]

# ---------------------------- Site crawl & parse ----------------------------
def extract_emails(text: str) -> List[str]:
    return list(dict.fromkeys(m.group(0).strip() for m in EMAIL_RE.finditer(text or "")))

def extract_phones(text: str) -> List[str]:
    out=[]
    for m in PHONE_RE.findall(text or ""):
        p = normalize_phone(m)
        if p: out.append(p)
    return list(dict.fromkeys(out))

def extract_linkedin(text: str) -> Optional[str]:
    m = re.search(r"https?://(?:in\.)?linkedin\.com/(company|in)/[A-Za-z0-9\-_/%]+", text or "", re.I)
    return m.group(0) if m else None

class SiteScanner:
    def __init__(self, http: Http, logger: logging.Logger):
        self.http=http; self.logger=logger
        self.page_cache: Dict[str, bytes] = {}
    async def fetch(self, url: str) -> Optional[str]:
        if url in self.page_cache:
            try: return self.page_cache[url].decode("utf-8","ignore")
            except: return None
        data = await self.http.get(url)
        if not data: return None
        self.page_cache[url] = data
        try: return data.decode("utf-8","ignore")
        except: return None
    async def crawl(self, site_url: str) -> Tuple[List[str], List[str], Optional[str], str, str]:
        if not is_valid_url(site_url): return ([],[],None,"","")
        base = urllib.parse.urljoin(site_url, "/")
        dom = domain_of(base)
        if is_dir_domain(dom): return ([],[],None,"","")
        found_e, found_p = set(), set()
        li_url, title, last_text = None, "", ""
        for p in CANDIDATE_PATHS:
            url = urllib.parse.urljoin(base, p)
            text = await self.fetch(url)
            if not text: continue
            last_text = text[:2000]
            if not title:
                m = re.search(r"<title[^>]*>(.*?)</title>", text, re.I|re.S)
                if m:
                    raw = re.sub(r"\s+"," ", m.group(1)).strip()
                    title = unescape(raw[:180])
            for e in extract_emails(text):
                if is_valid_email(e) and not any(e.lower().endswith("@"+d) for d in DIRECTORY_DOMAINS):
                    found_e.add(e)
            for ph in extract_phones(text): found_p.add(ph)
            if not li_url:
                li = extract_linkedin(text)
                if li: li_url = li
            # early stop if we already have good signals
            if any(rank_email(e, dom)==3 for e in found_e) and found_p and li_url:
                break
        emails = sorted(found_e, key=lambda e: rank_email(e, dom), reverse=True)
        phones = sorted(found_p)
        return (emails, phones, li_url, title, last_text)

# ---------------------------- Dedupe Index ----------------------------
class DedupeIndex:
    def __init__(self):
        self.by_domain: Set[str] = set()
        self.by_phone: Set[str] = set()
        self.by_email: Set[str] = set()
        self.by_li_slug: Set[str] = set()
        self.names: List[str] = []
        self.titles: List[str] = []
    @staticmethod
    def _li_slug(url: Optional[str]) -> Optional[str]:
        if not url: return None
        try:
            p = urllib.parse.urlparse(url)
            return p.path.strip("/").lower()
        except: return None
    def seen_domain(self, url: str) -> bool:
        d = domain_of(url) or ""
        return d in self.by_domain
    def add(self, company: str, website: str, phone: Optional[str], email: Optional[str], li: Optional[str], title: Optional[str]):
        d = domain_of(website) or ""
        if d: self.by_domain.add(d)
        if phone: self.by_phone.add(phone)
        if email: self.by_email.add(email.lower())
        slug = self._li_slug(li)
        if slug: self.by_li_slug.add(slug)
        if company: self.names.append(company)
        if title: self.titles.append(title_head(title))
    def is_duplicate(self, company: str, website: str, phone: Optional[str], email: Optional[str], li: Optional[str], title: Optional[str]) -> bool:
        d = domain_of(website) or ""
        if d and d in self.by_domain: return True
        if phone and phone in self.by_phone: return True
        if email and email.lower() in self.by_email: return True
        slug = self._li_slug(li)
        if slug and slug in self.by_li_slug: return True
        th = title_head(title or "")
        for n in self.names:
            if fuzzy_sim(n, company) >= 0.85: return True
        for t in self.titles:
            if fuzzy_sim(t, th) >= 0.90: return True
        return False

# ---------------------------- Orchestrator ----------------------------
class Scraper:
    def __init__(self, base_dir: str, args: Dict[str, Any]):
        self.base_dir = base_dir
        self.log_dir = os.path.join(base_dir, "Logs")
        self.export_dir = os.path.join(base_dir, "Exports")
        self.db_path = os.path.join(base_dir, "Database", "leads_async.db")
        safe_mkdir(self.export_dir); safe_mkdir(os.path.dirname(self.db_path)); safe_mkdir(self.log_dir)
        self.logger = setup_logger(self.log_dir)
        self.db = LeadsDB(self.db_path, self.logger)
        self.limit_per_combo = int(args.get("limit_per_combo", 12))
        self.max_runtime_min = int(args.get("max_runtime_min", 15))
        self.start_ts = time.time()
        self.max_workers = int(args.get("workers", MAX_WORKERS))
        self.sources = args.get("sources", ["serpapi"])
        self.dedupe = DedupeIndex()
        self.batch_id = uuid.uuid4().hex  # this run's batch id

    def time_up(self) -> bool:
        return (time.time() - self.start_ts) > (self.max_runtime_min*60)

    async def serp_seed(self, http: Http, kw: str, city: str) -> List[str]:
        urls: List[str] = []
        queries = build_queries(kw, city)
        rnd = list(range(len(queries))); random.shuffle(rnd)
        for qi in rnd:
            q = queries[qi]
            batch: List[str] = []
            if "serpapi" in self.sources:
                batch.extend(await SerpAPI.search(http, q, city, num=self.limit_per_combo*2))
            if "serper" in self.sources:
                batch.extend(await Serper.search(http, q, city, num=self.limit_per_combo*2))
            uniq: Dict[str,str] = {}
            for u in batch:
                if not is_valid_url(u): continue
                h = domain_of(u) or ""
                if any(h.endswith(b) for b in SERP_NEVER): continue
                uniq.setdefault(h, u)
            new_domains = [h for h in uniq.keys() if not self.dedupe.seen_domain(uniq[h])]
            urls.extend(list(uniq.values()))
            if len(new_domains) <= max(2, math.ceil(0.3*len(uniq))): break
            if len(urls) >= self.limit_per_combo*3 or self.time_up(): break
        outs=[]
        for u in urls:
            p = urllib.parse.urlparse(u)
            home = f"{p.scheme}://{p.netloc}/"
            if home not in outs: outs.append(home)
        return outs[: self.limit_per_combo]

    async def process_site(self, scanner: SiteScanner, site_url: str, kw: str, city: str) -> Optional[Dict[str, Any]]:
        if self.time_up(): return None
        dom = domain_of(site_url)
        if not dom or is_dir_domain(dom): return None
        emails, phones, li, title, snippet = await scanner.crawl(site_url)
        title = unescape(title or "")
        if not looks_like_company_site(site_url, title, snippet): return None

        email = ""
        if emails:
            e_sorted = sorted(emails, key=lambda e: rank_email(e, dom), reverse=True)
            for e in e_sorted:
                if not any(e.lower().endswith("@"+d) for d in DIRECTORY_DOMAINS):
                    email = e; break
        phone = phones[0] if phones else ""
        company = best_company_from_url_or_title(site_url, title) or (dom.split(".")[0].title())
        if not company or len(company.strip()) < 3: return None
        if self.dedupe.is_duplicate(company, site_url, phone or None, email or None, li, title): return None

        row = {
            "company_name": company.strip(),
            "contact_person": "",
            "email": email or "",
            "phone": phone or "",
            "website": site_url,
            "industry": industry_for_kw(kw),
            "location": city,
            "company_type": match_company_type(kw) or kw,
            "source": site_url,
            "scraped_date": iso_now(),
            "linkedin_url": li or "",
            "enriched_emails": ""
        }

        if not (row["email"] or row["phone"]): return None
        if is_dir_domain(domain_of(row["website"])): return None
        if row["email"] and any(row["email"].lower().endswith("@"+d) for d in DIRECTORY_DOMAINS): return None
        return row

    async def run_combo(self, http: Http, kw: str, city: str, scanner: SiteScanner):
        seeds = await self.serp_seed(http, kw, city)
        if not seeds: return
        sem = asyncio.Semaphore(self.max_workers)
        async def task(site):
            async with sem:
                try:
                    return await self.process_site(scanner, site, kw, city)
                except Exception as e:
                    self.logger.info(f"process_site error {site}: {e}")
                    return None
        tasks = [asyncio.create_task(task(s)) for s in seeds]
        for coro in asyncio.as_completed(tasks):
            if self.time_up(): break
            row = await coro
            if not row: continue
            if self.dedupe.is_duplicate(row["company_name"], row["website"], row["phone"] or None, row["email"] or None, row["linkedin_url"] or None, ""):
                continue
            rid = self.db.upsert(row, self.batch_id)
            if rid:
                self.dedupe.add(row["company_name"], row["website"], row["phone"], row["email"], row["linkedin_url"], "")
                self.logger.info(f"Inserted: {row['company_name']} ({rid})")
            else:
                self.logger.info(f"Skipped (duplicate/failed): {row['company_name']}")

    async def run(self, locations: List[str], keywords: List[str]):
        combos = [(kw, loc) for loc in locations for kw in keywords]
        self.logger.info(f"Starting (combos={len(combos)}, limit_per_combo={self.limit_per_combo}, sources={self.sources})")
        async with Http(self.logger, DEFAULT_TIMEOUT) as http:
            scanner = SiteScanner(http, self.logger)
            for (kw, city) in combos:
                if self.time_up(): break
                await self.run_combo(http, kw, city, scanner)

        rows = self.db.fetch_batch(self.batch_id)     # only this run
        csv_path, xlsx_path = export_csv_xlsx(rows, os.path.join(self.base_dir, "Exports"))
        self.logger.info(f"Exported CSV: {csv_path}")
        self.logger.info(f"Exported Excel: {xlsx_path or '(skipped)'}")

# ---------------------------- CLI ----------------------------
def parse_cli():
    import argparse
    p = argparse.ArgumentParser(description="Async B2B lead scraper (directory-safe)")
    p.add_argument("--locations", type=str, default=os.environ.get("SCRAPER_LOCATIONS",""))
    p.add_argument("--keywords", type=str, default=os.environ.get("SCRAPER_KEYWORDS",""))
    p.add_argument("--sources", type=str, default=os.environ.get("SCRAPER_SOURCES","serpapi,serper"))
    p.add_argument("--limit-per-combo", type=int, default=int(os.environ.get("SCRAPER_LIMIT_PER_COMBO","12")))
    p.add_argument("--max-runtime-min", type=int, default=int(os.environ.get("SCRAPER_MAX_RUNTIME_MIN","15")))
    p.add_argument("--workers", type=int, default=int(os.environ.get("SCRAPER_WORKERS", str(MAX_WORKERS))))
    args = p.parse_args()
    locs = [s.strip() for s in (args.locations or "").split(",") if s.strip()] or ["Pune","Vadodara","Vapi"]
    kws = [s.strip() for s in (args.keywords or "").split(",") if s.strip()] or [
        "Forging Company","Steel Forging Supplier","Flanges Manufacturer",
        "Hydraulic Cylinder Manufacturer","Industrial Gearbox Manufacturer","Precision Machined Components"
    ]
    sources = [s.strip().lower() for s in (args.sources or "serpapi,serper").split(",") if s.strip()]
    return {
        "locations": locs,
        "keywords": kws,
        "sources": sources,
        "limit_per_combo": args.limit_per_combo,
        "max_runtime_min": args.max_runtime_min,
        "workers": args.workers
    }

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(script_dir)
    safe_mkdir(os.path.join(base_dir, "Database"))
    safe_mkdir(os.path.join(base_dir, "Exports"))
    safe_mkdir(os.path.join(base_dir, "Logs"))
    args = parse_cli()
    scraper = Scraper(base_dir, args)
    asyncio.run(scraper.run(args["locations"], args["keywords"]))

if __name__ == "__main__":
    main()
