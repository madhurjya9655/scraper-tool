import os, sys, asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable

from .settings import EXPORT_BASE, DOTENV_PATH
from .utils import ensure_task_dir, latest_by_glob, copy_if_exists, make_task_manifest, now_iso

THIS_DIR = Path(__file__).resolve().parent
SCRAPER_DIR = THIS_DIR / "scraper_scripts"
sys.path.insert(0, str(SCRAPER_DIR))

# Use the in-project async scraper and safe enricher
from b2b_lead_scraper import Scraper as AsyncScraper  # type: ignore
from contact_enricher_asyncsafe import run as run_enricher  # type: ignore


def _load_dotenv():
    if DOTENV_PATH.exists():
        for line in DOTENV_PATH.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def run_scrape_job_inproc(
    task_id: str,
    payload: Dict[str, Any],
    progress_cb: Optional[Callable[[str, str], None]] = None
) -> Dict[str, Any]:
    """Run scraper + enrichment fully in-process (no Redis)."""

    def set_status(phase: str, progress: str = ""):
        if progress_cb:
            try:
                progress_cb(phase, progress)
            except Exception:
                pass

    set_status("starting", "init")
    _load_dotenv()

    base_dir = SCRAPER_DIR.parent
    exports_dir = base_dir / "Exports"
    for d in (exports_dir, base_dir / "Database", base_dir / "Logs"):
        d.mkdir(parents=True, exist_ok=True)

    kws: List[str] = payload.get("keywords", [])
    locs: List[str] = payload.get("locations", [])
    industry: str = payload.get("industry", "").strip()
    company_type: str = payload.get("company_type", "").strip()

    # Force SerpAPI only (prevents serper 400s and stalls)
    args = {
        "limit_per_combo": int(payload.get("limit_per_combo", 12)),
        "max_runtime_min": int(payload.get("max_runtime_min", 20)),
        "workers": int(payload.get("workers", 16)),
        "sources": ["serpapi"],  # <- ONLY serpapi
    }

    set_status("scraping", "SERP & site crawl")
    scraper = AsyncScraper(str(base_dir), args)
    # Run and let the scraper write CSV/XLSX for THIS batch only
    asyncio.run(scraper.run(locs, kws))

    # Grab the most recent files (created by the run above)
    latest_csv = latest_by_glob(exports_dir, "b2b_leads_*.csv")
    latest_xlsx = latest_by_glob(exports_dir, "b2b_leads_*.xlsx")

    set_status("enriching", "Hunter/Clearbit")
    run_enricher()  # writes enriched CSV using the latest CSV

    latest_enriched_csv = latest_by_glob(exports_dir, "b2b_leads_enriched_*.csv")

    task_dir = ensure_task_dir(EXPORT_BASE, task_id)
    csv_dst = task_dir / "output.csv"
    xlsx_dst = task_dir / "output.xlsx"

    final_csv_src = latest_enriched_csv or latest_csv
    copy_if_exists(final_csv_src, csv_dst)
    copy_if_exists(latest_xlsx, xlsx_dst)

    manifest = {
        "task_id": task_id,
        "inputs": {"keywords": kws, "locations": locs, "industry": industry, "company_type": company_type},
        "outputs": {
            "csv": str(csv_dst if csv_dst.exists() else ""),
            "xlsx": str(xlsx_dst if xlsx_dst.exists() else "")
        },
        "finished_at": now_iso(),
        "phase": "completed"
    }
    make_task_manifest(task_dir, manifest)
    set_status("completed", "done")
    return manifest
