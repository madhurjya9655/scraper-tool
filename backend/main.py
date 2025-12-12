from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from pathlib import Path
import json
import redis
from rq import Queue
from rq.job import Job

from .settings import CORS_ALLOW_ORIGINS, REDIS_URL, RQ_QUEUE_NAME, EXPORT_BASE
from .tasks import run_scrape_job

app = FastAPI(title="B2B Lead Scraper API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in CORS_ALLOW_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

rconn = redis.from_url(REDIS_URL)
queue = Queue(RQ_QUEUE_NAME, connection=rconn)

class StartPayload(BaseModel):
    keywords: List[str] = Field(..., min_items=1)
    locations: List[str] = Field(..., min_items=1)
    industry: Optional[str] = ""
    company_type: Optional[str] = ""
    limit_per_combo: Optional[int] = Field(12, ge=1, le=50)
    max_runtime_min: Optional[int] = Field(15, ge=1, le=120)
    workers: Optional[int] = Field(24, ge=1, le=64)

@app.get("/", response_class=HTMLResponse)
def index():
    html = (Path(__file__).parent / "static" / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(content=html, status_code=200)

@app.post("/start-scrape")
def start_scrape(p: StartPayload):
    job = queue.enqueue(run_scrape_job, p.dict(), job_timeout=60*60*2)
    return {"task_id": job.get_id(), "status": "queued"}

@app.get("/scrape-status/{task_id}")
def scrape_status(task_id: str):
    try:
        job = Job.fetch(task_id, connection=rconn)
    except Exception:
        manifest = _read_manifest(task_id)
        if manifest:
            return {"task_id": task_id, "status": "completed", "result": manifest}
        raise HTTPException(status_code=404, detail="task_id not found")

    status = job.get_status()
    meta = job.meta or {}
    out = {
        "task_id": task_id,
        "status": status,
        "phase": meta.get("phase", ""),
        "progress": meta.get("progress", ""),
        "outputs": meta.get("outputs", {}),
    }
    if status == "finished":
        try:
            out["result"] = job.return_value()
        except Exception:
            pass
    return out

@app.get("/download/csv/{task_id}")
def download_csv(task_id: str):
    csv_path = EXPORT_BASE / task_id / "output.csv"
    if not csv_path.exists():
        raise HTTPException(status_code=404, detail="CSV not ready")
    return FileResponse(csv_path, filename="output.csv", media_type="text/csv")

@app.get("/download/excel/{task_id}")
def download_excel(task_id: str):
    xlsx_path = EXPORT_BASE / task_id / "output.xlsx"
    if not xlsx_path.exists():
        raise HTTPException(status_code=404, detail="Excel not ready")
    return FileResponse(xlsx_path, filename="output.xlsx",
                        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

def _read_manifest(task_id: str) -> Optional[Dict[str, Any]]:
    m = EXPORT_BASE / task_id / "manifest.json"
    if m.exists():
        try:
            return json.loads(m.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None
