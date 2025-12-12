from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from pathlib import Path
import uuid
from concurrent.futures import ThreadPoolExecutor

from .settings import CORS_ALLOW_ORIGINS, EXPORT_BASE
from .tasks_inproc import run_scrape_job_inproc

app = FastAPI(title="B2B Lead Scraper API (in-process)", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in CORS_ALLOW_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory task registry
TASKS: Dict[str, Dict[str, Any]] = {}
EXECUTOR = ThreadPoolExecutor(max_workers=2)

class StartPayload(BaseModel):
    keywords: List[str] = Field(..., min_items=1)
    locations: List[str] = Field(..., min_items=1)
    industry: Optional[str] = ""
    company_type: Optional[str] = ""
    limit_per_combo: Optional[int] = Field(12, ge=1, le=50)
    max_runtime_min: Optional[int] = Field(20, ge=1, le=120)
    workers: Optional[int] = Field(16, ge=1, le=64)

@app.get("/", response_class=HTMLResponse)
def index():
    html = (Path(__file__).parent / "static" / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(content=html, status_code=200)

@app.post("/start-scrape")
def start_scrape(p: StartPayload):
    task_id = uuid.uuid4().hex
    TASKS[task_id] = {"status":"queued", "phase":"queued", "progress":"", "outputs":{}}

    def progress_cb(phase: str, progress: str):
        t = TASKS.get(task_id)
        if t:
            t.update({"status": phase, "phase": phase, "progress": progress})

    def run():
        try:
            TASKS[task_id].update({"status":"running", "phase":"running"})
            result = run_scrape_job_inproc(task_id, p.dict(), progress_cb=progress_cb)
            TASKS[task_id].update({
                "status":"finished",
                "phase":"completed",
                "outputs": result.get("outputs", {}),
                "result": result
            })
        except Exception as e:
            TASKS[task_id].update({"status":"failed", "phase":"failed", "error": str(e)})

    EXECUTOR.submit(run)
    return {"task_id": task_id, "status": "queued"}

@app.get("/scrape-status/{task_id}")
def scrape_status(task_id: str):
    t = TASKS.get(task_id)
    if not t:
        m = _read_manifest(task_id)
        if m:
            return {"task_id": task_id, "status": "completed", "result": m, "outputs": m.get("outputs", {})}
        raise HTTPException(status_code=404, detail="task_id not found")
    return {"task_id": task_id, **t}

@app.get("/download/csv/{task_id}")
def download_csv(task_id: str):
    p = EXPORT_BASE / task_id / "output.csv"
    if not p.exists():
        raise HTTPException(status_code=404, detail="CSV not ready")
    return FileResponse(p, filename="output.csv", media_type="text/csv")

@app.get("/download/excel/{task_id}")
def download_excel(task_id: str):
    p = EXPORT_BASE / task_id / "output.xlsx"
    if not p.exists():
        raise HTTPException(status_code=404, detail="Excel not ready")
    return FileResponse(
        p,
        filename="output.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

def _read_manifest(task_id: str) -> Optional[Dict[str, Any]]:
    m = EXPORT_BASE / task_id / "manifest.json"
    if m.exists():
        import json
        try:
            return json.loads(m.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None
