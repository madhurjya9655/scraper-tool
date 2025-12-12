import shutil
from pathlib import Path
from typing import Optional, Dict, Any

def latest_by_glob(dirpath: Path, pattern: str) -> Optional[Path]:
    files = sorted(dirpath.glob(pattern))
    return files[-1] if files else None

def ensure_task_dir(base: Path, task_id: str) -> Path:
    p = base / task_id
    p.mkdir(parents=True, exist_ok=True)
    return p

def copy_if_exists(src: Optional[Path], dst: Path) -> Optional[Path]:
    if src and src.exists():
        shutil.copy2(src, dst)
        return dst
    return None

def make_task_manifest(task_dir: Path, meta: Dict[str, Any]) -> None:
    (task_dir / "manifest.json").write_text(
        __import__("json").dumps(meta, indent=2), encoding="utf-8"
    )

def now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).astimezone().isoformat()
