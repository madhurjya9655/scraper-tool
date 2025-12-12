# B2B Lead Generator (India – Industrial)

Collect and enrich B2B leads from IndiaMART, JustDial, and TradeIndia across 24 locations and 21 company types. Exports CSV + stores to SQLite.

## Structure
- `Scripts/b2b_lead_scraper.py`
- `Scripts/contact_enricher.py`
- `Documentation/`
- `Database/` (auto)
- `Exports/` (CSV)
- `Logs/` (log)
- `Config/requirements.txt`
- `.env` (API keys)

## Quick Start
```powershell
Set-Location "E:\CLIENT PROJECT\Bos Scrap"
. .\.venv\Scripts\Activate.ps1
python .\Scripts\b2b_lead_scraper.py
python .\Scripts\contact_enricher.py
