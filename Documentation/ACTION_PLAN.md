
## `Documentation/ACTION_PLAN.md`
```markdown
# 7-DAY ACTION PLAN

**Day 1 – Setup & Smoke Test**
- Add keys to `.env`, run scraper once, confirm CSV.

**Day 2 – Tune Queries**
- Adjust `TARGET_COMPANY_TYPES` or add more locations.

**Day 3 – Enrichment**
- Run `contact_enricher.py`, validate emails/LinkedIn.

**Day 4 – CRM Integration**
- Import enriched CSV to your CRM (map fields 1:1).

**Day 5 – Automation**
- Add Task Scheduler jobs (daily scrape, weekly enrich).

**Day 6 – QA & Compliance**
- Review `Logs/lead_scraper.log`, confirm robots compliance.

**Day 7 – Scale**
- Raise per-combo cap, add keywords, consider proxies if needed.
