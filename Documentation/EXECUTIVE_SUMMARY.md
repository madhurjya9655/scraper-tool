# EXECUTIVE SUMMARY

**Objective:** Generate industrial B2B leads across 24 Indian hubs and enrich to 90%+ email coverage.

## KPIs
- 300–800 leads per run
- 60–70% emails raw → 90%+ after enrichment
- 1,500–4,500 net-new per month (automation)
- <1s DB queries; <30s export for 10k rows

## Success Factors
- Robots-aware scraping at 1 req/sec
- Resilient HTTP (timeouts, retries)
- Hunter.io + optional Clearbit enrichment
- Clean logging & 1-year retention policy

## Risks & Guards
- Blocking (403s): mitigated with SerpAPI seeds + rate limit
- Site structure changes: generic parsers, frequent updates
- Data drift: periodic keyword tuning
