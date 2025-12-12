# TECHNICAL REFERENCE

## DB Schema (SQLite: `Database/leads_database.db`)
Table `leads`:
- id INTEGER PK AUTOINCREMENT
- company_name TEXT UNIQUE NOT NULL
- contact_person TEXT
- email TEXT
- phone TEXT
- website TEXT
- industry TEXT
- location TEXT
- company_type TEXT
- source TEXT
- scraped_date TEXT (ISO 8601)
- verified INTEGER (0/1)

Indexes:
- idx_leads_location, idx_leads_industry, idx_leads_source, idx_leads_date

## HTTP Controls
- Rate limit: 1 request/sec
- Timeout: 12s (configurable)
- Retries: 3 w/ 5s backoff
- Robots.txt respected for IndiaMART, JustDial, TradeIndia

## Google Dorks (examples)
- `site:indiamart.com "forging" Pune`
- `site:justdial.com "gear manufacturer" Coimbatore`
- `site:tradeindia.com "hydraulic cylinder" Chennai`

## Data Quality
- Phones normalized to 10 digits (last 10)
- Email/URL regex validation
- Location matched to 24 targets
- Company type matched from 21 keywords

## Compliance
- Public data only; robots-aware.
- DPDP aligned; retention default 1 year.
- Support GDPR expansion by honoring deletion requests.
