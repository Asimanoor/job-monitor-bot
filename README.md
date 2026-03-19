# 🎯 Job Monitor Bot

Automated job monitoring system for fresh CS graduates. It watches `links.txt` career pages, extracts job-only postings, filters by role (keyword + optional semantic), and logs all findings to **Google Sheets**. Additionally, the bot searches the internet for job openings from companies in `links.txt`.

> Built by Fawwaz as a portfolio project — FAST NUCES Lahore, BSCS

---

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────────────┐
│  links.txt   │     │   jobs.txt   │     │    filters.txt       │
│  (URLs)      │     │  (Titles)    │     │ (Include keywords)   │
└──────┬───────┘     └──────┬───────┘     └──────────┬───────────┘
       │                    │                        │
       ▼                    ▼                        ▼
┌────────────────────┐  ┌─────────────────────┐  ┌──────────────────────┐
│ URL Monitor        │  │ Job-only Extraction │  │ Role Filter           │
│ (content tracking) │  │ (cards + JSON-LD)   │  │ (keyword + optional  │
└────────┬───────────┘  └────────┬────────────┘  │ semantic similarity) │
         │                       │                └──────────┬───────────┘
         │                       ▼                           │
         │              ┌──────────────────────┐             │
         │              │ Job Details         │             │
         │              │ Enrichment          │             │
         │              └──────────┬───────────┘             │
         │                         │                        │
         └──────────────┬──────────┴────────────────────────┘
                        ▼
       ┌────────────────────────────────┐
       │ Google Sheets Logger           │
       │ (Audit + Openings Logs)        │
       │ • URL Changes Log              │
       │ • Career Openings Log          │
       │ • Search Activity Log          │
       └────────────────────────────────┘

┌──────────────────────────────────────────┐
│ Internet Job Searcher (New!)             │
│ • Search DuckDuckGo for company careers  │
│ • Extract job opening links              │
│ • Append to Google Sheets                │
└──────────────────────────────────────────┘
```

## Files

| File | Purpose |
|---|---|
| `monitor.py` | Main orchestrator (URL monitoring + job extraction + Google Sheets logging) |
| `internet_job_searcher.py` | Internet search for job openings from companies in links.txt |
| `job_extractor.py` | Job-only extraction + job-detail description enrichment |
| `google_sheets_client.py` | Google Sheets CRUD + career openings log |
| `email_notifier.py` | Gmail SMTP with rate limiting (optional fallback) |
| `email_templates.py` | Responsive HTML templates (FAST NUCES branding) |
| `notification_manager.py` | Channel management: Google Sheets → Email → JSON |
| `weekly_report.py` | Weekly summary: stats + auto-archive old jobs |
| `encode_credentials.py` | Utility: base64-encode Google service account key |
| `Code.gs` | Google Apps Script: custom menu for the Sheet |

## Quick Start

### 1. Clone & Install

```bash
git clone https://github.com/YOUR_USERNAME/Job-automate.git
cd Job-automate
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
```

Optional strategy packages included in `requirements.txt`:
- `langchain` + `langchain-community` for recursive crawl fallback
- `crewai` for post-processing/normalization fallback layer

### 2. Configure Secrets

Create a `.env` file (never commit this!):

```env
GH_PAT=ghp_your_token
GOOGLE_SHEET_ID=your_sheet_id
GOOGLE_SERVICE_ACCOUNT_EMAIL=your@service-account.iam.gserviceaccount.com
EMAIL_SENDER=your.email@gmail.com
EMAIL_APP_PASSWORD=your_16_char_app_password
EMAIL_RECIPIENT=your.email@gmail.com
GROQ_API_KEY=your_groq_api_key
ENABLE_SEMANTIC_FILTER=true
SEMANTIC_MODEL_NAME=sentence-transformers/all-MiniLM-L6-v2
ENABLE_PLAYWRIGHT_SCRAPER=true
ENABLE_LANGCHAIN_SCRAPER=true
ENABLE_CREWAI_SCRAPER=true
COMPANY_TARGETED_SEARCH_ENABLED=true
COMPANY_TARGETED_MAX_COMPANIES=90
COMPANY_TARGETED_MAX_QUERIES_PER_RUN=4
LINK_SCRAPER_MAX_PAGES=8
LINK_SCRAPER_MAX_OPENINGS_PER_SITE=300
PLAYWRIGHT_MAX_OPENINGS_PER_PAGE=120
ENABLE_INTERNET_COMPANY_SEARCH=true
INTERNET_SEARCH_MAX_COMPANIES=15
INTERNET_SEARCH_MAX_RESULTS_PER_COMPANY=3
RECORD_URL_CHANGES_TO_SHEETS=true
RECORD_SEARCH_ACTIVITY_TO_SHEETS=true
HF_TOKEN=optional_huggingface_token
```

### GROQ AI Setup

1. Create a free API key at [Groq Console](https://console.groq.com/keys)
2. Add the key as `GROQ_API_KEY` in local `.env` and GitHub Secrets
3. The bot uses GROQ only after keyword filters pass (cost-aware)
4. If GROQ is unavailable, the bot falls back to deterministic keyword heuristics (no crash)

### 3. Google Sheets Setup

1. Run `python encode_credentials.py` → copy base64 string
2. Add it as GitHub Secret: `GOOGLE_CREDENTIALS_JSON`
3. Follow [sheet_setup_guide.md](sheet_setup_guide.md) for formatting
4. Follow [EMAIL_SETUP.md](EMAIL_SETUP.md) for Gmail App Password
5. On first URL-change run, the bot auto-creates two extra worksheets:
       - `URL Changes Log` (every detected career-page change)
       - `Career Openings Log` (detected opening title + opening link)
       - `Search Activity Log` (per-URL scan outcome: searched/changed/ignored/error)

### 4. Run

```bash
# Normal run
python monitor.py

# Continuous local automation (every 6 hours)
python monitor.py --every-hours 6

# Automation smoke test: 2 quick cycles (for validation)
python monitor.py --dry-run --every-hours 0.001 --max-cycles 2

# Dry run (no notifications sent)
python monitor.py --dry-run

# Help
python monitor.py --help
```

### 5. Pause / Resume

Create `pause.txt` in the repo root to pause the bot. Delete it to resume.

### GitHub Actions

| Workflow | Schedule | Purpose |
|---|---|---|
| `job_monitor.yml` | Every 6 hours | URL change monitoring (links.txt) + internet job search + Sheets logging |
| `weekly_report.yml` | Sunday 9 AM PKT | Weekly summary + archive |

### Required Secrets

| Secret | Description |
|---|---|
| `GH_PAT` | GitHub PAT (repo scope only) |
| `GOOGLE_CREDENTIALS_JSON` | Base64-encoded service account JSON |
| `GOOGLE_SHEET_ID` | Spreadsheet ID from URL |
| `GOOGLE_SERVICE_ACCOUNT_EMAIL` | Service account email |
| `EMAIL_SENDER` | Gmail address (optional) |
| `EMAIL_APP_PASSWORD` | Gmail App Password (16-char, optional) |
| `EMAIL_RECIPIENT` | Alert recipient email (optional) |
| `GROQ_API_KEY` | GROQ API key for AI scoring/summaries |
| `HF_TOKEN` | Optional Hugging Face token for higher model-download rate limits |

## Notification Chain

```
1. Google Sheets (persistent record — primary)
2. Email (HTML + plain-text, if Sheets fails)
3. failed_alerts.json (last resort — nothing lost)
```

### Internet Job Search

The bot automatically searches the internet for job openings from companies in `links.txt`:

1. Extracts company names from career page URLs
2. Searches DuckDuckGo for "[Company] careers jobs" queries
3. Filters search results for likely job posting pages
4. Extracts job opening titles and links
5. Appends to Google Sheets `Career Openings Log` worksheet

Configuration:
- `ENABLE_INTERNET_COMPANY_SEARCH` (default: true)
- `INTERNET_SEARCH_MAX_COMPANIES` (default: 15)
- `INTERNET_SEARCH_MAX_RESULTS_PER_COMPANY` (default: 3)
- `RECORD_URL_CHANGES_TO_SHEETS` (default: true)
- `RECORD_SEARCH_ACTIVITY_TO_SHEETS` (default: true)

## Troubleshooting

| Problem | Fix |
|---|---|
| No jobs found | Check `jobs.txt` keywords, try broader terms |
| API 429 errors | Free tier: 500 calls/month. Reduce `SEARCH_LOCATIONS` |
| Sheets permission denied | Share sheet with service account email as Editor |
| Email auth failed | Enable 2FA → generate App Password (see EMAIL_SETUP.md) |
| Internet search not working | Check DuckDuckGo connectivity; verify `links.txt` has valid URLs |
| GROQ errors / invalid JSON | Verify `GROQ_API_KEY`; bot auto-falls back to keyword heuristics |
| AI confidence filtering seems strict | Lower `ai_confidence_threshold` in `config.json` (default 70) |

## Testing

Use these scripts before production deployment:

- `python seed_sheet_data.py` → seeds 12 dummy rows (safe: skips if sheet already has data)
- `python test_notifications.py` → validates Telegram, Email, and Google Sheets channels
- `python monitor.py --test-mode` → runs channel tests through the main orchestrator path
- `python monitor.py --dry-run --health-check-only` → checks channel health without searching jobs

For sheet formatting and required headers, see [`sheet_setup_guide.md`](sheet_setup_guide.md).

## License

MIT



