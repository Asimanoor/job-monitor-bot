# 🎯 Job Monitor Bot

Automated job monitoring system for fresh CS graduates. Searches JSearch API, filters entry-level roles, and notifies via **Telegram**, **Google Sheets**, and **Email**.

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
┌──────────────┐     ┌──────────────┐     ┌──────────────────────┐
│ URL Monitor  │     │ JSearch API  │     │   Filter Engine      │
│ (SHA-256)    │     │ (RapidAPI)   │     │ (Fuzzy + Exclusion)  │
└──────┬───────┘     └──────┬───────┘     └──────────┬───────────┘
       │                    │                        │
       └────────────┬───────┘────────────────────────┘
                    ▼
          ┌─────────────────┐
          │ NotificationMgr │
          │ (Telegram first │
          │  → Sheets → Email│
          │  → failed.json) │
          └────────┬────────┘
                   ▼
          ┌─────────────────┐
          │   state.json    │
          │ (dedup + commit)│
          └─────────────────┘
```

## Files

| File | Purpose |
|---|---|
| `monitor.py` | Main orchestrator (URL monitor + JSearch + notifications) |
| `google_sheets_client.py` | Google Sheets CRUD + archive + weekly stats |
| `email_notifier.py` | Gmail SMTP with rate limiting |
| `email_templates.py` | Responsive HTML templates (FAST NUCES branding) |
| `notification_manager.py` | Channel fallback: Telegram → Sheets → Email → JSON |
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
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
GH_PAT=ghp_your_token
JSEARCH_API_KEY=your_rapidapi_key
GOOGLE_SHEET_ID=your_sheet_id
GOOGLE_SERVICE_ACCOUNT_EMAIL=your@service-account.iam.gserviceaccount.com
EMAIL_SENDER=your.email@gmail.com
EMAIL_APP_PASSWORD=your_16_char_app_password
EMAIL_RECIPIENT=your.email@gmail.com
GROQ_API_KEY=your_groq_api_key
ENABLE_PLAYWRIGHT_SCRAPER=true
ENABLE_LANGCHAIN_SCRAPER=true
ENABLE_CREWAI_SCRAPER=true
COMPANY_TARGETED_SEARCH_ENABLED=true
COMPANY_TARGETED_MAX_COMPANIES=90
COMPANY_TARGETED_MAX_QUERIES_PER_RUN=4
LINK_SCRAPER_MAX_PAGES=8
LINK_SCRAPER_MAX_OPENINGS_PER_SITE=300
PLAYWRIGHT_MAX_OPENINGS_PER_PAGE=120
```

Company targeting behavior:
- `links.txt` domains are parsed to infer company hints (e.g., Lever/Workable/Ashby/Breezy patterns).
- JSearch runs company-targeted discovery first (`JSEARCH_COMPANY_TARGETED`) and then generic role/location searches.
- Openings found while targeting a company but belonging to other employers are still kept (`JSEARCH_OTHER_COMPANY_DISCOVERY`).

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

### 4. Run

```bash
# Normal run
python monitor.py

# Continuous local automation (every 8 hours)
python monitor.py --every-hours 8

# Automation smoke test: 2 quick cycles (for validation)
python monitor.py --dry-run --every-hours 0.001 --max-cycles 2

# Dry run (no notifications sent)
python monitor.py --dry-run

# Help
python monitor.py --help
```

### 5. Pause / Resume

Create `pause.txt` in the repo root to pause the bot. Delete it to resume.

## GitHub Actions

| Workflow | Schedule | Purpose |
|---|---|---|
| `job_monitor.yml` | Every 8 hours | Main job search + notify (links.txt company targeting first, plus Playwright → LangChain → CrewAI → BS4 fallback with pagination/related ATS traversal) |
| `weekly_report.yml` | Sunday 9 AM PKT | Weekly summary + archive |

### Required Secrets

| Secret | Description |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Telegram Bot API token |
| `TELEGRAM_CHAT_ID` | Your Telegram chat ID |
| `GH_PAT` | GitHub PAT (repo scope only) |
| `JSEARCH_API_KEY` | RapidAPI JSearch key |
| `GOOGLE_CREDENTIALS_JSON` | Base64-encoded service account JSON |
| `GOOGLE_SHEET_ID` | Spreadsheet ID from URL |
| `GOOGLE_SERVICE_ACCOUNT_EMAIL` | Service account email |
| `EMAIL_SENDER` | Gmail address |
| `EMAIL_APP_PASSWORD` | Gmail App Password (16-char) |
| `EMAIL_RECIPIENT` | Alert recipient email |
| `GROQ_API_KEY` | GROQ API key for AI scoring/summaries |

## Notification Fallback Chain

```
1. Telegram (inline buttons: Apply Now + View Sheet)
2. Google Sheets (persistent record)
3. Email (HTML + plain-text fallback)
4. failed_alerts.json (last resort — nothing lost)
```

## Troubleshooting

| Problem | Fix |
|---|---|
| No jobs found | Check `jobs.txt` keywords, try broader terms |
| API 429 errors | Free tier: 500 calls/month. Reduce `SEARCH_LOCATIONS` |
| Sheets permission denied | Share sheet with service account email as Editor |
| Email auth failed | Enable 2FA → generate App Password (see EMAIL_SETUP.md) |
| Bot not responding | Check `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` |
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



