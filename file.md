You are upgrading an AI job scraping + monitoring system.

The system currently:

scrapes career URLs from links.txt
detects changes
pushes to Google Sheets
uses AI filtering
has internet fallback search
tracks openings

You must fix issues + implement missing logic.

1. CRITICAL FIX — REMOVE CLOSED POSITIONS

Currently:

new jobs are added
closed jobs remain in sheet

You must implement:

Required Behavior

If job previously existed but no longer scraped:

mark as CLOSED
OR
remove row completely

Implement:

job_id = hash(company + title + location)

previous_jobs - current_jobs = closed_jobs

Then:

Option A (preferred)

Update column:

Status = CLOSED
Closed Date = now

Option B

Remove row completely

Must be configurable:

REMOVE_CLOSED_ROWS = True
2. FIX: DUPLICATE URL DETECTION

Your logs show:

same company tracked multiple times

Example:

venturedive.applytojob.com
venturedive.applytojob.com/#alljobs

Fix:

Normalize URLs:

remove query params
remove anchors
remove trailing slash
lowercase domain
https://site.com/jobs
https://site.com/jobs/
https://site.com/jobs?loc=lahore

→ same
3. FIX: "IGNORED CHANGE (NO VALID JOB POSTINGS)"

This happens because:

scraper fails to detect jobs on:

JS rendered sites
pagination
load more buttons
API-driven lists

Implement fallback scraping strategy:

Step 1

requests + bs4

Step 2

if empty → try:

playwright / selenium

Step 3

if still empty → detect API calls

Example:

/jobs
/api/jobs
/graphql
/workday
/lever
/greenhouse
/breezy
/workable

Then fetch JSON

4. INTERNET SEARCH FALLBACK — IMPROVEMENT

Currently:

search runs but finds very few jobs

You must improve search queries

For each company:

search:

"{company} careers"
"{company} jobs pakistan"
"{company} careers lahore"
"{company} jobs site:lever.co"
"{company} jobs site:greenhouse.io"
"{company} jobs site:workday"
"{company} jobs site:applytojob.com"
"{company} jobs site:workable.com"

Also search:

"{company} associate software engineer"
"{company} associate data science"
"{company} associate ai engineer"
"{company} fresh graduate software engineer"
"{company} ai engineer {company}"
"{company} ai engineer"
"{company} machine learning engineer"
5. CREATE companies_pakistan.txt

Create file:

companies_pakistan.txt

Include:

Pakistani tech companies
companies hiring in Pakistan
remote companies hiring in Pakistan

Example entries:

10Pearls
Afiniti
Aitomation
AIM Digital Technologies
ArhamSoft
Arbisoft
Bazaar Technologies
Big Byte Insights
CareAxiom
Careem
Carbonteq
CareCloud
Cogent Labs
CodeNinja
Colab Software
Confiz
Conrad Labs
Contour Software
Cubix
CureMD
Daraz
Devsinc
Dubizzle Labs
Educative
FiveRivers Technologies
Folio3
Folium AI
GoMotive
Motive
GoSaaS
ILI Digital
InvoZone
i2c Inc.
LMKR
Manafa Technologies
Mavericks United
Maqsood Labs
Maq Dev
NayaPay
NetSol Technologies
Nextbridge
Nisum
NorthBay Solutions
Noetic Technologies
Naseeb Enterprise Inc.
P99soft
Programmers Force
PureLogics
QBXNet
Retailo
SadaPay
SastaTicket
Softpers Interactive
SquareNodes
Strategic Systems International
SSI
Sybrid Pvt Ltd
Systems Limited
Tajir
Techlogix
TKXEL
Turing
Veevo Tech
VentureDive
Vortexian Tech
Zaptatech
Zilon International


File must contain:

these above companies
6. ENTRY LEVEL FILTERING (IMPORTANT)

Only include jobs:

Titles containing:

Associate
Junior
Entry
Graduate
Fresh

OR experience:

0-1 years
0-2 years
1 year
new grad
early career

Exclude:

Senior
Lead
Principal
Manager
Staff
Director

7. FIX: DEDUPLICATE JOBS

Same job appears:

multiple URLs
same company
same title

Create job fingerprint:

fingerprint = hash(
company +
title +
location
)

Only insert if new.

8. ADD SMART COMPANY NAME EXTRACTION

From URL:

jobs.careem.com
→ careem

venturedive.applytojob.com
→ venturedive

Use:

domain parsing
subdomain extraction
title fallback
9. ADD RATE LIMIT PROTECTION

If 429:

exponential backoff
cooldown
retry
retry: 3
delay: 2s, 5s, 10s
10. ADD JOB STATE TRACKING

state.json must store:

{
 job_id:
 {
   title
   company
   first_seen
   last_seen
   status
 }
}

If not seen again:

mark closed

11. FIX: TOO MANY "NEW URL TRACKED"

Do NOT auto-add:

filtered URLs

like:

?location=lahore
?page=2
#jobs

Instead:

track only base domain

12. IMPROVE SCRAPER ENGINE

Must support:

Workday
Greenhouse
Lever
Ashby
Workable
Breezy
SmartRecruiters
ZohoRecruit
SAP SuccessFactors
iCIMS
ApplyToJob
Custom HTML

Auto-detect platform.

13. ADD PAGINATION SUPPORT

Detect:

page=
offset=
cursor=
next button

Loop until exhausted.

14. ADD LOAD MORE SUPPORT

Detect:

"Load more" button

simulate click via Playwright.

15. ADD LOGGING IMPROVEMENTS

Add:

Jobs found
Jobs filtered
Jobs added
Jobs removed
Jobs closed
Duplicates skipped
16. GOOGLE SHEETS SYNC FIX

Before insert:

check if exists

if exists:

update

if closed:

remove

17. ADD PERFORMANCE OPTIMIZATION
concurrent scraping
async requests
caching
skip unchanged pages
18. FINAL OUTPUT FORMAT

Each job:

Company
Title
Location
Experience
URL
Posted Date
Status
First Seen

19. MUST NOT BREAK EXISTING SYSTEM

Keep:

monitor.py
state.json
Google sheets integration
AI filtering

20. SUCCESS CRITERIA

System must:

scrape 80%+ sites successfully
remove closed jobs
no duplicates
detect entry level jobs only
fallback search working
Pakistan companies dataset used
Google sheet always clean

Implement clean modular code