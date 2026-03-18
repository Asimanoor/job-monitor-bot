"""
Email Templates
───────────────
Responsive, mobile-friendly HTML email templates.
Design: FAST NUCES brand colors (navy blue #1a3a5c / white / light gray).

All templates include a plain-text fallback.
"""

from __future__ import annotations

from datetime import datetime, timezone


# ── Brand colours ────────────────────────────────────────────────────────────
_PRIMARY = "#1a3a5c"   # FAST NUCES navy
_ACCENT = "#2e86de"   # link blue
_BG = "#f4f6f9"   # light gray
_CARD_BG = "#ffffff"
_TEXT = "#333333"
_MUTED = "#888888"
_GREEN = "#27ae60"
_TAG_BG = "#e8f0fe"
_TAG_TEXT = "#1a3a5c"


def _esc(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _time_ago(posted_at: str) -> str:
    """Convert ISO date to '2 days ago'-style string."""
    if not posted_at or len(posted_at) < 10:
        return "Recently"
    try:
        dt = datetime.fromisoformat(posted_at[:10])
        delta = datetime.now(timezone.utc).date() - dt.date()
        days = delta.days
        if days == 0:
            return "Today"
        if days == 1:
            return "Yesterday"
        if days < 7:
            return f"{days} days ago"
        if days < 30:
            return f"{days // 7} week{'s' if days // 7 > 1 else ''} ago"
        return f"{days // 30} month{'s' if days // 30 > 1 else ''} ago"
    except Exception:
        return posted_at[:10] if posted_at else "Unknown"


def _tag_html(text: str) -> str:
    return (
        f"<span style='display:inline-block;padding:2px 8px;margin:2px;"
        f"border-radius:12px;background:{_TAG_BG};color:{_TAG_TEXT};"
        f"font-size:12px'>{_esc(text)}</span>"
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  JOB ALERT EMAIL
# ═══════════════════════════════════════════════════════════════════════════════
def render_job_alert_html(
    jobs: list[dict],
    sheet_link: str = "",
    header_name: str = "Fawwaz",
) -> str:
    """
    Render a responsive HTML email with job cards.

    Args:
        jobs:        List of job dicts.
        sheet_link:  URL to Google Sheet.
        header_name: Personalisation for the header.

    Returns:
        Full HTML string.
    """
    cards_html = ""
    for j in jobs:
        title = _esc(j.get("job_title", ""))
        company = _esc(j.get("employer_name") or j.get("company", ""))
        location = _esc(j.get("location", j.get("job_location", "N/A")))
        posted = _time_ago(j.get("posted_at") or j.get("posted_date", ""))
        link = j.get("apply_link", "")
        matched = j.get("matched_as", "")
        fk = j.get("filter_keyword", "")

        tags_html = ""
        if matched:
            tags_html += _tag_html(matched)
        if fk and fk != "No filter":
            tags_html += _tag_html(fk)

        cards_html += f"""
        <div style="background:{_CARD_BG};border-radius:8px;padding:16px;
                     margin-bottom:12px;border-left:4px solid {_ACCENT}">
          <div style="font-size:16px;font-weight:bold;color:{_TEXT};margin-bottom:4px">
            {title}
          </div>
          <div style="font-size:14px;color:{_MUTED};margin-bottom:8px">
            🏢 {company}
          </div>
          <div style="font-size:13px;color:{_MUTED};margin-bottom:8px">
            📍 {location} &nbsp;&nbsp; ⏰ {posted}
          </div>
          <div style="margin-bottom:10px">{tags_html}</div>
          <a href="{link}" style="display:inline-block;padding:8px 20px;
             background:{_ACCENT};color:white;text-decoration:none;
             border-radius:6px;font-size:14px;font-weight:bold">
            🔗 Apply Now
          </a>
        </div>"""

    sheet_footer = ""
    if sheet_link:
        sheet_footer = f"""
        <p style="text-align:center;margin-top:16px">
          <a href="{sheet_link}" style="color:{_ACCENT};text-decoration:underline">
            📊 Manage in Google Sheet
          </a>
        </p>"""

    return f"""\
<html>
<head>
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
</head>
<body style="margin:0;padding:0;background:{_BG};font-family:Arial,Helvetica,sans-serif">
  <div style="max-width:600px;margin:0 auto;padding:20px">

    <!-- Header -->
    <div style="background:{_PRIMARY};border-radius:10px 10px 0 0;padding:20px;text-align:center">
      <h1 style="color:white;margin:0;font-size:22px">
        🎯 Job Alert from {_esc(header_name)}'s Bot
      </h1>
      <p style="color:#ccc;margin:6px 0 0;font-size:14px">
        {len(jobs)} new position{"s" if len(jobs) != 1 else ""} matched
      </p>
    </div>

    <!-- Body -->
    <div style="background:{_BG};padding:16px;border-radius:0 0 10px 10px">
      {cards_html}
      {sheet_footer}
    </div>

    <!-- Footer -->
    <div style="text-align:center;padding:16px;font-size:12px;color:{_MUTED}">
      <p>Powered by Job Monitor Bot</p>
      <p>Pause alerts: create <code>pause.txt</code> in the repo root</p>
    </div>

  </div>
</body>
</html>"""


def render_job_alert_plain(
    jobs: list[dict],
    sheet_link: str = "",
) -> str:
    """Plain-text fallback for the job alert email."""
    lines = [f"🎯 Job Alert — {len(jobs)} new position(s)\n"]
    for j in jobs:
        title = j.get("job_title", "")
        company = j.get("employer_name") or j.get("company", "")
        loc = j.get("location", j.get("job_location", "N/A"))
        posted = _time_ago(j.get("posted_at") or j.get("posted_date", ""))
        link = j.get("apply_link", "")
        lines.append(
            f"• {title} @ {company}\n"
            f"  📍 {loc}  ⏰ {posted}\n"
            f"  🔗 {link}\n"
        )
    if sheet_link:
        lines.append(f"\n📊 Manage all: {sheet_link}")
    lines.append("\nPause alerts: create pause.txt in the repo root")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
#  WEEKLY REPORT EMAIL
# ═══════════════════════════════════════════════════════════════════════════════
def render_weekly_report_html(
    total_found: int,
    total_applied: int,
    total_interviews: int,
    top_companies: list[str],
    top_keywords: list[str],
    sheet_link: str = "",
) -> str:
    companies_html = "".join(f"<li>{_esc(c)}</li>" for c in top_companies[:10])
    keywords_html = "".join(_tag_html(k) for k in top_keywords[:10])

    return f"""\
<html>
<head><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:{_BG};font-family:Arial,Helvetica,sans-serif">
  <div style="max-width:600px;margin:0 auto;padding:20px">
    <div style="background:{_PRIMARY};border-radius:10px 10px 0 0;padding:20px;text-align:center">
      <h1 style="color:white;margin:0;font-size:20px">📊 Weekly Job Report</h1>
    </div>
    <div style="background:{_CARD_BG};padding:20px;border-radius:0 0 10px 10px">
      <table style="width:100%;border-collapse:collapse;margin-bottom:16px">
        <tr><td style="padding:8px;font-weight:bold">Found</td>
            <td style="padding:8px">{total_found} jobs</td></tr>
        <tr><td style="padding:8px;font-weight:bold">Applied</td>
            <td style="padding:8px;color:{_GREEN}">{total_applied}</td></tr>
        <tr><td style="padding:8px;font-weight:bold">Interviews</td>
            <td style="padding:8px;color:{_ACCENT}">{total_interviews}</td></tr>
      </table>
      <h3>Top Companies</h3>
      <ul>{companies_html or "<li>None this week</li>"}</ul>
      <h3>Top Keywords</h3>
      <div>{keywords_html or "None"}</div>
      {"<p style='margin-top:16px'><a href='" + sheet_link + "'>View full sheet ↗</a></p>" if sheet_link else ""}
    </div>
  </div>
</body>
</html>"""


def render_weekly_report_plain(
    total_found: int,
    total_applied: int,
    total_interviews: int,
    top_companies: list[str],
    top_keywords: list[str],
    sheet_link: str = "",
) -> str:
    companies = "\n".join(f"  • {c}" for c in top_companies[:10]) or "  None"
    keywords = ", ".join(top_keywords[:10]) or "None"
    return (
        f"📊 Weekly Job Report\n"
        f"{'=' * 40}\n"
        f"Found: {total_found} | Applied: {total_applied} | Interviews: {total_interviews}\n\n"
        f"Top Companies:\n{companies}\n\n"
        f"Top Keywords: {keywords}\n"
        f"{f'Sheet: {sheet_link}' if sheet_link else ''}"
    )
