"""
Email Notifier
──────────────
Sends HTML job-alert emails and daily summaries via Gmail SMTP.

SECURITY NOTE:
  Use Gmail App Passwords, NEVER store your real Gmail password.
  Enable 2FA → Google Account → Security → App Passwords → generate a 16-char code.
  Store it as EMAIL_APP_PASSWORD in your environment / GitHub Secrets.
"""

from __future__ import annotations

import logging
import os
import smtplib
import time
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

log = logging.getLogger(__name__)

# ── SMTP config ──────────────────────────────────────────────────────────────
_SMTP_HOST = "smtp.gmail.com"
_SMTP_PORT = 587
_SMTP_TIMEOUT = 30

# Rate limiting: minimum seconds between emails
_MIN_EMAIL_INTERVAL = 300  # 5 minutes


class EmailNotifier:
    """Send job-alert emails via Gmail SMTP with rate limiting."""

    def __init__(
        self,
        sender_email: str = "",
        sender_password: str = "",
        recipient_email: str = "",
    ) -> None:
        """
        Initialise the notifier.

        Reads credentials from env vars if constructor args are empty:
          EMAIL_SENDER       (or fallback SMTP_SENDER_EMAIL)
          EMAIL_APP_PASSWORD (or fallback SMTP_SENDER_PASSWORD)
          EMAIL_RECIPIENT    (or fallback SMTP_RECIPIENT_EMAIL)

        Args:
            sender_email:    Gmail address to send from.
            sender_password: Gmail App Password (16-char code, NOT real password).
            recipient_email: Recipient address.
        """
        self.sender_email = (
            sender_email
            or os.environ.get("EMAIL_SENDER", "").strip()
            or os.environ.get("SMTP_SENDER_EMAIL", "").strip()
        )
        self.sender_password = (
            sender_password
            or os.environ.get("EMAIL_APP_PASSWORD", "").strip()
            or os.environ.get("SMTP_SENDER_PASSWORD", "").strip()
        )
        self.recipient_email = (
            recipient_email
            or os.environ.get("EMAIL_RECIPIENT", "").strip()
            or os.environ.get("SMTP_RECIPIENT_EMAIL", "").strip()
        )
        self._last_sent_at: float = 0.0

        if not self.sender_email or not self.sender_password or not self.recipient_email:
            log.warning(
                "Email notifier: one or more SMTP credentials missing. "
                "Emails will be skipped until credentials are set."
            )

    @property
    def is_configured(self) -> bool:
        """Return True if all three credential fields are populated."""
        return bool(self.sender_email and self.sender_password and self.recipient_email)

    # ── rate limiter ─────────────────────────────────────────────────────
    def _check_rate_limit(self) -> bool:
        """Return True if we are allowed to send now."""
        elapsed = time.time() - self._last_sent_at
        if elapsed < _MIN_EMAIL_INTERVAL:
            remaining = int(_MIN_EMAIL_INTERVAL - elapsed)
            log.warning(
                "Email rate-limited. Next send allowed in %ds.", remaining)
            return False
        return True

    def _record_send(self) -> None:
        self._last_sent_at = time.time()

    # ── internal send ────────────────────────────────────────────────────
    def _send(self, subject: str, html_body: str, plain_body: str = "") -> bool:
        """
        Connect to Gmail SMTP, authenticate, send, and disconnect.
        Returns True on success, False on any failure.
        """
        if not self.is_configured:
            log.warning("Email not configured — skipping send.")
            return False

        if not self._check_rate_limit():
            return False

        msg = MIMEMultipart("alternative")
        msg["From"] = self.sender_email
        msg["To"] = self.recipient_email
        msg["Subject"] = subject

        if plain_body:
            msg.attach(MIMEText(plain_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        try:
            with smtplib.SMTP(_SMTP_HOST, _SMTP_PORT, timeout=_SMTP_TIMEOUT) as srv:
                srv.ehlo()
                srv.starttls()
                srv.ehlo()
                srv.login(self.sender_email, self.sender_password)
                srv.sendmail(self.sender_email,
                             self.recipient_email, msg.as_string())

            self._record_send()
            log.info("✅ Email sent: '%s' → %s", subject, self.recipient_email)
            return True

        except smtplib.SMTPAuthenticationError:
            log.error(
                "SMTP authentication failed. "
                "Ensure you are using a Gmail App Password (not your real password) "
                "and that 2FA is enabled on the sending account."
            )
        except smtplib.SMTPRecipientsRefused:
            log.error("Recipient refused: %s", self.recipient_email)
        except smtplib.SMTPException as exc:
            log.error("SMTP error: %s", exc)
        except OSError as exc:
            log.error("Network error sending email: %s", exc)
        return False

    # ── public API ───────────────────────────────────────────────────────
    def send_job_alert_email(
        self,
        jobs: list[dict[str, Any]],
        search_query: str = "",
        sheet_link: str = "",
    ) -> bool:
        """
        Send an HTML email with a jobs table.

        Args:
            jobs:         List of job dicts.
            search_query: Descriptive label for the email subject.
            sheet_link:   Optional link to the Google Sheet for the footer.

        Returns:
            True on success, False on failure.
        """
        if not jobs:
            log.info("No jobs to email — skipping.")
            return True

        subject = f"🎯 New Job Alerts: {len(jobs)} position{'s' if len(jobs) != 1 else ''} found"
        if search_query:
            subject += f" — {search_query}"

        rows_html = ""
        cover_points_sections: list[str] = []
        for j in jobs:
            title = _esc(j.get("job_title", ""))
            company = _esc(j.get("employer_name") or j.get("company", ""))
            loc = _esc(j.get("location", j.get("job_location", "")))
            posted = _esc(
                (j.get("posted_at") or j.get("posted_date", ""))[:10])
            link = j.get("apply_link", "")
            ai_score = _esc(str(j.get("ai_score", "")))
            rows_html += (
                "<tr>"
                f"<td style='padding:8px;border:1px solid #ddd'>{title}</td>"
                f"<td style='padding:8px;border:1px solid #ddd'>{company}</td>"
                f"<td style='padding:8px;border:1px solid #ddd'>{loc}</td>"
                f"<td style='padding:8px;border:1px solid #ddd'>{posted}</td>"
                f"<td style='padding:8px;border:1px solid #ddd;text-align:center'>{ai_score}</td>"
                f"<td style='padding:8px;border:1px solid #ddd'>"
                f"<a href='{link}'>Apply ↗</a></td>"
                "</tr>"
            )

            points = j.get("cover_letter_points")
            if isinstance(points, list) and points:
                points_html = "".join(f"<li>{_esc(str(p))}</li>" for p in points[:3])
                cover_points_sections.append(
                    "<div style='margin-top:12px;padding:10px;border:1px solid #eee;border-radius:6px'>"
                    f"<strong>{title}</strong>"
                    "<ul style='margin:8px 0 0 18px;padding:0'>"
                    f"{points_html}"
                    "</ul>"
                    "</div>"
                )

        footer = ""
        if sheet_link:
            footer = (
                f"<p style='margin-top:20px;font-size:13px;color:#666'>"
                f"Manage all alerts: <a href='{sheet_link}'>Google Sheet</a></p>"
            )

        cover_points_html = ""
        if cover_points_sections:
            cover_points_html = (
                "<h3 style='margin-top:16px'>📝 Cover Letter Talking Points</h3>"
                + "".join(cover_points_sections)
            )

        html = f"""\
<html>
<body style="font-family:Arial,sans-serif;color:#333">
  <h2 style="color:#1a73e8">🎯 Job Alert</h2>
  <p>{len(jobs)} new position{'s' if len(jobs) != 1 else ''} matched your criteria.</p>
  <table style="border-collapse:collapse;width:100%">
    <tr style="background:#1a73e8;color:white">
      <th style="padding:10px;border:1px solid #ddd">Job Title</th>
      <th style="padding:10px;border:1px solid #ddd">Company</th>
      <th style="padding:10px;border:1px solid #ddd">Location</th>
      <th style="padding:10px;border:1px solid #ddd">Posted</th>
            <th style="padding:10px;border:1px solid #ddd">AI Score</th>
      <th style="padding:10px;border:1px solid #ddd">Apply</th>
    </tr>
    {rows_html}
  </table>
    {cover_points_html}
  {footer}
</body>
</html>"""

        plain_lines: list[str] = []
        for j in jobs:
            base_line = (
                f"- {j.get('job_title', '')} @ {j.get('employer_name', j.get('company', ''))} "
                f"[AI Score: {j.get('ai_score', 'N/A')}] — {j.get('apply_link', '')}"
            )
            plain_lines.append(base_line)

            points = j.get("cover_letter_points")
            if isinstance(points, list) and points:
                for p in points[:3]:
                    plain_lines.append(f"    • {p}")

        plain = "\n".join(plain_lines)

        return self._send(subject, html, plain)

    def send_daily_summary(
        self,
        total_found: int,
        total_new: int,
        sheet_link: str = "",
    ) -> bool:
        """Send a simple daily summary email."""
        subject = f"📊 Job Monitor Daily Summary — {total_new} new of {total_found} found"
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        sheet_line = f"\nView all jobs: {sheet_link}" if sheet_link else ""

        plain = (
            f"Job Monitor Summary — {now}\n"
            f"{'=' * 40}\n"
            f"Total qualified jobs found: {total_found}\n"
            f"New jobs (first seen):      {total_new}\n"
            f"{sheet_line}\n"
        )

        html = f"""\
<html>
<body style="font-family:Arial,sans-serif;color:#333">
  <h2>📊 Job Monitor Summary</h2>
  <p style="color:#666">{now}</p>
  <table style="border-collapse:collapse">
    <tr><td style="padding:6px 12px"><strong>Total found</strong></td>
        <td style="padding:6px 12px">{total_found}</td></tr>
    <tr><td style="padding:6px 12px"><strong>New jobs</strong></td>
        <td style="padding:6px 12px">{total_new}</td></tr>
  </table>
  {"<p>View all: <a href='" + sheet_link + "'>Google Sheet</a></p>" if sheet_link else ""}
</body>
</html>"""

        return self._send(subject, html, plain)

    def send_health_warning(self, health_results: dict) -> bool:
        """Send a warning email when health_check() detects failures."""
        failed = {k: v for k, v in health_results.items() if v is False}
        if not failed:
            return True

        subject = f"⚠️ Job Monitor: {len(failed)} channel(s) unhealthy"
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        rows = "\n".join(
            f"  • {ch}: {'❌ FAILED' if v is False else v}" for ch, v in health_results.items())
        plain = f"Health Check Warning — {now}\n\n{rows}\n\nPlease check your configuration."

        html_rows = "".join(
            (
                "<tr>"
                f"<td style='padding:6px 12px'><strong>{ch}</strong></td>"
                f"<td style='padding:6px 12px;color:{'#e53e3e' if v is False else '#38a169'}'>"
                f"{'❌ FAILED' if v is False else ('✅ OK' if v is True else str(v))}</td>"
                "</tr>"
            )
            for ch, v in health_results.items()
        )

        html = f"""\
<html>
<body style="font-family:Arial,sans-serif;color:#333">
  <h2 style="color:#e53e3e">⚠️ Job Monitor Health Warning</h2>
  <p style="color:#666">{now}</p>
  <table style="border-collapse:collapse">
    {html_rows}
  </table>
</body>
</html>"""

        return self._send(subject, html, plain)

    def health_check(self) -> bool:
        """Test SMTP connectivity (login) without sending. Returns True/False."""
        if not self.is_configured:
            return False
        try:
            with smtplib.SMTP(_SMTP_HOST, _SMTP_PORT, timeout=_SMTP_TIMEOUT) as srv:
                srv.ehlo()
                srv.starttls()
                srv.ehlo()
                srv.login(self.sender_email, self.sender_password)
            log.info("Email health check passed.")
            return True
        except Exception as exc:
            log.error("Email health check failed: %s", exc)
            return False


# ── Utility ──────────────────────────────────────────────────────────────────
def _esc(text: str) -> str:
    """Minimal HTML-escape for table cell content."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
