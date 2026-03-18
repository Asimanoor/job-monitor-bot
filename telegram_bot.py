import logging
import os
import requests

log = logging.getLogger(__name__)


class TelegramBot:
    def __init__(self, bot_token: str | None = None, chat_id: str | None = None):
        self.bot_token = bot_token or os.environ.get(
            "TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = chat_id or os.environ.get(
            "TELEGRAM_CHAT_ID", "").strip()
        self.session = requests.Session()
        self.telegram_max_len = 4000

    def send_job_alert(self, jobs: list[dict]) -> bool:
        """Send job alerts, max 3 jobs per message to avoid limits."""
        if not self.bot_token or not self.chat_id:
            log.warning("Telegram token or chat ID missing.")
            return False

        # Chunk jobs into groups of 3
        success = True
        for i in range(0, len(jobs), 3):
            chunk = jobs[i:i + 3]
            for job in chunk:
                ai_score = job.get("ai_score")
                score_line = ""
                if ai_score is not None and str(ai_score).strip() != "":
                    score_line = f"\n🤖 Match Score: {ai_score}/100"

                ai_summary = job.get("ai_summary")
                summary_lines: list[str] = []
                if isinstance(ai_summary, list):
                    summary_lines = [str(s).strip() for s in ai_summary if str(s).strip()][:3]
                elif isinstance(ai_summary, str) and ai_summary.strip():
                    summary_lines = [ai_summary.strip()]

                summary_block = ""
                if summary_lines:
                    summary_block = "\n🧠 Summary:\n" + "\n".join(f"• {s}" for s in summary_lines)

                text = (
                    f"🎯 <b>{job.get('job_title', 'Unknown')}</b>\n"
                    f"🏢 {job.get('employer_name', 'Unknown')}\n"
                    f"📍 {job.get('location', 'Remote/Unknown')}\n"
                    f"📅 {job.get('posted_at', 'Unknown dates')}\n"
                    f"🏷️ Matched: {job.get('matched_as', '')} ({job.get('filter_keyword', '')})\n\n"
                    f"{job.get('description', '')[:200]}..."
                    f"{score_line}"
                    f"{summary_block}"
                )

                reply_markup = {
                    "inline_keyboard": [[
                        {"text": "📝 Apply Now", "url": job.get(
                            "apply_link", "https://google.com")}
                    ]]
                }

                # Check for sheet link config
                sheet_id = os.environ.get("GOOGLE_SHEET_ID")
                if sheet_id:
                    reply_markup["inline_keyboard"][0].append(
                        {"text": "📊 View Sheet",
                            "url": f"https://docs.google.com/spreadsheets/d/{sheet_id}"}
                    )

                if not self.send_message(text, reply_markup):
                    success = False
        return success

    def send_message(self, text: str, reply_markup: dict | None = None) -> bool:
        """Post a single message to Telegram, optionally with an inline keyboard."""
        if not self.bot_token or not self.chat_id:
            return False

        if len(text) > self.telegram_max_len:
            text = text[:self.telegram_max_len - 30] + \
                "\n\n… (message truncated)"

        api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload: dict[str, object] = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup

        try:
            resp = self.session.post(api_url, json=payload, timeout=10)
            if resp.ok:
                return True
            # Check for 429
            if resp.status_code == 429:
                log.warning("Telegram rate limited. Wait and retry if needed.")
            else:
                log.warning("Telegram API returned %s: %s",
                            resp.status_code, resp.text[:200])
        except requests.RequestException as exc:
            log.warning("Telegram send failed: %s", exc)
        return False

    def process_updates(self, state_manager, notifier=None) -> None:
        """Poll getUpdates for /status, /pause, /resume commands."""
        if not self.bot_token or not self.chat_id:
            return

        try:
            last_update_id = state_manager.get_last_telegram_update_id()
            api_url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
            params = {"offset": last_update_id + 1, "timeout": 0, "limit": 20}

            resp = self.session.get(api_url, params=params, timeout=10)
            if not resp.ok:
                return

            updates = resp.json().get("result", [])
            for update in updates:
                update_id = update.get("update_id", 0)
                state_manager.update_last_telegram_update_id(update_id)

                msg = update.get("message", {})
                text = (msg.get("text") or "").strip()

                if text.lower() == "/status":
                    self._handle_status(state_manager, notifier)
                elif text.lower() == "/pause":
                    with open("pause.txt", "w") as f:
                        f.write("Paused via Telegram command.")
                    self.send_message(
                        "⏸️ Bot is now paused. Send /resume to restart.")
                elif text.lower() == "/resume":
                    if os.path.exists("pause.txt"):
                        os.remove("pause.txt")
                    self.send_message("▶️ Bot restored and running.")

        except Exception as exc:
            log.warning("Failed to process Telegram commands: %s", exc)

    def _handle_status(self, state_manager, notifier) -> None:
        notified_count = len(state_manager.state.get("notified_job_ids", []))
        url_count = len(state_manager.state.get("url_hashes", {}))
        api_usage = state_manager.state.get("api_usage", {}).get("count", 0)

        health_info = "N/A"
        if notifier is not None and hasattr(notifier, "health_check"):
            try:
                h = notifier.health_check()
                health_info = "\n".join(
                    f"  {ch}: {'✅' if v is True else '❌' if v is False else str(v)}"
                    for ch, v in h.items()
                )
            except Exception:
                health_info = "Error running health check"

        reply = (
            "📊 <b>Job Monitor Status</b>\n\n"
            f"🔗 URLs tracked: {url_count}\n"
            f"📌 Jobs notified: {notified_count}\n"
            f"⚙️ API calls this month: {api_usage}/500\n\n"
            f"<b>Channel Health:</b>\n{health_info}"
        )
        self.send_message(reply)
