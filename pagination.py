from __future__ import annotations

from urllib.parse import parse_qs, urlencode, urlparse, urlunparse


def infer_pagination_mode(url: str) -> str:
    lower = (url or "").lower()
    if "cursor=" in lower:
        return "cursor"
    if "offset=" in lower:
        return "offset"
    if "page=" in lower:
        return "page"
    return "unknown"


def build_next_page_url(url: str, step: int = 1) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)

    if "page" in qs:
        current = int(qs.get("page", ["1"])[0] or "1")
        qs["page"] = [str(max(1, current + step))]
    elif "offset" in qs:
        current = int(qs.get("offset", ["0"])[0] or "0")
        qs["offset"] = [str(max(0, current + step))]
    else:
        qs["page"] = ["2"]

    query = urlencode(qs, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, parsed.fragment))
