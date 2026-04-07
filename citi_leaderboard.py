# citi_leaderboard.py
"""
Bike Angels leaderboard helpers for standard metrics layout and legend toggling.

Uses the same page and parsing approach as citibike_leaderboard_scraper.py.
Requires: pip install requests beautifulsoup4
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup

LEADERBOARD_URL = "https://account.citibikenyc.com/bike-angels/leaderboard"
TARGET_RIDER_ID = "PV758"
COMPARISON_TARGET_ID = "NS143" 
USER_AGENT = "RideOnPV758/1.0 (+mailto:jsimon732@gmail.com)"


@dataclass
class LeaderboardSnapshot:
    points: int
    rank: int | str
    first_place_points: int
    points_behind_first: int
    fetched_at: str
    
    # ---------------------------------------------------------
    # 1. NEW/UPDATED: Fields for standard NS143 metrics layout
    # ---------------------------------------------------------
    # Contains the point difference or the fallback message
    ns143_diff_str: str | None = None
    # Contains the hex color code for the value
    ns143_color: str = "#e4e4e4" # Default Gray


def fetch_leaderboard_html() -> str:
    resp = requests.get(
        LEADERBOARD_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.text


def _parse_row_text(parts: list[str]) -> tuple[int, str, int] | None:
    if not parts or not parts[0].isdigit():
        return None
    rank = int(parts[0])
    rider_id = None
    points = None
    for i, token in enumerate(parts[1:], 1):
        if re.match(r"^[A-Z]{2}\d+$", token):
            rider_id = token
            for j in range(i + 1, min(i + 6, len(parts))):
                t = parts[j].replace(",", "")
                if t.isdigit():
                    points = int(t)
                    break
            break
    if rider_id is None or points is None:
        return None
    return rank, rider_id, points


def _parse_all_table_rows(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    seen_ids: set[str] = set()
    out: list[dict] = []
    for r in soup.select("tr"):
        text = r.get_text(" ", strip=True)
        parts = text.split()
        parsed = _parse_row_text(parts)
        if not parsed:
            continue
        rank, rider_id, points = parsed
        if rider_id in seen_ids:
            continue
        seen_ids.add(rider_id)
        out.append({"rank": rank, "id": rider_id, "points": points})
    return out


def _find_target_fallback(html: str, target_id: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    text_node = soup.find(string=lambda s: s and target_id in s)
    if not text_node:
        return None
    parent = getattr(text_node, "parent", None)
    while parent is not None and parent.name not in ("tr", "li", "div"):
        parent = parent.parent
    if not parent:
        return None
    row_text = parent.get_text(" ", strip=True)
    parts = row_text.split()
    parsed = _parse_row_text(parts)
    if parsed:
        rank, rider_id, points = parsed
        if rider_id == target_id:
            return {"rank": rank, "id": target_id, "points": points}
    rank: int | str = int(parts[0]) if parts and parts[0].isdigit() else "?"
    for i, token in enumerate(parts):
        if token == target_id:
            for j in range(i + 1, min(i + 6, len(parts))):
                t = parts[j].replace(",", "")
                if t.isdigit():
                    return {"rank": rank, "id": target_id, "points": int(t)}
            break
    return None


def snapshot_pv758() -> LeaderboardSnapshot | None:
    """
    Return PV758 points, rank, first-place points, gap to first, AND NS143 points.
    """
    html = fetch_leaderboard_html()
    rows = _parse_all_table_rows(html)
    by_id = {r["id"]: r for r in rows}

    first = None
    for r in rows:
        if r["rank"] == 1:
            first = r
            break
    if first is None and rows:
        first = min(rows, key=lambda x: x["rank"] if isinstance(x["rank"], int) else 10**9)

    target = by_id.get(TARGET_RIDER_ID)
    if target is None:
        target = _find_target_fallback(html, TARGET_RIDER_ID)

    if target is None or first is None:
        return None

    first_pts = int(first["points"])
    tgt_pts = int(target["points"])
    rank = target["rank"]
    behind = max(0, first_pts - tgt_pts)

    # ---------------------------------------------------------
    # 2. UPDATED: Prepare standard metric values for NS143
    # ---------------------------------------------------------
    ns_diff_str = None
    ns_color = "#e4e4e4" # Gray
    
    NS143_BOUNDARY = 10 
    
    ns_target = by_id.get(COMPARISON_TARGET_ID)
    
    # If found, check boundary limit
    if ns_target and isinstance(ns_target.get("rank"), int) and ns_target["rank"] <= NS143_BOUNDARY:
        # Calculate standard point difference
        diff = tgt_pts - int(ns_target["points"])
        
        # Determine prefix and color
        if diff >= 0:
            ns_diff_str = f"+{diff:,}"
            ns_color = "#3dd56d" # Success Green
        else:
            # Negative difference handles its own sign
            ns_diff_str = f"{diff:,}"
            ns_color = "#ff4b4b" # Error Red
        
    # Else: Fallback message
    else:
        ns_diff_str = "NS143 not in Top 10"
        ns_color = "#ffee44" # Custom legible yellow

    return LeaderboardSnapshot(
        points=tgt_pts,
        rank=rank,
        first_place_points=first_pts,
        points_behind_first=behind,
        fetched_at=datetime.now().strftime("%m/%d %H:%M"),
        ns143_diff_str=ns_diff_str,
        ns143_color=ns_color,
    )
