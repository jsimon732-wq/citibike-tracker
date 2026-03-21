#!/usr/bin/env python3
"""
Scraper for Citi Bike Bike Angels leaderboard.
Tracks top 5 leaders + NS143 every run.
Requirements:
source /Users/joannasimon/ns143-venv/bin/activate
pip install requests beautifulsoup4 gspread
"""
import requests
import re
import csv
import sqlite3
import os
import gspread
from bs4 import BeautifulSoup
from datetime import datetime

# Configuration
URL = "https://account.citibikenyc.com/bike-angels/leaderboard"
TARGET_ID = "NS143"
CSV_PATH = os.path.join(os.path.dirname(__file__), "ns143_points.csv")
DB_PATH  = os.path.join(os.path.dirname(__file__), "ns143_points.db")
USER_AGENT = "NS143-tracker-bot/1.0 (+mailto:jsimon732@gmail.com)"
CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH", "/Users/joannasimon/citibike-tracker-490800-2438cbb4be63.json")
SHEET_ID = os.environ.get("SHEET_ID", "1ZeUAh1Cz0Rtmx9wnK57AjR31bddNslicdLfVpyfG2Fk")
CSV_HEADERS = ["date", "time", "rank", "id", "points"]


# ── fetch ──────────────────────────────────────────────────────────────────────
def fetch_page(url):
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.text


# ── parse ──────────────────────────────────────────────────────────────────────
def parse_leaderboard(html, target_id, top_n=10):
    soup = BeautifulSoup(html, "html.parser")
    results = []
    seen_ids = set()

    rows = soup.select("tr")
    for r in rows:
        text = r.get_text(" ", strip=True)
        parts = text.split()
        # Look for rows that start with a rank number
        if not parts or not parts[0].isdigit():
            continue
        rank = int(parts[0])
        # Find the rider ID and points in the row
        rider_id = None
        points = None
        for i, token in enumerate(parts[1:], 1):
            if re.match(r'^[A-Z]{2}\d+$', token):  # matches IDs like NS143, AB12
                rider_id = token
                # look for points after the ID
                for j in range(i + 1, min(i + 6, len(parts))):
                    t = parts[j].replace(",", "")
                    if t.isdigit():
                        points = int(t)
                        break
                break
        if rider_id and points is not None and rider_id not in seen_ids:
            seen_ids.add(rider_id)
            if rank <= top_n:
                results.append({"rank": rank, "id": rider_id, "points": points})

    # Always include NS143 even if outside top 5
    if target_id not in seen_ids:
        text_node = soup.find(string=lambda s: s and target_id in s)
        if text_node:
            parent = getattr(text_node, "parent", None)
            while parent is not None and parent.name not in ("tr", "li", "div"):
                parent = parent.parent
            if parent:
                row_text = parent.get_text(" ", strip=True)
                parts = row_text.split()
                rank = int(parts[0]) if parts and parts[0].isdigit() else "?"
                for i, token in enumerate(parts):
                    if token == target_id:
                        for j in range(i + 1, min(i + 6, len(parts))):
                            t = parts[j].replace(",", "")
                            if t.isdigit():
                                results.append({"rank": rank, "id": target_id, "points": int(t)})
                                break
                        break

    return results


# ── CSV ────────────────────────────────────────────────────────────────────────
def write_csv(path, rows: list, date_str, time_str):
    file_exists = os.path.isfile(path)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({"date": date_str, "time": time_str, "rank": row["rank"], "id": row["id"], "points": row["points"]})


# ── SQLite ─────────────────────────────────────────────────────────────────────
def write_db(path, rows: list, date_str, time_str):
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS points (
            date    TEXT,
            time    TEXT,
            rank    INTEGER,
            id      TEXT,
            points  INTEGER
        )
    """)
    for row in rows:
        cur.execute(
            "INSERT INTO points (date, time, rank, id, points) VALUES (?, ?, ?, ?, ?)",
            (date_str, time_str, row["rank"], row["id"], row["points"])
        )
    con.commit()
    con.close()


# ── Google Sheets ──────────────────────────────────────────────────────────────
import json

def write_gsheet(rows: list, date_str, time_str):
    creds_json = os.environ.get("GOOGLE_CREDS")
    if not creds_json:
        raise ValueError("GOOGLE_CREDS not set")

    creds_dict = json.loads(creds_json)
    gc = gspread.service_account_from_dict(creds_dict)

    sh = gc.open_by_key(SHEET_ID)
    ws = sh.sheet1

    if not ws.get_all_values():
        ws.append_row(CSV_HEADERS)

    for row in rows:
        ws.append_row([date_str, time_str, row["rank"], row["id"], row["points"]])


# ── main ───────────────────────────────────────────────────────────────────────
def main():
    now = datetime.now()
    date_str = now.strftime("%B %d, %Y")
    time_str = now.strftime("%-I:%M%p").lower()

    html = fetch_page(URL)
    rows = parse_leaderboard(html, TARGET_ID, top_n=10)

    write_csv(CSV_PATH, rows, date_str, time_str)
    write_db(DB_PATH, rows, date_str, time_str)
    write_gsheet(rows, date_str, time_str)

    print(f"[{date_str} {time_str}] Scraped {len(rows)} riders:")
    for r in rows:
        ns_marker = " ← NS143" if r["id"] == TARGET_ID else ""
        print(f"  #{r['rank']} {r['id']}: {r['points']} points{ns_marker}")


if __name__ == "__main__":
    main()
