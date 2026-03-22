#!/usr/bin/env python3

import requests
import re
import csv
import sqlite3
import os
import gspread
from bs4 import BeautifulSoup
from datetime import datetime

# ── Configuration ──────────────────────────────────────────────────────────────
URL = "https://account.citibikenyc.com/bike-angels/leaderboard"
TARGET_ID = "NS143"

BASE_DIR = os.path.dirname(__file__)
CSV_PATH = os.path.join(BASE_DIR, "ns143_points.csv")
DB_PATH  = os.path.join(BASE_DIR, "ns143_points.db")

# IMPORTANT: this matches your GitHub Actions YAML
CREDENTIALS_PATH = os.environ.get("GOOGLE_CREDENTIALS_PATH")
SHEET_ID = os.environ.get("SHEET_ID")

CSV_HEADERS = ["date", "time", "rank", "id", "points"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://account.citibikenyc.com/",
}

# ── Fetch ──────────────────────────────────────────────────────────────────────
def fetch_page(url):
    print("Fetching leaderboard...")
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text

# ── Parse ──────────────────────────────────────────────────────────────────────
def parse_leaderboard(html, target_id, top_n=10):
    soup = BeautifulSoup(html, "html.parser")
    results = []
    seen_ids = set()

    for r in soup.select("tr"):
        parts = r.get_text(" ", strip=True).split()
        if not parts or not parts[0].isdigit():
            continue

        rank = int(parts[0])
        rider_id, points = None, None

        for i, token in enumerate(parts[1:], 1):
            if re.match(r'^[A-Z]{2}\d+$', token):
                rider_id = token
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

    # Ensure TARGET_ID is included
    if target_id not in seen_ids:
        text_node = soup.find(string=lambda s: s and target_id in s)
        if text_node:
            parent = text_node.parent
            while parent and parent.name not in ("tr", "li", "div"):
                parent = parent.parent

            if parent:
                parts = parent.get_text(" ", strip=True).split()
                rank = int(parts[0]) if parts and parts[0].isdigit() else "?"

                for i, token in enumerate(parts):
                    if token == target_id:
                        for j in range(i + 1, min(i + 6, len(parts))):
                            t = parts[j].replace(",", "")
                            if t.isdigit():
                                results.append({
                                    "rank": rank,
                                    "id": target_id,
                                    "points": int(t)
                                })
                                break
                        break

    print(f"Parsed {len(results)} riders")
    print("Using credentials:", CREDENTIALS_PATH)
    return results

# ── CSV ────────────────────────────────────────────────────────────────────────
def write_csv(rows, date_str, time_str):
    file_exists = os.path.isfile(CSV_PATH)

    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if not file_exists:
            writer.writeheader()

        for row in rows:
            writer.writerow({
                "date": date_str,
                "time": time_str,
                "rank": row["rank"],
                "id": row["id"],
                "points": row["points"],
            })

    print("CSV updated")

# ── SQLite ─────────────────────────────────────────────────────────────────────
def write_db(rows, date_str, time_str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS points (
            date TEXT,
            time TEXT,
            rank INTEGER,
            id TEXT,
            points INTEGER
        )
    """)

    for row in rows:
        cur.execute(
            "INSERT INTO points VALUES (?, ?, ?, ?, ?)",
            (date_str, time_str, row["rank"], row["id"], row["points"])
        )

    con.commit()
    con.close()
    print("Database updated")

# ── Google Sheets ──────────────────────────────────────────────────────────────
def write_gsheet(rows, date_str, time_str):
    print("Writing to Google Sheets...")

    if not CREDENTIALS_PATH or not os.path.exists(CREDENTIALS_PATH):
        raise RuntimeError(f"Credentials file not found: {CREDENTIALS_PATH}")

    if not SHEET_ID:
        raise RuntimeError("SHEET_ID not set")

    gc = gspread.service_account(filename=CREDENTIALS_PATH)
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.sheet1

    if not ws.get_all_values():
        ws.append_row(CSV_HEADERS)

    for row in rows:
        ws.append_row([
            date_str,
            time_str,
            row["rank"],
            row["id"],
            row["points"]
        ])

    print("Google Sheets updated")

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    now = datetime.now()
    date_str = now.strftime("%B %d, %Y")
    time_str = now.strftime("%-I:%M%p").lower()

    html = fetch_page(URL)
    rows = parse_leaderboard(html, TARGET_ID)

    write_csv(rows, date_str, time_str)
    write_db(rows, date_str, time_str)
    write_gsheet(rows, date_str, time_str)

    print(f"\n[{date_str} {time_str}] Results:")
    for r in rows:
        marker = " ← NS143" if r["id"] == TARGET_ID else ""
        print(f"#{r['rank']} {r['id']}: {r['points']} pts{marker}")

# ── Run ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
