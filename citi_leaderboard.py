#!/usr/bin/env python3
"""
Unified leaderboard access for PV758:

- Locally: reads from CSV (`ns143_points.csv`) if it exists.
- Streamlit Cloud: reads from Google Sheets using service account secret.
"""

import os
import csv
import json

TARGET_ID = "NS143"
CSV_PATH = os.path.join(os.path.dirname(__file__), "ns143_points.csv")


def snapshot_pv758():
    """
    Returns a simple object with rank, points, points_behind_first, fetched_at.
    Tries Google Sheets if running in Streamlit Cloud, else CSV.
    """
    # Check if running in Streamlit
    try:
        import streamlit as st
        in_streamlit = True
    except ImportError:
        in_streamlit = False

    # --- Streamlit Cloud: use Google Sheets ---
    if in_streamlit and "GOOGLE_CREDENTIALS_JSON" in st.secrets:
        try:
            import gspread

            creds_dict = json.loads(st.secrets["GOOGLE_CREDENTIALS_JSON"])
            gc = gspread.service_account_from_dict(creds_dict)
            sh = gc.open_by_key(st.secrets["SHEET_ID"])
            ws = sh.sheet1
            rows = ws.get_all_records()

            # Search from the most recent rows first
            for row in reversed(rows):
                row_id = str(row.get("id", "")).strip()
                if row_id == TARGET_ID:
                    class LB:
                        rank = int(row.get("rank"))
                        points = int(row.get("points", 0))
                        points_behind_first = 0
                        fetched_at = f"{row.get('date')} {row.get('time')}"
                    return LB()
        except Exception as e:
            print("DEBUG: Could not fetch PV758 from Google Sheets:", e)
            # Fall back to CSV if available

    # --- Local fallback: use CSV ---
    if os.path.exists(CSV_PATH):
        try:
            with open(CSV_PATH, newline="") as f:
                reader = list(csv.DictReader(f))
                for row in reversed(reader):
                    row_id = str(row.get("id", "")).strip()
                    if row_id == TARGET_ID:
                        class LB:
                            rank = int(row.get("rank"))
                            points = int(row.get("points", 0))
                            points_behind_first = 0
                            fetched_at = f"{row.get('date')} {row.get('time')}"
                        return LB()
        except Exception as e:
            print("DEBUG: Could not fetch PV758 from CSV:", e)

    # --- No leaderboard available ---
    return None
