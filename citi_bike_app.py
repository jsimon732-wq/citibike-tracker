#!/usr/bin/env python3
"""
Streamlit Citi Bike map with PV758 leaderboard.
Works locally with CSV or on Streamlit Cloud with Google Sheets.
"""

from __future__ import annotations
import html
import urllib.error
import sys

import streamlit as st
from streamlit_autorefresh import st_autorefresh
from streamlit_folium import st_folium
import folium
from folium.plugins import HeatMap

from citi_bike_scraper import heat_red_green_weights, scrape_availability
from citi_leaderboard import snapshot_pv758

REFRESH_MS = 30_000

st.set_page_config(page_title="Ride on PV758", layout="wide")
st.markdown(
    '<p style="font-size: 2.75rem; font-weight: 700; margin-top: -0.85rem; '
    "margin-bottom: 0.4rem; line-height: 1.15;\">Ride on PV758</p>",
    unsafe_allow_html=True,
)
st.markdown(
    '<p style="font-size: 0.875rem; color: #e4e4e4; margin: 0 0 0.05rem 0;">'
    "Map refreshes every 30 seconds. Pan and zoom freely.</p>",
    unsafe_allow_html=True,
)

# --- Load Citi Bike station data ---
try:
    stations = scrape_availability(min_bikes=0, min_docks=0)
except urllib.error.URLError as e:
    st.error(f"Network error while fetching GBFS data: {e}")
    st.stop()
except Exception as e:
    st.error(f"Unexpected error: {e}")
    st.stop()

valid = [s for s in stations if s.get("latitude") is not None and s.get("longitude") is not None]
if not valid:
    st.warning("No stations with coordinates.")
    st.stop()

# --- Load PV758 leaderboard ---
lb = None
lb_err = None
try:
    lb = snapshot_pv758()
except Exception as e:
    lb_err = str(e)

if lb is not None:
    c1, c2, c3, c4 = st.columns(4)
    rank_disp = f"#{lb.rank}" if isinstance(lb.rank, int) else str(lb.rank)
    c1.metric("Points", f"{lb.points:,}")
    c2.metric("Rank", rank_disp)
    c3.metric("Behind 1st place", f"{lb.points_behind_first:,} pts")
    c4.metric("Updated at", lb.fetched_at)
else:
    msg = "Could not load Bike Angels leaderboard for PV758."
    if lb_err:
        st.warning(f"{msg} {lb_err}")
    else:
        st.info(msg + " Check that PV758 appears on the leaderboard page.")

# --- Compute map center ---
center_lat = sum(s["latitude"] for s in valid) / len(valid)
center_lon = sum(s["longitude"] for s in valid) / len(valid)

# --- Build Folium map ---
m = folium.Map(location=[center_lat, center_lon], zoom_start=11, tiles=None, control_scale=True)
folium.TileLayer("CartoDB Positron", control=False).add_to(m)

green_data, red_data = [], []
for s in valid:
    rw, gw = heat_red_green_weights(s)
    lat, lon = float(s["latitude"]), float(s["longitude"])
    if gw > 0: green_data.append([lat, lon, max(0.35, gw)])
    if rw > 0: red_data.append([lat, lon, max(0.35, rw)])

heat_kw = {"min_opacity": 0.28, "max_zoom": 18, "radius": 9, "blur": 7}

if green_data:
    fg_green = folium.FeatureGroup(name="Plenty of Bikes", show=True)
    HeatMap(green_data, gradient={0.25: "#004400", 0.5: "#00aa44", 0.75: "#44dd66", 1: "#aaffaa"}, **heat_kw).add_to(fg_green)
    fg_green.add_to(m)

if red_data:
    fg_red = folium.FeatureGroup(name="Low on Bikes", show=True)
    HeatMap(red_data, gradient={0.25: "#440000", 0.5: "#cc2222", 0.75: "#ee6666", 1: "#ffaaaa"}, **heat_kw).add_to(fg_red)
    fg_red.add_to(m)

if not green_data and not red_data:
    st.warning("No stations in extreme bands; widen thresholds or try later.")

# --- Add individual station markers ---
fg_stations = folium.FeatureGroup(name="Stations (click for availability)", show=False)
for s in valid:
    lat, lon = float(s["latitude"]), float(s["longitude"])
    name = html.escape(str(s.get("name", "Unknown")))
    bikes = int(s.get("bikes_available", 0) or 0)
    ebikes = int(s.get("ebikes_available", 0) or 0)
    docks = int(s.get("docks_available", 0) or 0)
    popup_html = (
        f'<div style="font-family: system-ui, sans-serif; font-size: 13px; min-width: 200px;">'
        f"<strong>{name}</strong><br/>"
        f"Bikes available: {bikes} (e-bikes: {ebikes})<br/>"
        f"Docks available: {docks}</div>"
    )
    folium.CircleMarker(location=[lat, lon], radius=2, color="#0d47a1", weight=0.5,
                        fill=True, fill_color="#64b5f6", fill_opacity=0.92,
                        popup=folium.Popup(popup_html, max_width=300)).add_to(fg_stations)
fg_stations.add_to(m)

folium.LayerControl(collapsed=False, position="bottomright").add_to(m)

# --- Adjust spacing for Streamlit ---
st.markdown("""
<style>
div[data-testid="stHorizontalBlock"] { margin-bottom: -0.35rem !important; }
div[data-testid="column"] { padding-top: 0.1rem !important; padding-bottom: 0.1rem !important; }
div[data-testid="stVerticalBlock"] > div:has(iframe[height="560"]),
div[data-testid="stVerticalBlock"] > div:has(iframe[title*="folium"]) { margin-top: -0.9rem !important; }
</style>
""", unsafe_allow_html=True)

st_folium(m, width=None, height=560, returned_objects=[], key="citi_map")

with st.expander("How this relates to the static heat map"):
    st.markdown("""
Each station’s **empty-dock share** is `docks_available / station capacity`.

- **Plenty of Bikes** (green heat): share ≤ 30%
- **Low on Bikes** (red heat): share ≥ 70%
- **Orange**: browser blend where both kinds of stations sit close together.

Toggle layers on the map. For a PNG with the same logic, run:
`python citi_bike_scraper.py --heatmap out.png`
""")

# --- Auto-refresh ---
st_autorefresh(interval=REFRESH_MS, key="citi_refresh")
