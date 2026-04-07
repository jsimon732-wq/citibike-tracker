# citi_bike_app.py
#!/usr/bin/env python3
"""
Interactive Citi Bike map in the browser with auto-refresh and NS143 comparison.

Install (once):
  pip install streamlit folium streamlit-folium streamlit-autorefresh requests beautifulsoup4

Launch (must use streamlit — not plain python):
  streamlit run citi_bike_app.py

With a venv:
  .venv-citi/bin/streamlit run citi_bike_app.py
"""

from __future__ import annotations

import html
import sys
import urllib.error


def _running_in_streamlit() -> bool:
    """True when this script is executed by `streamlit run`, not `python ...`."""
    import streamlit.runtime as st_runtime

    return st_runtime.exists()


def main() -> None:
    # ---------------------------------------------------------
    # 1. New Imports Needed for Custom Legend
    # ---------------------------------------------------------
    import folium
    import branca.element # Used for custom HTML
    from folium.plugins import HeatMap
    from streamlit_autorefresh import st_autorefresh
    from streamlit_folium import st_folium

    # NOTE: assuming heat_red_green_weights is still needed, 
    # though yellow logic is explicitly defined here now.
    from citi_bike_scraper import heat_red_green_weights, scrape_availability

    # Change refresh to 1 minute (60,000 ms)
    REFRESH_MS = 60_000

    st.set_page_config(page_title="Ride on PV758", layout="wide")
    st.markdown(
        '<p style="font-size: 2.75rem; font-weight: 700; margin-top: -0.85rem; '
        "margin-bottom: 0.4rem; line-height: 1.15;\">Ride on PV758</p>",
        unsafe_allow_html=True,
    )
    st.markdown(
        # Increased margin-bottom to 1.5rem for desired spacing
        '<p style="font-size: 1.275rem; color: #e4e4e4; margin: 0 0 1.5rem 0;">'
        "Map refreshes every minute.</p>",
        unsafe_allow_html=True,
    )

    try:
        stations = scrape_availability(min_bikes=0, min_docks=0)
    except urllib.error.URLError as e:
        st.error(f"Network error while fetching GBFS data: {e}")
        st.stop()
    except Exception as e:
        st.error(f"Unexpected error: {e}")
        st.stop()

    valid = [
        s for s in stations if s.get("latitude") is not None and s.get("longitude") is not None
    ]
    if not valid:
        st.warning("No stations with coordinates.")
        st.stop()

    lb = None
    lb_err: str | None = None
    try:
        from citi_leaderboard import snapshot_pv758

        lb = snapshot_pv758() # Data for PV758, now includes NS143 data
    except Exception as e:
        lb_err = str(e)

    if lb is not None:
        c1, c2, c3, c4 = st.columns(4)
        rank_disp = f"#{lb.rank}" if isinstance(lb.rank, int) else str(lb.rank)
        c1.metric("Points", f"{lb.points:,}")
        c2.metric("Rank", rank_disp)
        
        # ---------------------------------------------------------
        # 2. NS143 Comparison Metric with custom HTML/CSS
        # ---------------------------------------------------------
        
        # 1. Handle case where NS143 isn't found (fallback)
        if lb.ns143_points is None:
            # Revert to standard metric if comparison data is missing
            c3.metric("Behind 1st place", f"{lb.points_behind_first:,} pts")
        else:
            # 2. Calculate comparison (PV758 vs NS143)
            diff = lb.points - lb.ns143_points
            
            # 3. Determine Prefix (+ or -) and Color (Green or Red)
            if diff >= 0:
                diff_prefix = "+"
                diff_color = "#3dd56d" # Streamlit Green (success color)
            else:
                diff_prefix = "" # Negative numbers include their own '-' sign
                diff_color = "#ff4b4b" # Streamlit Red (error color)
            
            # 4. Format the comparison string (e.g., +1,234 pts)
            ns_comp_text = f"{diff_prefix}{diff:,} pts"

            # 5. Build Custom HTML replicating st.metric while adding comparison label
            # The structure is: Label (Behind 1st place) + Smaller styled text for NS143 comparison
            metric_html = f"""
                <div data-testid="stMetric" style="width: 100%;">
                    <label data-testid="stMetricLabel" style="font-size: 14px; color: rgba(250, 250, 250, 0.6); display: flex; align-items: baseline; gap: 6px;">
                        <div>Behind 1st place</div>
                        <div style="font-size: 11px; opacity: 0.8;">vs NS143: <span style="color: {diff_color}; font-weight: 600;">{ns_comp_text}</span></div>
                    </label>
                    <div data-testid="stMetricValue" style="font-size: 32px; font-weight: 400; color: rgb(250, 250, 250); padding-top: 2px;">
                        {lb.points_behind_first:,} pts
                    </div>
                </div>
            """
            # Render the custom HTML in c3
            c3.markdown(metric_html, unsafe_allow_html=True)
            
        #c4.metric("Updated at", lb.fetched_at)
    else:
        # standard fallback message...
        msg = "Could not load Bike Angels leaderboard for PV758."
        if lb_err:
            st.warning(f"{msg} {lb_err}")
        else:
            st.info(msg + " Check that PV758 appears on the leaderboard page.")

    center_lat = sum(s["latitude"] for s in valid) / len(valid)
    center_lon = sum(s["longitude"] for s in valid) / len(valid)

    m
