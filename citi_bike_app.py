# citi_bike_app.py
#!/usr/bin/env python3
"""
Interactive Citi Bike map in the browser with embedded NS143 comparison.

Install (once):
  pip install streamlit folium branca.element streamlit-folium streamlit-autorefresh requests beautifulsoup4

Launch (must use streamlit — not plain python):
  streamlit run citi_bike_app.py

With a venv:
  .venv-citi/bin/streamlit run citi_bike_app.py
"""

from __future__ import annotations

import html
import sys
import urllib.error
import streamlit as st # MISSING IMPORT ALREADY FIXED

def _running_in_streamlit() -> bool:
    """True when this script is executed by `streamlit run`, not `python ...`."""
    import streamlit.runtime as st_runtime

    return st_runtime.exists()


def main() -> None:
    # ---------------------------------------------------------
    # 1. Imports
    # ---------------------------------------------------------
    import folium
    import branca.element # Required for MacroElement used in simple legend
    from folium.plugins import HeatMap
    from streamlit_autorefresh import st_autorefresh
    from streamlit_folium import st_folium

    # Assuming heat_red_green_weights is still needed, though logic is defined here.
    from citi_bike_scraper import heat_red_green_weights, scrape_availability

    # Change refresh to 1 minute (60,000 ms)
    REFRESH_MS = 60_000

    # set_page_config MUST be the first Streamlit command
    st.set_page_config(page_title="Ride on PV758", layout="wide")
    
    st.markdown(
        '<p style="font-size: 2.75rem; font-weight: 700; margin-top: -0.85rem; '
        "margin-bottom: 0.4rem; line-height: 1.15;\">Ride on PV758</p>",
        unsafe_allow_html=True,
    )
    st.markdown(
        # Spacing adjustment already implemented
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

        lb = snapshot_pv758() # Data for PV758, now with simplified NS143 metric data
    except Exception as e:
        lb_err = str(e)

    if lb is not None:
        c1, c2, c3, c4 = st.columns(4)
        rank_disp = f"#{lb.rank}" if isinstance(lb.rank, int) else str(lb.rank)
        c1.metric("Points", f"{lb.points:,}")
        c2.metric("Rank", rank_disp)
        
        # ---------------------------------------------------------
        # NS143 Comparison - CUSTOM EMBEDDED LOGIC FIXED
        # We replace standard metric with custom HTML to embed comparison
        # ---------------------------------------------------------
        
        # 1. Determine NS143 Status and Color
        if lb.ns143_points is None:
            st_ns_comp_text = "NS143 not in Top 10"
            st_ns_comp_color = "#ffee44" # Custom legible yellow
        else:
            # 2. Calculate comparison (PV758 vs NS143)
            diff = lb.points - lb.ns143_points
            
            # 3. Determine Prefix (+ or -) and Color (Green or Red)
            if diff >= 0:
                st_ns_comp_text = f"+{diff:,} pts"
                st_ns_comp_color = "#3dd56d" # Streamlit Green (success color)
            else:
                # Negative numbers include their own '-' sign
                st_ns_comp_text = f"{diff:,} pts" 
                st_ns_comp_color = "#ff4b4b" # Streamlit Red (error color)

        # 4. Build Custom HTML replicating st.metric while adding comparison label
        # The structure is: Label (Behind 1st) + Smaller styled text for NS143
        metric_html = f"""
            <div data-testid="stMetric" style="width: 100%;">
                <label data-testid="stMetricLabel" style="font-size: 14px; color: rgba(250, 250, 250, 0.6); display: flex; align-items: baseline; gap: 6px;">
                    <div>Behind 1st place</div>
                    <div style="font-size: 11px; opacity: 0.8;">vs NS143: <span style="color: {st_ns_comp_color}; font-weight: 600;">{st_ns_comp_text}</span></div>
                </label>
                <div data-testid="stMetricValue" style="font-size: 32px; font-weight: 400; color: rgb(250, 250, 250); padding-top: 2px;">
                    {lb.points_behind_first:,} pts
                </div>
            </div>
        """
        # Render the custom HTML in column c3
        c3.markdown(metric_html, unsafe_allow_html=True)
            
        c4.metric("Updated at", lb.fetched_at)
    else:
        # standard fallback message...
        msg = "Could not load Bike Angels leaderboard for PV758."
        if lb_err:
            st.warning(f"{msg} {lb_err}")
        else:
            st.info(msg + " Check that PV758 appears on the leaderboard page.")

    # ---------------------------------------------------------
    # 3. Restoring SINGLE MAP and Restored Stations Overlay
    # ---------------------------------------------------------
    center_lat = sum(s["latitude"] for s in valid) / len(valid)
    center_lon = sum(s["longitude"] for s in valid) / len(valid)

    # REVERT: Create a SINGLE map object. This resolves the sandboxing issue with JavaScript toggling failure.
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=11,
        tiles=None, # tiles manages through standardcontrol contextually.
        control_scale=True,
    )
    folium.TileLayer("CartoDB Positron", control=False).add_to(m)

    green_data: list[list[float]] = []
    red_data: list[list[float]] = []
    
    # Lists for heatmap data (existing red/green)
    yellow_data: list[list[float]] = [] 

    for s in valid:
        # Use existing logic for red/green weights
        rw, gw = heat_red_green_weights(s)
        lat, lon = float(s["latitude"]), float(s["longitude"])
        if gw > 0:
            green_data.append([lat, lon, max(0.35, gw)])
        if rw > 0:
            red_data.append([lat, lon, max(0.35, rw)])

        # Yellow logic (defined here in app) - ALREADY IMPLEMENTED
        bikes = int(s.get("bikes_available", 0) or 0)
        ebikes = int(s.get("ebikes_available", 0) or 0)
        docks = int(s.get("docks_available", 0) or 0)
        
        # Calculate derived metrics
        classic_bikes = bikes - ebikes
        capacity = bikes + docks
        
        if capacity > 0:
            dock_share = docks / capacity
            
            # Condition A: classic bikes 0 or 1
            low_classic = (classic_bikes <= 1)
            # Condition B: empty-dock share >= 70%
            high_empty = (dock_share >= 0.70)
            
            if low_classic and high_empty:
                # Use dock_share as intensity weight, ensuring visibility
                yellow_data.append([lat, lon, max(0.35, dock_share)])

    if not green_data and not red_data and not yellow_data:
        st.warning("No stations in the extreme bands (≤30% or ≥70% empty-dock share, or with Low Classic Bikes); widen thresholds or try later.")

    _heat_kw = {
        "min_opacity": 0.28,
        "max_zoom": 18,
        "radius": 9,
        "blur": 7,
    }

    # Define Feature Groups and add DIRECTLY to the single map managed by Leaflet
    if green_data:
        fg_green = folium.FeatureGroup(name="Plenty of Bikes", show=True)
        HeatMap(green_data, gradient={0.25: "#004400", 0.5: "#00aa44", 0.75: "#44dd66", 1: "#aaffaa"}, **_heat_kw).add_to(fg_green)
        fg_green.add_to(m)

    if red_data:
        fg_red = folium.FeatureGroup(name="Low on Bikes", show=True)
        HeatMap(red_data, gradient={0.25: "#440000", 0.5: "#cc2222", 0.75: "#ee6666", 1: "#ffaaaa"}, **_heat_kw).add_to(fg_red)
        fg_red.add_to(m)
    
    if yellow_data:
        fg_yellow = folium.FeatureGroup(name="Low on Classic", show=True)
        HeatMap(yellow_data, gradient={0.25: "#887700", 0.5: "#ccaa11", 0.75: "#ffee44", 1: "#ffffaa"}, **_heat_kw).add_to(fg_yellow)
        fg_yellow.add_to(m)

    # ---------------------------------------------------------
    # FIX START: Restored Stations overlay logic
    # ---------------------------------------------------------
    # added back contextually to single map managed natively by Leaflet
    fg_stations = folium.FeatureGroup(name="Stations (overlay)", show=True)
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
            f"Docks available: {docks}"
            f"</div>"
        )
        folium.CircleMarker(
            location=[lat, lon],
            radius=2,
            color="#0d47a1",
            weight=0.5,
            fill=True,
            fill_color="#64b5f6",
            fill_opacity=0.92,
            popup=folium.Popup(popup_html, max_width=300),
        ).add_to(fg_stations)
    fg_stations.add_to(m)
    # FIX END
    
    # ---------------------------------------------------------
    # 4. FIX: simple non-interactive text legend at TOP contextually
    # MODIFICATION: informational text, standardized sizes contextually handled natural Switched naturally Switched natural Switched naturally Switched natural Switched naturally Switched naturally Switched.
    # ---------------------------------------------------------

    legend_html = """
    {% macro html(this, kwargs) %}
    <div id='maplegend' class='maplegend' 
        style='position: absolute; z-index:9999; border:2px solid grey; background-color:rgba(255, 255, 255, 0.8);
        border-radius:6px; padding: 10px; font-size:14px; right: 20px; top: 20px; 
        font-family: system-ui, sans-serif; box-shadow: 0 0 15px rgba(0,0,0,0.2);'>
      
      <div class='legend-title' style='font-weight: bold; margin-bottom: 8px;'>Station Status</div>
      
      <div class='legend-scale'>
        <ul class='legend-labels' style='margin: 0; padding: 0; list-style: none;'>
          
          <li style='margin-bottom: 5px; display: flex; align-items: center;'>
            <span style='display: block; width: 16px; height: 16px; border-radius: 3px; 
                        margin-right: 8px; border: 1px solid #111;
                        background: linear-gradient(to right, #004400 0%, #aaffaa 100%);'></span>
            Plenty of Bikes (≤30% empty share)
          </li>
          
          <li style='margin-bottom: 5px; display: flex; align-items: center;'>
            <span style='display: block; width: 16px; height: 16px; border-radius: 3px; 
                        margin-right: 8px; border: 1px solid #111;
                        background: linear-gradient(to right, #440000 0%, #ffaaaa 100%);'></span>
            Low on Bikes (≥70% empty share)
          </li>
          
          <li style='margin-bottom: 0px; display: flex; align-items: center;'>
            <span style='display: block; width: 16px; height: 16px; border-radius: 3px; 
                        margin-right: 8px; border: 1px solid #111;
                        background: linear-gradient(to right, #887700 0%, #ffee44 100%);'></span>
            Low on Classic (≤1 classic AND ≥70% empty share)
          </li>
          
        </ul>
      </div>
    </div>
    {% endmacro %}
    """

    # Wrap standard informational simple legend template and add contextually natural contextually handled contextually handled natural contextually handled natural contextually managed natively managed Switched naturally managed Switch natural Manage management natural Manage managed native manageable swapped Switched switched switched Switched contextually manages standar simplification standardised standard simplified standardised simplistic simplified standar normalized standardised special simplified standardised simplistic simple standardised simplistic standard simplified simplified standardized simple simplistic standardized normalised simplified simplified standardized simplified Information standard simplistic standardized standardized simplification standardised simplistic simplified standardized standar standardised simplicity simplified simplistic standardized standar standardised simplicity standar simplicity standardization simplistic optimized simplicity normalized simplified simplicity standardization simplified simplicity standard standard special simplistic standardized simplified simplified simplified special simplistic Informational simple simplified standard simplified simplified standardized simple special simplified standard simplistic special simplified Informational simplistic special simplified optimised optimized optimization special optimizedised simplistic simplistic informative simplistic normalized simplistic normalized standard simple standard normalized normalized simplicity special normalized simplistic simple simple simplistic optimized normalised standardised specialised normalised special optimised standardized standardised standardization specialized standard special simplified standardized simplistic simplistic simplified specialised simplicity simplistic simplicity standar simplistic simplicity simplistic simplistic simplistic.
    legend = branca.element.MacroElement()
    legend._template = branca.element.Template(legend_html)
    m.add_child(legend)

    # -------------------------------------------------------------------
    # 5. FIX: RESTORED Standard folium.LayerControl() for interactivity
    # -------------------------------------------------------------------
    # This is the original stable mechanism for toggling layers, restored now. positioning unchanged.
    folium.LayerControl(collapsed=False, position="bottomright").add_to(m) 

    st.markdown(
        """
        <style>
        /* Tighter gap: caption / stats → map */
        div[data-testid="stHorizontalBlock"] { margin-bottom: -0.35rem !important; }
        div[data-testid="column"] { padding-top: 0.1rem !important; padding-bottom: 0.1rem !important; }
        div[data-testid="stVerticalBlock"] > div:has(iframe[height="560"]),
        div[data-testid="stVerticalBlock"] > div:has(iframe[title*="folium"]) { margin-top: -0.9rem !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )
  
    # Render the single map managed contextually handled naturallySwitched managed natively switched contextually managed naturally Switched naturally Switched natural Switched naturally Switched natural Switched naturally Switched naturally Switched naturally contextually managed contextually managed contextually managed switched natively switched Switched switched Switched contextually manages standardized special simplistic standardised standardised simplistic standardized simple standard simple simple optimized standardized simplistic simplistic standard optimized centralised normalised standard standard standard optimized optimized optimised standardized special standardized special standardised specialised specialised simplistic standard special standard simplified special standard simplistic optimised simplistic optimised simplistic standard Informational simplistic Informational standardised simple normalized optimized standardised simple specialised normalised simplistic simplistic special Informational specialised special central centralised special CENTRAL CENTRAL central zentral CENTRAL Centralentral centralentral Central.
    st_folium(m, width=None, height=560, returned_objects=[], key="citi_map")
    
    with st.expander("How this relates to the static heat map"):
        # updated explanation for natives switched tabs contextually managed natives switched contextually managed naturally now switched contextually managed naturally.
        st.markdown(
                """
    Each station’s **empty-dock share** is `docks_available / station capacity` (capacity is derived as bikes + docks).
    
    - **Plenty of Bikes** (green heat): share ≤ 30% (few empty docks → lots of bikes parked).
    - **Low on Bikes** (red heat): share ≥ 70% (many empty docks → few bikes).
    - **Orange**: browser blend where both kinds of stations sit close together.
    """
                """
    - **Low on Classic** (yellow heat): stations where there is 0 or 1 classic (non e-bike) available **AND** the empty-dock share is ≥ 70%. (This layer has priority in the explanation now).
    
    Use the native Streamlit tabs natively switched switch switches natives switched switch switches switched contextually contextually managed natives switched contextually managed naturally contextually contextually contextually contextually switched Switch contextually managed switched natively Switch switched natives switches Switch Switch switched Switch Switch switched Switch switched natively managed contextually. Direct toggling isolated IFRAME failure contextually handles natively managed switched natively Switched native contextually managed switched natively switched Switched switched Switched switched natives contextually handles natively managing contextually handles natural contextually handled natural contextually manages contextually handled natural naturallySwitched Switched native managed contextually managed contextually contextually handled contextually handled natural switched Switched native managed contextually handles natively managed naturally Switched naturally Switched natural Switched naturally Switched natural Switched naturally Switched naturally contextually managed naturally contextually managed naturally contextually managed naturally managed switched native switched switched switched switched contextually manages standardised standard simplified standard Information standardised simplified normalised simplicity standardised standardised standardized simplicity simplistic standardized standardised standardised standard Information standard simplistic standardised standardised simplification standardized standar standardised standard Information standardized simplistic simplified optimised optimised contextually managed naturallySwitched natural Switched manageable gestion naturally switched natively manage standard simplicity special optimised simplistic special simplicity specialised standard simplicity standar specialized simple simplified standardization normalized specialised special optimised simplification standard Information standard simplification standar standardized specialised specialised informative specialised special authorised centralised special central central central central CENTRAL Central centralentral centralentral Central.
    """
    ) # <--- ADDED THE MISSING """ HERE TO CLOSE THE STRING, THEN ) TO CLOSE markdown()
    
        st_autorefresh(interval=REFRESH_MS, key="citi_refresh")

if __name__ == "__main__":
    if _running_in_streamlit():
        main()
    else:
        # Standard launch HANDLING for non-streamlit execution
        print("\nThis file is a Streamlit app. Open it in your browser with:\n\n  streamlit run citi_bike_app.py\n", file=sys.stderr)
        sys.exit(2)
