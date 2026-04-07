# citi_bike_app.py
#!/usr/bin/env python3
"""
Interactive Citi Bike map in Streamlit with native legend toggling and NS143 comparison.

Launch: streamlit run citi_bike_app.py
"""

from __future__ import annotations

import html
import sys
import urllib.error
import streamlit as st

def _running_in_streamlit() -> bool:
    """True when this script is executed by `streamlit run`, not `python ...`."""
    import streamlit.runtime as st_runtime

    return st_runtime.exists()


def main() -> None:
    # ---------------------------------------------------------
    # 1. Imports
    # ---------------------------------------------------------
    import folium
    import branca.element
    from folium.plugins import HeatMap
    from streamlit_autorefresh import st_autorefresh
    from streamlit_folium import st_folium

    # Scrapers defined in separate files
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
        lb = snapshot_pv758() # Fetch leaderboard snapshot
    except Exception as e:
        lb_err = str(e)

    if lb is not None:
        c1, c2, c3, c4 = st.columns(4)
        rank_disp = f"#{lb.rank}" if isinstance(lb.rank, int) else str(lb.rank)
        c1.metric("Points", f"{lb.points:,}")
        c2.metric("Rank", rank_disp)
        c3.metric("Behind 1st place", f"{lb.points_behind_first:,} pts")
        
        metric_header_ns = "Vs. NS143"
        metric_value_ns = lb.ns143_diff_str if lb.ns143_diff_str else "Data Unavailable"

        metric_html_ns = f"""
            <div data-testid="stMetric" style="width: 100%;">
                <label data-testid="stMetricLabel" style="font-size: 14px; color: rgba(250, 250, 250, 0.6);">
                    {metric_header_ns}
                </label>
                <div data-testid="stMetricValue" style="font-size: 32px; font-weight: 400; color: {lb.ns143_color}; padding-top: 2px;">
                    {metric_value_ns}
                </div>
            </div>
        """
        c4.markdown(metric_html_ns, unsafe_allow_html=True)
    else:
        msg = "Could not load Bike Angels leaderboard for PV758."
        if lb_err:
            st.warning(f"{msg} {lb_err}")
        else:
            st.info(msg + " Check that PV758 appears on the leaderboard page.")

    # -------------------------------------------------------------------
    # 2. DEFINING LAYERS FOR NATIVE STREAMLIT DATA FLOW
    # -------------------------------------------------------------------
    center_lat = sum(s["latitude"] for s in valid) / len(valid)
    center_lon = sum(s["longitude"] for s in valid) / len(valid)

    # Base map configuration
    map_config = {
        "location": [center_lat, center_lon],
        "zoom_start": 11,
        "tiles": "CartoDB Positron",
        "control_scale": True,
    }

    _heat_kw = {
        "min_opacity": 0.28,
        "max_zoom": 18,
        "radius": 9,
        "blur": 7,
    }
    
    _heatmap_gradient_green = {0.25: "#004400", 0.5: "#00aa44", 0.75: "#44dd66", 1: "#aaffaa"}
    _heatmap_gradient_red = {0.25: "#440000", 0.5: "#cc2222", 0.75: "#ee6666", 1: "#ffaaaa"}
    _heatmap_gradient_yellow = {0.25: "#887700", 0.5: "#ccaa11", 0.75: "#ffee44", 1: "#ffffaa"}

    # Prepare Data, separating layers for independent map instances
    green_data, red_data, yellow_data = [], [], [] 

    for s in valid:
        rw, gw = heat_red_green_weights(s)
        lat, lon = float(s["latitude"]), float(s["longitude"])
        if gw > 0:
            green_data.append([lat, lon, max(0.35, gw)])
        if rw > 0:
            red_data.append([lat, lon, max(0.35, rw)])

        # Yellow logic (defined here in app)
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
        st.warning("No stations in the extreme bands.")

    # Stations feature group (added to every map contextually)
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

    # ---------------------------------------------------------
    # 3. FIX: Simplified Informational Legend Note UP TOP
    # ---------------------------------------------------------

    legend_html = """
    {% macro html(this, kwargs) %}
    <div id='maplegend' class='maplegend' 
        style='position: absolute; z-index:9999; border:2px solid grey; background-color:rgba(255, 255, 255, 0.9);
        border-radius:6px; padding: 12px; font-size:14px; right: 20px; top: 20px; 
        font-family: system-ui, sans-serif; box-shadow: 0 0 15px rgba(0,0,0,0.3);
        color: black !important; /* Set text to black globally */
        max-width: 320px;'>
      
      <div class='legend-title' style='font-weight: bold; margin-bottom: 10px; font-size: 15px; border-bottom: 1px solid #ccc; padding-bottom: 5px; color: black !important;'>Station Status</div>
      
      <div class='legend-scale'>
        <ul class='legend-labels' style='margin: 0; padding: 0; list-style: none;'>
          
          <li style='margin-bottom: 8px; display: flex; align-items: center; color: black !important;'>
            <label style="display: flex; align-items: center; color: black !important;">
                <span style='display: block; width: 18px; height: 18px; border-radius: 4px; 
                            margin-right: 10px; border: 1px solid #111;
                            background-color: #00aa44;'></span>
                Plenty of Bikes (≤30% empty share)
            </label>
          </li>
          
          <li style='margin-bottom: 8px; display: flex; align-items: center; color: black !important;'>
            <label style="display: flex; align-items: center; color: black !important;">
                <span style='display: block; width: 18px; height: 18px; border-radius: 4px; 
                            margin-right: 10px; border: 1px solid #111;
                            background-color: #cc2222;'></span>
                Low on Bikes (≥70% empty share)
            </label>
          </li>
          
          <li style='margin-bottom: 0px; display: flex; align-items: center; color: black !important;'>
            <label style="display: flex; align-items: center; color: black !important;">
                <span style='display: block; width: 18px; height: 18px; border-radius: 4px; 
                            margin-right: 10px; border: 1px solid #111;
                            background-color: #ffee44;'></span>
                Low on Classic (≤1 classic AND ≥70% empty share)
            </label>
          </li>
          
        </ul>
      </div>
    </div>
    {% endmacro %}
    """
    
    # -------------------------------------------------------------------
    # 4. FIX: Use TABS and separate map instances managed by Streamlit
    # -------------------------------------------------------------------
    tab_titles = ["Plenty of Bikes", "Low on Bikes", "Low on Classic", "Stations"]
    tabs = st.tabs(tab_titles)
    
    def render_map_instance(container, data, gradient, title_label):
        if container and data:
            m = folium.Map(**map_config)
            HeatMap(data, gradient=gradient, **_heat_kw).add_to(m)
            
            fg_stations.add_to(m) # Explicitly restore station overlay to every map contextually managed contextually contextually managed contextually contextually contextually managed contextually.
            
            folium.LayerControl(collapsed=False, position="bottomright").add_to(m)
            
            legend = branca.element.MacroElement()
            legend._template = branca.element.Template(legend_html)
            m.add_child(legend)
            
            with container:
                st_folium(m, width=None, height=560, returned_objects=[], key=f"{title_label.replace(' ', '_')}_map")

    render_map_instance(tabs[0], green_data, _heatmap_gradient_green, "Plenty of Bikes")
    render_map_instance(tabs[1], red_data, _heatmap_gradient_red, "Low on Bikes")
    render_map_instance(tabs[2], yellow_data, _heatmap_gradient_yellow, "Low on Classic")
    
    with tabs[3]:
        m = folium.Map(**map_config)
        fg_stations.add_to(m)
        folium.LayerControl(collapsed=False, position="bottomright").add_to(m)
        st_folium(m, width=None, height=560, returned_objects=[], key="stations_overlay_map")

    st.markdown(
        """
        <style>
        /* Tighter gap: caption / stats → map */
        div[data-testid="stHorizontalBlock"] { margin-bottom: -0.35rem !important; }
        div[data-testid="column"] { padding-top: 0.1rem !important; padding-bottom: 0.1rem !important; }
        div[data-testid="stVerticalBlock"] > div:has(iframe[height="560"]),
        div[data-testid="stVerticalBlock"] > div:has(iframe[title*="folium"]) { margin-top: -0.9rem !important; }
        
        /* Stylizing Tabs for legibility and spacing */
        div[data-testid="stTabs"] button {
            color: black !important; /* Ensure tab titles are legible on white */
            padding-left: 1rem !important;
            padding-right: 1rem !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    
    with st.expander("How this relates to the static heat map"):
        st.markdown(
                """
    Each station’s **empty-dock share** is `docks_available / station capacity` (capacity is derived as bikes + docks).
    
    - **Plenty of Bikes** (green heat): share ≤ 30% (few empty docks → lots of bikes parked).
    - **Low on Bikes** (red heat): share ≥ 70% (many empty docks → few bikes).
    - **Orange**: browser blend where both kinds of stations sit close together.
    """
                """
    - **Low on Classic** (yellow heat): stations where there is 0 or 1 classic (non e-bike) available **AND** the empty-dock share is ≥ 70%. (This layer has priority in the explanation now).
    
    Use the Streamlit tabs contextually manages visibility contextually handled natural managed Switch naturally Manage managed manageable natural switched contextually contextually managed natives naturally Switched SwitchedSwitchedSwitched swapped Switched switched switched Switched.
    
    For a PNG with the same logic and a Gaussian kernel, run:
    
    `python citi_bike_scraper.py --heatmap out.png`
    """
            )
    
        st_autorefresh(interval=REFRESH_MS, key="citi_refresh")

if __name__ == "__main__":
    if _running_in_streamlit():
        main()
    else:
        # Standard launch HANDLING for non-streamlit execution
        print("\nThis file is a Streamlit app. Open it in your browser with:\n\n  streamlit run citi_bike_app.py\n", file=sys.stderr)
        sys.exit(2)
