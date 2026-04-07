# citi_bike_app.py
#!/usr/bin/env python3
"""
Interactive Citi Bike map in Streamlit with native legend toggling and NS143 comparison.

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
import streamlit as st # FIX ALREADY IMPLEMENTED

def _running_in_streamlit() -> bool:
    """True when this script is executed by `streamlit run`, not `python ...`."""
    import streamlit.runtime as st_runtime

    return st_runtime.exists()


def main() -> None:
    # ---------------------------------------------------------
    # 1. Imports
    # ---------------------------------------------------------
    import folium
    import branca.element # Used for standardizing swatch sizes
    from folium.plugins import HeatMap
    from streamlit_autorefresh import st_autorefresh
    from streamlit_folium import st_folium

    # Assuming heat_red_green_weights is needed, though logic is defined here.
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
        # standard 4 metrics layout
        c1, c2, c3, c4 = st.columns(4)
        rank_disp = f"#{lb.rank}" if isinstance(lb.rank, int) else str(lb.rank)
        c1.metric("Points", f"{lb.points:,}")
        c2.metric("Rank", rank_disp)
        c3.metric("Behind 1st place", f"{lb.points_behind_first:,} pts")
        
        # ---------------------------------------------------------
        # 2. NEW LAYOUT: Render NS143 as a standard metric in c4 (ALREADY IMPLEMENTED)
        # ---------------------------------------------------------
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
        #c4.metric("Updated at", lb.fetched_at)
    else:
        # standard fallback message...
        msg = "Could not load Bike Angels leaderboard for PV758."
        if lb_err:
            st.warning(f"{msg} {lb_err}")
        else:
            st.info(msg + " Check that PV758 appears on the leaderboard page.")

    # -------------------------------------------------------------------
    # 3. FIX: DEFINING LAYERS FOR NATIVE STREAMLIT TOGGLING
    # -------------------------------------------------------------------
    center_lat = sum(s["latitude"] for s in valid) / len(valid)
    center_lon = sum(s["longitude"] for s in valid) / len(valid)

    _heat_kw = {
        "min_opacity": 0.28,
        "max_zoom": 18,
        "radius": 9,
        "blur": 7,
    }
    
    _heatmap_gradient_green = {0.25: "#004400", 0.5: "#00aa44", 0.75: "#44dd66", 1: "#aaffaa"}
    _heatmap_gradient_red = {0.25: "#440000", 0.5: "#cc2222", 0.75: "#ee6666", 1: "#ffaaaa"}
    _heatmap_gradient_yellow = {0.25: "#887700", 0.5: "#ccaa11", 0.75: "#ffee44", 1: "#ffffaa"}

    # Base map configuration
    map_config = {
        "location": [center_lat, center_lon],
        "zoom_start": 11,
        "tiles": "CartoDB Positron",
        "control_scale": True,
    }

    # Prepare Data but DO NOT ADD to a single map object yet.
    green_data = []
    red_data = []
    yellow_data = [] 

    for s in valid:
        # Use existing logic for red/green weights
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
        st.warning("No stations in the extreme bands (≤30% or ≥70% empty-dock share, or with Low Classic Bikes); widen thresholds or try later.")

    # Stations feature group (managed natively by Streamlit)
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
    # 4. FIX: HTML Legend definition
    # MODIFICATION: Toggling interacting with hidden standard control
    # ---------------------------------------------------------

    legend_html = """
    {% macro html(this, kwargs) %}
    <div id='maplegend' class='maplegend' 
        style='position: absolute; z-index:9999; border:2px solid grey; background-color:rgba(255, 255, 255, 0.9);
        border-radius:6px; padding: 12px; font-size:14px; right: 20px; bottom: 50px; 
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
                            background-color: #00aa44; /* Solid simple green */'></span>
                Plenty of Bikes (≤30% empty share)
            </label>
          </li>
          
          <li style='margin-bottom: 8px; display: flex; align-items: center; color: black !important;'>
            <label style="display: flex; align-items: center; color: black !important;">
                <span style='display: block; width: 18px; height: 18px; border-radius: 4px; 
                            margin-right: 10px; border: 1px solid #111;
                            background-color: #cc2222; /* Solid simple red */'></span>
                Low on Bikes (≥70% empty share)
            </label>
          </li>
          
          <li style='margin-bottom: 0px; display: flex; align-items: center; color: black !important;'>
            <label style="display: flex; align-items: center; color: black !important;">
                <span style='display: block; width: 18px; height: 18px; border-radius: 4px; 
                            margin-right: 10px; border: 1px solid #111;
                            background-color: #ffee44; /* Solid simple yellow */'></span>
                Low on Classic (≤1 classic AND ≥70% empty share)
            </label>
          </li>
          
        </ul>
      </div>
    </div>
    
    <script>
      // -------------------------------------------------------------------
      // MODIFICATION: NO JAVASCRIPT NEEDED FOR TOGGLING ANYMORE. 
      //Streamlit cloud handles native Streamlit checkboxes. 
      //Interacting with the hidden drop-down control failed due to 
      //Streamlit isolatling Folium in an IFRAME and JavaScript sandbox.
      // -------------------------------------------------------------------
    </script>
    
    {% endmacro %}
    """
    
    # -------------------------------------------------------------------
    # 5. FIX: Restructuring main() to render NATIVE TABS and Map instances
    # -------------------------------------------------------------------
    
    # NEW LAYOUT: Use Tabs as layout containers for maps, providing implicit separation
    tab_titles = ["Plenty of Bikes", "Low on Bikes", "Low on Classic", "Stations"]
    tabs = st.tabs(tab_titles)
    
    # Define a generic function to render a separate map instance for a given data/gradient.
    # Leaflet cannot add multiple HeatMap layers cleanly to a single map object managed by Streamlit.
    def render_separate_heatmap_map(container, data, gradient, title):
        if container and data:
            m = folium.Map(**map_config)
            HeatMap(data, gradient=gradient, **_heat_kw).add_to(m)
            # Add Stations overlay to every heatmap map for context
            fg_stations.add_to(m)
            
            # Add standard collapsable LayerControl so users can still toggleStations overlay
            folium.LayerControl(collapsed=True, position="topright").add_to(m)
            
            # Wrap the HTML legend in a template and add to every map instance
            legend = branca.element.MacroElement()
            legend._template = branca.element.Template(legend_html)
            m.add_child(legend)
            
            # Streamlit is manages the visibility of these instances natively now via tabs.
            # Storing width/height settings consistent with desire for simple, legible map view.
            with container:
                st_folium(m, width=None, height=560, returned_objects=[], key=f"{title.replace(' ', '_')}_map")

    # Render each heatmap layer on its own map instance within separate tabs managed by Streamlit's backend.
    render_separate_heatmap_map(tabs[0], green_data, _heatmap_gradient_green, "Plenty of Bikes")
    render_separate_heatmap_map(tabs[1], red_data, _heatmap_gradient_red, "Low on Bikes")
    render_separate_heatmap_map(tabs[2], yellow_data, _heatmap_gradient_yellow, "Low on Classic")
    
    # Stations layer on its own separate map within the final tab, always shown context.
    with tabs[3]:
        m = folium.Map(**map_config)
        fg_stations.add_to(m)
        folium.LayerControl(collapsed=True, position="topright").add_to(m)
        st_folium(m, width=None, height=560, returned_objects=[], key="stations_only_map")

    # The standard Folium LayerControl dropdown is redundant and confusing now, so we don't need to explicitlyhide it
    # as the new restructured layout is natively managing visibility. Users can use LayerControl *within* tabs to contextually
    # toggle the stations overlay.

    st.markdown(
        """
        <style>
        /* Tighter gap: caption / stats → map */
        div[data-testid="stHorizontalBlock"] { margin-bottom: -0.35rem !important; }
        div[data-testid="column"] { padding-top: 0.1rem !important; padding-bottom: 0.1rem !important; }
        div[data-testid="stVerticalBlock"] > div:has(iframe[height="560"]),
        div[data-testid="stVerticalBlock"] > div:has(iframe[title*="folium"]) { margin-top: -0.9rem !important; }
        
        /* --------------------------------------------------------- */
        /* MODIFICATION: Stylizing Tabs for legibility and spacing */
        /* --------------------------------------------------------- */
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
        # Rel relates explanation... Explanation updated for new layout
        st.markdown(
                """
    Each station’s **empty-dock share** is `docks_available / station capacity` (capacity is derived as bikes + docks).
    
    - **Plenty of Bikes** (green heat): share ≤ 30% (few empty docks → lots of bikes parked).
    - **Low on Bikes** (red heat): share ≥ 70% (many empty docks → few bikes).
    - **Orange**: browser blend where both kinds of stations sit close together.
    """
                """
    - **Low on Classic** (yellow heat): stations where there is 0 or 1 classic (non e-bike) available **AND** the empty-dock share is ≥ 70%. (This layer has priority in the explanation now).
    
    Use the Streamlit tabs below the legend to natives switch between map layer instances managed by Streamlit's backend. This avoids limitations with direct Leaflet JavaScript interaction within the isolated Streamlit IFRAME sandbox.
    
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
