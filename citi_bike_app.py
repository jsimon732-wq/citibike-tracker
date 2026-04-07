# citi_bike_app.py
#!/usr/bin/env python3
"""
Interactive Citi Bike map with standard metrics layout and legend toggling fix.

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
import streamlit as st 

def _running_in_streamlit() -> bool:
    """True when this script is executed by `streamlit run`, not `python ...`."""
    import streamlit.runtime as st_runtime

    return st_runtime.exists()


def main() -> None:
    # ---------------------------------------------------------
    # Imports
    # ---------------------------------------------------------
    import folium
    import branca.element # Required for MacroElement used in legend toggling fix
    from folium.plugins import HeatMap
    from streamlit_autorefresh import st_autorefresh
    from streamlit_folium import st_folium

    # Assuming heat_red_green_weights is still needed, though logic is here.
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
        c3.metric("Behind 1st place", f"{lb.points_behind_first:,} pts")
        
        # ---------------------------------------------------------
        # 1. NEW LAYOUT: Render NS143 as a standard metric in c4
        # ---------------------------------------------------------
        # label-above-value layout matching Rank/Points
        st_ns_header = "Vs. NS143"
        
        # Fallback if points or message is missing
        st_ns_value = lb.ns143_diff_str if lb.ns143_diff_str else "Data Unavailable"

        # Build standard metric HTML
        metric_html_ns = f"""
            <div data-testid="stMetric" style="width: 100%;">
                <label data-testid="stMetricLabel" style="font-size: 14px; color: rgba(250, 250, 250, 0.6);">
                    {st_ns_header}
                </label>
                <div data-testid="stMetricValue" style="font-size: 32px; font-weight: 400; color: {lb.ns143_color}; padding-top: 2px;">
                    {st_ns_value}
                </div>
            </div>
        """
        # Render standard metric for NS143 comparison in column c4
        c4.markdown(metric_html_ns, unsafe_allow_html=True)

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

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=11,
        tiles=None,
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

        # ---------------------------------------------------------
        # Yellow logic (defined here in app) - ALREADY IMPLEMENTED
        # ---------------------------------------------------------
        
        # We need values that might not be in 's' for heat_red_green_weights
        # but are used in the station markers.
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

    _heat_kw = {
        "min_opacity": 0.28,
        "max_zoom": 18,
        "radius": 9,
        "blur": 7,
    }

    # Define Feature Groups but don't add to map immediately (handle via standard control and JS click)
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

    if not green_data and not red_data and not yellow_data:
        st.warning("No stations in the extreme bands (≤30% or ≥70% empty-dock share, or with Low Classic Bikes); widen thresholds or try later.")

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
    fg_stations.add_to(m)

    # ---------------------------------------------------------
    # 2. FIX: HTML Legend and JavaScript for Legend Toggling
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
            <input type="checkbox" id="plentyBikesToggler" checked style="margin-right: 8px; cursor: pointer;">
            <label for="plentyBikesToggler" style="display: flex; align-items: center; cursor: pointer; color: black !important;">
                <span style='display: block; width: 18px; height: 18px; border-radius: 4px; 
                            margin-right: 10px; border: 1px solid #111;
                            background-color: #00aa44; /* Solid simple green */'></span>
                Plenty of Bikes (≤30% empty share)
            </label>
          </li>
          
          <li style='margin-bottom: 8px; display: flex; align-items: center; color: black !important;'>
            <input type="checkbox" id="lowBikesToggler" checked style="margin-right: 8px; cursor: pointer;">
            <label for="lowBikesToggler" style="display: flex; align-items: center; cursor: pointer; color: black !important;">
                <span style='display: block; width: 18px; height: 18px; border-radius: 4px; 
                            margin-right: 10px; border: 1px solid #111;
                            background-color: #cc2222; /* Solid simple red */'></span>
                Low on Bikes (≥70% empty share)
            </label>
          </li>
          
          <li style='margin-bottom: 0px; display: flex; align-items: center; color: black !important;'>
            <input type="checkbox" id="lowClassicToggler" checked style="margin-right: 8px; cursor: pointer;">
            <label for="lowClassicToggler" style="display: flex; align-items: center; cursor: pointer; color: black !important;">
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
      // MODIFICATION: JavaScript for Toggling Layers by Clicking Hidden Standard Control
      // -------------------------------------------------------------------
      
      // Leaflet automatically references the map object with this pattern:
      // map_{{this.get_name()}} (Branca renders {{this.get_name()}} dynamically)
      var leafletMap = map_{{this.get_name()}};

      // Function to programmatically click Leaflet checkboxes
      function clickLeafletCheckbox(layerName) {
        // Leaflet references objects stored in its internal list.
        leafletMap.eachLayer(function(layer) {
          // Feature groups created by Folium have an internal option property named 'control' set to True
          // However, Branca/Folium creates separate JS objects for feature groups, not easily accessible here.
          // Leaflet doesn't expose the underlying feature groups easily in a standard map.
          // Instead, we interact with the LayerControl component itself.
        });
        
        // INTERACTING WITH HIDDEN LAYER CONTROL:
        // Folium/Leaflet render the drop-down. We can find the DOM elements for its inputs.
        // Leaflet wraps its inputs in unique divs within the control container.
        // Standard ID structure isn't reliable, but name matching within labels works.

        // Standard Leaflet structure:
        // <form class="leaflet-control-layers-list">
        //   <div class="leaflet-control-layers-overlays">
        //     <label>
        //       <input type="checkbox" class="leaflet-control-layers-selector">
        //       <span>Plenty of Bikes</span>
        //     </label>
        //     ...
        //   </div>
        // </form>

        // Find the LayerControl container element
        var controlContainer = document.querySelector('.leaflet-control-layers-overlays');
        if (!controlContainer) return;

        // Find all labels within it
        var labels = controlContainer.querySelectorAll('label');
        labels.forEach(function(label) {
            // Check if the label's inner text matches our layer name
            if (label.innerText.trim() === layerName) {
                // Find the checkbox within this label
                var checkbox = label.querySelector('input[type="checkbox"]');
                if (checkbox) {
                    // Programmatically click it. Leaflet listens for 'click' on the label/input.
                    checkbox.click(); 
                }
            }
        });
      }

      // 2. Add Event Listeners for checkboxes in our custom legend
      document.getElementById('plentyBikesToggler').addEventListener('change', function() {
        clickLeafletCheckbox('Plenty of Bikes');
      });
      document.getElementById('lowBikesToggler').addEventListener('change', function() {
        clickLeafletCheckbox('Low on Bikes');
      });
      document.getElementById('lowClassicToggler').addEventListener('change', function() {
        clickLeafletCheckbox('Low on Classic');
      });
      
    </script>
    
    {% endmacro %}
    """

    # Wrap the HTML in a template and add to map
    legend = branca.element.MacroElement()
    legend._template = branca.element.Template(legend_html)
    m.add_child(legend)

    # NEW: Collapsed standard control (positioning doesn't matter, we hide it with CSS)
    # We MUST include this so Leaflet manages the layer states for us.
    folium.LayerControl(collapsed=True, position="topright").add_to(m) 

    st.markdown(
        """
        <style>
        /* Tighter gap: caption / stats → map */
        div[data-testid="stHorizontalBlock"] { margin-bottom: -0.35rem !important; }
        div[data-testid="column"] { padding-top: 0.1rem !important; padding-bottom: 0.1rem !important; }
        div[data-testid="stVerticalBlock"] > div:has(iframe[height="560"]),
        div[data-testid="stVerticalBlock"] > div:has(iframe[title*="folium"]) { margin-top: -0.9rem !important; }
        
        /* --------------------------------------------------------- */
        /* MODIFICATION: HIDE the standard Leaflet Layer Control Drop-down */
        /* --------------------------------------------------------- */
        .leaflet-control-layers.leaflet-control { display: none !important; }
        
        </style>
        """,
        unsafe_allow_html=True,
    )
  
    st_folium(m, width=None, height=560, returned_objects=[], key="citi_map")
    
    with st.expander("How this relates to the static heat map"):
        # Rel relates explanation...
        st.markdown(
                """
    Each station’s **empty-dock share** is `docks_available / station capacity` (capacity is derived as bikes + docks).
    
    - **Plenty of Bikes** (green heat): share ≤ 30% (few empty docks → lots of bikes parked).
    - **Low on Bikes** (red heat): share ≥ 70% (many empty docks → few bikes).
    - **Orange**: browser blend where both kinds of stations sit close together.
    """
                """
    - **Low on Classic** (yellow heat): stations where there is 0 or 1 classic (non e-bike) available **AND** the empty-dock share is ≥ 70%. (This layer has priority in the explanation now).
    
    Toggle layers directly on the map legend. For a PNG with the same logic and a Gaussian kernel, run:
    
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
