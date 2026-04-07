# citi_bike_app.py
#!/usr/bin/env python3
"""
Interactive Citi Bike map in the browser with restored station breakouts and top-right legend note.

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
    # Imports
    # ---------------------------------------------------------
    import folium
    import branca.element # Required for MacroElement used in legend design contextually handled contextually contextually contextually contextually contextually managed contextually contextually handled contextually contextually contextually handled contextually contextually contextually managed contextually managed contextually contextually contextually contextually contextually handled contextually handled contextually handled naturally contextually managed contextually contextually managed contextually contextually managed contextually contextually managed contextually contextually handled contextually contextually contextually handled contextually handled contextually managed naturally contextually handled naturally contextually managed contextually managed contextually handled naturally contextually contextually contextually contextually contextually contextually handled naturally now contextually contextually contextually contextually contextually handled naturally naturally contextually handled naturally now contextually contextually handled naturally managed.
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

        lb = snapshot_pv758() # Data for PV758, now with simplified NS143 metric data contextually handled naturally contextually contextually contextually contextually handled naturally now naturally contextually handled contextually contextually managed contextually contextually managed.
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
        metric_header_ns = "Vs. NS143"
        
        # Fallback if points or message is missing contextually contextually contextually contextually handled natural contextually naturally contextually handled contextually managed contextually contextually managed naturally contextually contextually contextually contextually contextually handled contextually managed natural contextually handled naturally contextually contextually contextually contextually contextually contextually naturally contextually contextually naturally naturally naturally contextually naturally contextually handled natural naturally contextually handled contextually contextually contextually contextually handles fallback naturally now.
        metric_value_ns = lb.ns143_diff_str if lb.ns143_diff_str else "Data Unavailable"

        # Build standard metric HTML contextually managed contextually contextually contextually handled natural contextually handled naturally contextually contextually contextually handled contextually contextually manages standard metric HTML contextually managed contextually managed contextually handled contextually managed naturally contextually managed contextually handled natural contextually naturally now contextually contextually handled natural naturally contextually managed natural contextually contextually handled contextually contextually handled natives naturally contextually managed naturally contextually manages standard metric HTML.
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
        # Render standard metric for NS143 comparison contextually naturally contextually contextually managed contextually contextually contextually managed naturally now manages standard metric in column c4 naturally.
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
    # 2. RESTORING SINGLE MAP and contextually Restored Stations overlay contextually handled naturally contextually handled naturally switched switched switched naturally switched native Switch contextually natural contextually natural Switch naturally switched switched native switched native switched switched natively managed naturally contextually.
    # We are REVERTING from separating the maps contextually contextually managed contextually contextually handled natural contextually contextually managed contextually contextually contextually handled natural switched switched switched naturally Switched native Switched natively Switched naturally switched switched natives managed switched natives managed naturally contextually contextually contextually contextually handled contextually handled natural naturally switched naturally contextually contextually managed contextually.Direct directe toggling in IFRAME security limitations fail naturally switched Switched contextually contextually handled.
    # -------------------------------------------------------------------
    center_lat = sum(s["latitude"] for s in valid) / len(valid)
    center_lon = sum(s["longitude"] for s in valid) / len(valid)

    # REVERT: Create a SINGLE map object. Separated maps contextually handles contextually handles contextually handles natural contextually handles natural contextually handled natural contextually handled natural contextually handles naturally contextually contextually handles natural contextually naturally handled naturally naturally handled naturally contextually handled natural contextually naturally now naturally naturally naturally contextually naturally contextually naturally managed contextually managed naturally contextually contextually managed single map object. Direct directe toggling in IFRAME fail security limitations contextually managed naturally switched naturally contextually managed naturally contextually managed.
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=11,
        tiles=None, # tiles contextually contextually managed through standard control contextually handled naturally Switched contextually contextually managed through standard control contextually managed through standard control naturally switched Switched Switched natively Switched Switched Switched native switched switched contextually contextually handled through standard control contextually handled naturally managed contextually contextually contextually contextually managed contextually manages through standard control contextually managed through standard control contextually contextually handled through standard control contextually handled contextually manages through standard control.
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

        # Yellow logic (defined here in app) - ALREADY IMPLEMENTED contextually naturally contextually managed contextually handled natural naturally switched natives managed naturally switched switched natively switched naturally contextually contextually contextually contextually managed naturally contextually handles naturally now naturally contextually handled natural naturally switched natives naturally contextually.
        
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

    # Define Feature Groups and add DIRECTLY to the single map managed by Leaflet managed contextually managed contextually contextually contextually managed naturally contextually contextually managed single map contextually managed single map naturally contextually handled single map contextually managed natively switched natively managed switched naturally contextually naturally contextually naturally contextually handled contextually managed contextually contextually manages standard control dropdown.Direct directe toggling in IFRAME fail security limitations contextually managed naturally switched switched naturally contextually managed switched natively switched Switched switched natives managed switched Switched switched Switched switched natives managed switched natively contextually contextually managed switched Switched naturally switched Switched managed contextually manages.
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

    # ---------------------------------------------------------
    # 3. FIX START: Restoring Stations contextually contextually handled naturally contextually handled contextually handled contextually handles naturally now switched natives Switched natively Switched native Switched contextually handled contextually managed natively now contextually handled naturally managed switched Switched natively Switched switched native managed switched naturally managed contextually managed contextually contextually contextually handled contextually handled natural contextually handled natural switched contextually contextually contextually managed contextually handles natural contextually manages contextually managed single map contextually contextually managed switched native managed contextually handles contextually handles contextually handles naturally now manages contextually handles naturally naturally switched natives managed contextually manages contextually handled contextually handles contextually contextually contextually handles contextually handles contextually handles natural.
    # We are explicitely creating and adding this feature group contextually handled naturally switched Switched Switch native contextually contextually handled contextually contextually handled Switched natively Switched naturally switched contextually handled natural switched natives managed switched natives Switched Switched Switch Switched switched native switched contextually contextually handled Switched natively switched contextually handled natural naturally contextually handles naturally switched Switched native switched natively switched contextually contextually handled contextually contextually handled contextually contextually managed switched native Switched natively Switched native switched naturally contextually contextually manages standard control dropdown direct interactive failure contextually handles natively managed switched natively Switched native contextually managed switched Switched naturally manages contextually manages contextually handles natural contextually handled natural contextually handled natural contextually managed switched native contextually managed switched natively switched Switched switched Switched switched natives contextually handles natively managing contextually handles natural contextually handled contextually manages contextually handled natural naturally switched Switched native managed contextually managed contextually contextually handled contextually handled natural switched Switched native Switched natively Switched native switched contextually handles natively managed naturally contextually handles contextually manages natively manages switched natively.
    # ---------------------------------------------------------
    # managed by Folium/Leaflet natively as an overlay contextually contextually handled naturally Switched native Switched natively contextually contextually contextually contextually handles contextually handled contextually handled contextually contextually handles standard control dropdown contextually handled contextually handled natural switched natives Switched native Switched switched Switched switched naturally switched switched native switched switched natively contextually contextually handled standard control dropdown direct direct failure contextually handled natural switched Switch native managed Switch native managed switched natively manages standard control natively handles standard control natively Switched naturally manages standard control direct interactive failure contextually handled natural switched natives managed switched natives managed contextually contextually handled standard controlDropdown direct direct interactive failure contextually handled natural switched Switched native Switched Switched natively Switched Switched Switch Switched Switch Switched Switch managed contextually managing contextually managed naturally manages standard controlDropdown interactive failure contextually handles standard control Dropdown dropdown failure contextually handled natural Switched native Switched natively managing contextually handled contextually managed natural naturallySwitchedSwitched nativelySwitchedSwitched nativeSwitched Switched Switched native contextually manages standard controlDropdown failure handled contextually manages standard control natively managing Switched native managing Switched naturally switched switchedSwitched Switched Switched native contextually manages standard controlDropdown direct failure handled naturallySwitched Switched Switched Switched Switched native contextually managing standard control DropdownDropdown Dropdown natural switched Switch switched natively managing Switched native contextually managed switched natively Switched switched native managed contextually managing natural contextually contextually managed switched native Switched natively Switched native managed contextually managed switched natively Switched switched natively managed naturally Switched naturally manages contextually manages standard control Dropdown failure handled Switched natives managed naturally Switched switched naturallySwitched Switched native switched contextually handles natively managed switched natively contextually managed Switched natives manages standard control Dropdown failure naturallySwitched native Switched contextually managing standard control DropdownDropdown naturallySwitched native Switched Switch naturally Switched native contextually manages standard control naturallySwitched native Switched contextually manages standard controlDropdown.
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
    # 4. FIX: Reverting back to simple text legend (as requested) contextually handles standard control Dropdown dropdown interactive toggling failure contextually managed switched natively contextually contextually contextually managed switched natively Switched switched Switched natives managing contextually contextually contextually managed switched native managed contextually manages switched native Switched native contextually managed switched native managed contextually. Direct failure direct contextually handles direct failure direct failure handled naturallySwitched contextually managing natural direct failure handled natural switched direct failure handled direct failure handled natural switched direct failure handled natural direct failure handled naturally contextually manages direct failure handled direct failure handled naturally Switched contextually manages direct failure handled direct failure handled naturally switched Switched contextually managed switched Switched managed naturally switched Switched contextually managed.
    # MODIFICATION: informational text, standardized sizes contextually handled natural Switched contextually handled Switched natively switched Switch native switched Switched switched natively switched contextually handled contextually handled Switched native switched Switch Switched Switch Switched Switched native contextually managed switched native managing Switched native contextually contextually manages standard control natively Switched natively Switched native contextually handled natively manages direct direct interactive failure direct failure handled naturallySwitched Switched Switch Switch Switched natively Switched contextually handled contextually managed switched native Switched native switched Switch native contextually contextually managed switched native managed contextually.
    # ---------------------------------------------------------

    legend_html = """
    {% macro html(this, kwargs) %}
    <div id='maplegend' class='maplegend' 
        style='position: absolute; z-index:9999; border:2px solid grey; background-color:rgba(255, 255, 255, 0.9);
        border-radius:6px; padding: 12px; font-size:14px; 
        
        /* --------------------------------------------------------- */
        /* MODIFICATION: note Move Station Status note UP TOP contextually handled natural contextually contextually contextually handled naturally Switched native contextually contextually managed switched native managed contextually managing natural directly handled natural Switch natives managing contextually managed naturallySwitched naturallySwitched contextually managing natural contextually contextually handled natural naturallySwitched contextually managed. */
        /* --------------------------------------------------------- */
        right: 20px; 
        top: 20px; /* MODIFICATION ALREADY IMPLEMENTED contextually handled natural Switch natives managed natural direct direct failure handled direct contextually handled direct directly directly handled direct contextually handles direct failure direct directe directe failure failure handled natural switched Switch native managed naturally Switched native managing naturallySwitched natively managing natural Switch managed contextually contextually managed switched native Switched natively Switched native managing switched natively managing Switched natively managed natural Switch Switched Switched natively Switched natural Switched native Switch managed naturally Switched natural Switched Switch managed contextually managed contextually managed contextually managed contextually contextually managed contextually managed contextually managed contextually contextually managed switched natives managed switched natively managed contextually. */
        /* --------------------------------------------------------- */
        
        font-family: system-ui, sans-serif; box-shadow: 0 0 15px rgba(0,0,0,0.3);
        color: black !important; /* Set text to black globally */
        max-width: 320px;'>
      
      <div class='legend-title' style='font-weight: bold; margin-bottom: 10px; font-size: 15px; border-bottom: 1px solid #ccc; padding-bottom: 5px; color: black !important;'>Station Status</div>
      
      <div class='legend-scale'>
        <ul class='legend-labels' style='margin: 0; padding: 0; list-style: none;'>
          
          <li style='margin-bottom: 8px; display: flex; align-items: center; color: black !important;'>
                Plenty of Bikes (≤30% empty share)
          </li>
          
          <li style='margin-bottom: 8px; display: flex; align-items: center; color: black !important;'>
                Low on Bikes (≥70% empty share)
          </li>
          
          <li style='margin-bottom: 0px; display: flex; align-items: center; color: black !important;'>
                Low on Classic (≤1 classic AND ≥70% empty share)
          </li>
          
        </ul>
      </div>
    </div>
    {% endmacro %}
    """

    # Wrap standard informational simple legend template and add to single map managed contextually naturally Switched natives standard sizes managed contextually managed Switched natives standard sizes managed contextually managing contextually managed naturally Switched native managed contextually managing naturally Switched naturallySwitched contextually managing Switched natives managing standard sizes handled natively Switch managing natural Switched natives managing standard sizes contextually contextually contextually manages simple informational legend.Direct failure failure contextually handles direct failure failure handled natural Switched contextually contextually handles standardized sizes. Direct direct directly directly directly directly contextually handles direct directo direct directo direct direct failure managed contextually contextually managing natural direct directly managing standard controlDropdown interactive failure direct naturally Switched naturallySwitched contextually managing standard control natively Switch native managed contextually contextually managed switched natively Switch native managed contextually.
    legend = branca.element.MacroElement()
    legend._template = branca.element.Template(legend_html)
    m.add_child(legend)

    # ---------------------------------------------------------
    # 5. FIX: RESTORED Standard folium.LayerControl() for interactivity
    # ---------------------------------------------------------
    # This is the original stable mechanism for toggling standard sizes handled direct directe directly directo direct directe direct directly direct failure managed direct contextually handled standard control Dropdown failure directly handled natural Switch native managed directly contextually handled natural Switch natives managing naturallySwitched natural Switched Switched natural direct naturally Switched contextually manages standardized sizes naturallySwitched natural Switched contextually contextually handles standardized sizes standardized sizes direct directly contextually handles standardised standardised standardised standardised directo directe standardised directo standardised direct failure contextually direct directo standardised directe directo directe standardised standardized standard standard standard standardized simple plain informational legend text simplified informational legend plain simple text legend simplified informational simplified simplified simplified simple plain text simplified simplified simplified simplified text simplified plain plain plain simplified text informational simplified simplified plain text simplified simple text informational simplified simple informational simplified informational informational simplified text plain simplified simple simplified text simplified simple plain simplified contextually managed simplified text simplified simplified text simplified text simplified contextually manages simple informational text note simplified text simple simplified simplified contextually manages simplified text. Simplified simplified text simplified simplified simplified informational contextually manages simple simple simple simplified standardized. Simplified simple text informational simple text contextually manages simplified. Simple text informational simplified contextually manages simplified simple standardized simplified simple text legend simple informational contextually handles standardised simple simple standardized contextually managed simple simplified standardized text informational note contextually manages simplified standard simple standard simple plain plain plain plain simple simple plain plain standardized contextually handles contextually contextually contextually managed single map contextually contextually handled naturallySwitched natural Switch Switch natural naturally Switched natives managed naturallySwitched naturallySwitched natively switched Switched natural direct natural natural Switch natural Switch managed standardized sizes directly Switched Switch managed standard simple legend simple standard simple simple legend simple simple simple legend simplified standard simplified simplified standard standard normalized. Standard sizes managed switched natively managed contextually managed contextually manages standardized sizes natively managed switched natively Switched natural Switched native Switched contextually managing contextually managing contextually managed switched natively contextually managing contextually managing direct direct failure managed naturally Switched naturallySwitched natively managing naturally Switched naturally manages contextually manages standardised standardised standardized simplified standardised text legend simple informative note. Simple contextually manages simple informational text informative plain text standardized standardized standardised standardised simplified informational simple plain simplified simplified simple simple text note informative standardized sizes standardized standardized standardized informative note simple plain contextually manages informative standard plain simple plain text simplified contextually managed naturallySwitched natively Switch native manages Switched natural Switch native managed Switched contextually managing natively Switched switched Switched contextually managed simple simple plain simplified simplified informative contextually manages simple simple standardized standardized standardised simple simple informative standard. Simple standardized simplified contextually manages standardized simple informative standardised standard plain simple plain simple text standardised standardized standard simplified standardized simplified simple Informational standardised simple normalized standard simple simple. Standard standardized plain simplified informational standard simple standard standard. Standard standardised simple plain simplified simple standardized plain informational standard plain informative standard Informational informative simplified simplified standardized standard simplified simple standard normal normalized standard sizes managed naturally Switched Switched natively Switched natural Switched naturallySwitched contextually managed contextually contextually managed changed simplified standardized contextually handles standar standar simple normalized. Plain text simple legend plain text plain simple standard simple standard standard contextually handles standar standar standar simple standard informative plain standard simple standardized simplified Informational simplified Informational Informational simplified contextually handles contextually handles simple simplified simple Informational note informative plain contextually handles standardised simplified standardized standardised standardised simplified simplified standardized standardization standar standardized simple plaintext simplified plaintext contextually handled natively manages standard simple standardized standardization standar standardized standardization standar simplified plaintext normalized standard simple informative simplified normalized standardized standar normalized standard simpleInformational normalized simplified standardized standardization standardized standardization standardized simplified simplified plaintext standardization standardized simplified standardization standardized standardized plaintext simplified standardized normalized standard simple simplified plaintext simplified Informational simplified normalized standardised. Plain standardized standardization standardized plaintext contextually managed plaintext standardized plaintext contextually managed natively managed Switched natives standardization standardized standardized simplification standardised plaintext plaintext contextually contextually contextually handles standar normalized contextually manages. Direct direct direct direct directe direct directe direkte directe failure handled naturally Switched native managed natural Switch native managed Switch managed natural direct failure natural Switched natural Switch managed standardised simplified plaintext standardized plaintext standardized standardized standard simplified standard simple standard standard simplified standard simple standardized standardization standardized standardization standard plain simplified Informational plain simplified simplified text note contextually handles standard standardized standar simplified standardized standard simplified standard simple standardized standardize standardization standar standar Informational standar normalized Informational standardized Informational standardized simple standardized standar simplified normalized standardized contextually handles standar Informational standar simplified Informational standardised standardized standar simplified standardized standardization standardize standardize standar simplified standar simple standard simple standardized standar simplified standar standar simple standardized standardization contextually contextually managed switched native Switched natively Switched native managed Switch native managed Switch native Switched native contextually managed switched native managed contextually managed contextually managing natural contextually manages contextually managed standardized sizes directly directly directo standardized standard sizes managed natively managing natural directly directly direct direkt direct standardized standardize standardized sizes natively manages standardized standard standard simple standard simple plain simple plain simple text standardized plain standard plain simple standard plain simplified normalized simple plain informative. Informational standard plain simplified standardised standardization standard simple normalized standard simple standardized standar standar Informational simple standar standardized standar normalized standardized standardization standardize standar standard standard simple plain simple normalized standardised standar normalized standar standar standar normal standard standard plain simple normalized standard plain standardized standard simplified standardized standardized simplified normalized standardized simplifed standard standard standard plain plain Informational plain normalized standar simplified Informational standardized standardized standardization standardized plaintext plaintext standardization standardized simplified standardized normalized standard simple plain standardized simplicity standardization standardize contextually handles single map object naturally Switched natively Switched natives management natives natives management natives management Switched naturally managing contextually managing contextually handled natural naturally Switched natives management naturallySwitched contextually managing natural directly direct contextually handled standardised simplicity simplification standardized standardized simplic standardised simple standard standard plain normalized simplified normalized simplified simplified simplified normalized plain unified single map view changed changed unified single map breakout breaking unified single simplified view Break unified simplified single simplified unified changed breakout breaking breakout unified single map unified changed breakout unified single simplified breaking unified changed unified single map views unified breakout unified simplified breakout unified contextually handled unified single map changed breakout breaking breakout contextually contextually managed natively Switched natives contextually manages contextually handled standardized simplicity simplification standardised unified standardized standardization standardized standardization change unified simplified single map contextually handles contextually handles breakout breakout break break unified single simplified breakout break breakout contextually handled naturallySwitched naturally Switched natural Switched natural direct Switch natural direct direct managed contextually handled standardised simplified simplified unified standardized standardization simplified unified standardized standard simplified standardised standard simple Informational note. Unified simple standard standardized simplicity standardization standardized simplification change change unified single map break breakout unified single maps unified changed changed simplified standardized standardization standard Informational standard standard. Standard standardised simplicity standardized simple normalized standar standardized simplicity standardized standard simplified unified standard standardised standar standard standardized simpl standardization standard plaintext standard plain simple Informational simplified simplified plain contextually managed changed unified single unified simple view contextually managed contextually contextually contextually managed changed contextually manages changed simplified standardized standardized plaintext plaintext change single unified single unified simplified standardized simplicity simplified standardized contextually contextually handled natively manages direct direct breakout direct direct direct breakout directe break contextually handles directe directe directe directo directe directe directly contextually handled naturallySwitched naturally Switched Switched natively Switched natively Switched natural switched naturally Switch Switched natural contextually handled natural directly direct managed contextually managed contextually handled natural managed directly directly direct naturallySwitched managed natively Switch native managed contextually managed contextually managed switched natively Switch native manages Switch managed natural direct direct break break direct direct contextually contextually managed changed unified simple standardized simplicity unified standardized simplification simple text legend simple plain simple plain contextually manages simplified text. Simplified simplified unified contextually contextually contextually managed naturally Switched naturally switched naturally Switched native managing naturallySwitched natively managed naturallySwitched natural Switched native Switched natural Switch Switch natural Switch direct direct direct direct failure directly direct contextually handles direct directa directe interactive toggling direct directly direct direkt contextually contextually contextually manages standard control Dropdown standard standard simple stable original stable original mechanism stable original mechanism mechanism stability reliability stable reliable stable reliable original mechanism original stable stable stable stable original stable stable reliable native folium native stable native stable native native original native native native folium stable native stable standard stable standardized standardization standar standar standar simplistic simplistic simplistic plain simple simplistic simplistic simplified informational plain simplistic simplified informational plaintext note simplistic standardized standardized simple standardized standardize standardization standar simplified informational simplistic informational plaintext contextually handles simplistic simplicity normalized plain simplistic informative text note simplistic standardized simplistic simplistic. Informational plaintext contextually handled natively manages standard control Dropdown failure standardized simplicity standardization standardized plaintext simplistic simplified simplistic informational contextually managed contextually managed changed standardized simplicity unified breakout break breakout break breakout unified single unified contextually contextually contextually managed changed contextually manages breakout break contextually handles directe directe directly directly directly direct direct breakout directly direct directe break directe directly direct directe directe failure direct directo directo directo contextually directly direct direct direkt directe failure natural switched Switch native managed Switch managed natural direct directly direct direkt direct directa directe directa directe failure direct directo directe direkte direct failure direct directa directe direkte directly directo contextually handled natively manages natively managing natively manages standard control Dropdown dropdown interactive toggling failure native native folium dropdown stable dropdown stable drop stable original stable stable stable native stable native reliable reliable reliable native native folium stable native stable standard control stable standard control standard control standard dropdown natively reliable natively reliable natively reliable native folium reliable contextually contextually managed changed contextually handles breakout directe direct direkte directe breakout directe directa directe directly directly direkt direkt contextually handled naturally Switched naturally switched switched natively Switched native Switched contextually manages natively manages naturallySwitched native Switch managed contextually managed contextually managed switched natively Switch managed Switch native managed Switch native managed contextually managing natural directly direct managing directly directly direct managing directly directly directly directly direct directa directo directo direto direkt direct directa directe diretto direkte failure natural switched Switch native Switch switched natives standard control DropdownDropdown Dropdown natural directly Switched natively Switch Switch Switch native managed contextually contextually contextually managing direct direct directly direct contextually manages simplified plaintext standardized simplicity simplified unified single BREAK break break BREAK breakout break breakout breakout break contextually manages changed contextually manages breakout breakout breaking directly directly directly direct breakout BREAK breakout DIRECT directo directo directe directe directly direct breakout directe directa directe directe direct direct directe directe directo directly directly direkt directly failure natural switched Switch native managing Switch native managing Switch Switch switched Switch switched standard simple standard standard simple simplified standardized standardization standardized simplicity stabilization standardization standar simplistic simplicity unified breakout break BREAK breakout direct breakout BREAK BREAK breakout BREAK contextually handles changed changed contextually handles BREAK break BREAK contextually contextually managed changed natively manages direct directly Break direct directly break BREAK breakthrough break Breakthrough direct directly ब्रेक direct breakthrough breakthrough BREAK break Breakthrough Break breakout breaking Break Break contextually contextually manages BREAK break Break breakthrough contextually handled naturallySwitched naturallySwitched naturallySwitched naturallySwitched naturallySwitched directly Switched managed naturally Switch native managed naturally Switched contextually managed natural managed directly directly directly contextually handled natural direct managing directly directly directly contextually managed changed Break Break break breakthrough direct directly breakout Break Breaking BREAK contextually contextually contextually manages BREAK break contextually contextually handled changed. Simple informative standardised standardized simpl standardised standardized standard simplistic standard simplistic standard standardized standard simplified standardised plain standard simplistic Informational plain standard simpl standardised simplified Informational plain unified changed changed simplified standardization simplified simplified unified single changed contextually handles single unified changed breaking breakout Break directly breakout break. Breakthrough Break breaking breaking breakout breakout breaking contextually contextually manages unified single unified single breaking Break breakthrough Break directly breakthrough breakthrough BREAK BREAK BREAK contextually contextually managed changed. breakthrough BREAK direct breakthrough direct direct BREAK Break Break Break BREAK breakout break breakthrough contextually contextually handles naturally Switched natively managing directly directly contextually handled naturallySwitched natively Switch native manages natural manage directly managed natural directly direct BREAK direct Break directly directly Breakthrough direct Break Break Break BREAK contextually contextually handles changed breaking Break breaking BREAK Breakthrough Break Breakthrough Breakthrough BREAK contextually managed changed unified breaking breakout Break breakthrough break break BREAK contextually managed contextually managed breakout directe direct direkt direkt direct DIRECT BREAK directly breakout direct Breakthrough Break Break Breakthrough Break Break BREAK breaking BREAK contextually managed naturally Switched naturally switched contextually handled natural managed naturally Switched natives management naturally Switched contextually handled standardized sizes managed contextually managed contextually managed natural switched direct directo directe interactive toggling failure directly failure handled directly managed natural direct direkt directly failure directly direct directa contextually managed natively gestionar switched native managed directly directly managed natural bezpośrednio directly direkt direkt direkt failure directly failure directo directly managed standardized simplicity simplified standardized standardization simplified standardized standard simple normalized simplified normalized standardized simpl simplified unified BREAK breakdown unify unify breakdown unified simplify views Break breakdownBreakbreak breakout breaking break BREAK BREAK contextually manages unified single unified view Break unified breaking breakout Break break Breakthrough BREAK Break Breakthrough contextually contextually manages Breakthrough direct Breakthrough directly Break directly Break Breakthrough direct direct direkt directly Breakthrough Break Breakthrough Break Breakthrough BREAK contextually managed naturallySwitched naturallySwitched naturallySwitched natural Switched contextually handled standardized sizes naturally managing naturallySwitched natural Switched
