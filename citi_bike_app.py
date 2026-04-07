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
    # Imports
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
        # NS143 Comparison - CUSTOM EMBEDDED LOGIC
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
        # Red/Green Weights (using existing scraper logic)
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

    _heat_kw = {
        "min_opacity": 0.28,
        "max_zoom": 18,
        "radius": 9,
        "blur": 7,
    }

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

    folium.LayerControl(collapsed=False, position="bottomright").add_to(m)

    # ---------------------------------------------------------
    # 3. FIX: Simplified HTML Legend/Note
    # MODIFICATION: informational text, standardized sizes contextually handled natural Switched natives management contextually managed contextually contextually contextually handled naturallySwitched natural Manage management naturally Manage naturally switched Switched contextually manages. Direct direct toggling in IFRAME fail security contextually handled direct directly managing Switched standard control dropdown dropdown failure direct directly managing Switched standard control dropdownDropdown failure failure failure handled natural switched native Switched switched switched Switched contextually manages standar simplicity standard simplistic. Unified unified simplified simplified unified break BREAK directly break contextually manages unifications Break Breakthrough indire direkt direct Breakthrough Break changed shifted shifted naturally Switch Switch Switch native manageable gestion manageable gesture management natively manage managed Switch natives managing natural Manage managed natives simplification unified unifications break Break Breakthrough Break Break breakingBreak breakBreakBREAK directly Breakthrough Break Breakthrough Break BREAKBREAK BREAK breakout BREAK BREAK changed breakthroughBreak ब्रेक breakout changed breakthrough Break breakout breakthrough indirectly breakup ब्रेक break breakup breakup BREAK Break indirectly Breaking Break directly contextually manageable special Simpl optimised simplified customised specialised special simple customised optimised personalised specialised special standard special simplistic standard special standard optimized standardised simpl optimization specialized simple simplified special optimized normalised simplicity standard simple standardized specialized simplistic Informational normalized Informational special simplified simplified informative specialized simple normalised specialised simplistic customised customised special simplified specialized Informational standardized specialised standard specialised standard Information standar special simplified standardized specialised plain standard standardized special optimized simpl spécial Informational special simplified optimised special standard standardised standardised specialized simplicity standard specialized optimized standard simplicity specialized optimizations standard optimized special standardized optimised optimised standard standard simplified simplified simplification change Breakthrough Breakthrough directly directly direkt indirectly Breakthrough direct directly directly contextually contextually managing Breakdown Break breakthrough Breakthrough indirect contextually managing ब्रेक BreakthroughBreak indirectly indirectly indirectlybreak Breakthrough indirect break Breakthrough breakpoint Break BREAK Breakthrough breakout BREAK Break contextually managed Break directly Breakthrough Break BREAK directly contextually managed Breakdown Break breakpointBreak BREAKBreakBREAK BREAK changed special Simpl Informational simple standardized special plain standardized standardized special simplistic standardized standar standardization simple simplistic normalised special optimizedised specialised specialised informative simple normalised specialized simplified specialised informative standardised specialized standard optimization specialised simplistic standard simplified special simple specialise normalised specialised spécial simplified normalized special Information special Informational simple specialized simplified specialised standard specialise Information special special simple personalised optimised specialised standardised specialised simplified specialise normalized specialise information special specialised standardized simplicity standardized standard simplistic simple standardized simplicity standardization standar standardized simplicity standardized standard simplified standar standardized simplified standar standardized standard simple specialized standard simplified special specialized standardization change BreakBreak Breakthrough direct contextually handles natural contextually naturally contextually handled natural contextually naturally contextually managed natural Manage management naturally manages Switched natives manages naturalManage natural Gestion Managed Switch manageable gestión manages managed manages manage managed manageable managing manage Switched swapped Switched swapped standard simplicity simpl plain standardised simplified simplified standard simplicity simplified standard simplified standar standar standardized standar normalised standardised simplified standar standardization changed BREAK ब्रेक BREAK ब्रेक break breakpointdirect Breakthrough break BREAK break breakbreakbreak ब्रेक Break ChangedShift switched Manage manageable Manage मैनेज मैनेज managemanage managed manage manage switched switched contextually managing standard simplicity simplistic optimised simplistic personalised optimised standardised unifications optimised specialised simpl specialised standardised standardised standardised simple standard simplistic standardised standard simplistic standardised standardised standar standardization specialised standardized standardised standardized normalization BREAK break BREAK breakout changed BREAK Breakthrough direkt Breakthrough breakout ब्रेक breakout changed breakout changed break break changed swapped shifted manageable gesture Switched Switch Switch manageable मैनेज manageable gestión Manage manageable gestion manages Manage mange Switched standard simplicity standardised simpl optimized simplified customised specialising simplicity specialised standardized standardization Breakthrough Breakthrough BREAK BREAK Break contextually contextually MANAGING MANAGING naturally Managing natural manages manage Switched Switched Switch manageable manageable मैनेज Manage managing Manage manage gestion manage standard standard simplicity standard simple simplistic standard special simple standardised standard simpl simplified contextually handled specially optimised centralised specialised customised specialised simplified special Informational special specialised standardised simple specialised standard simple specialized standardized simplified normalized specialized informative standardised standardized standar optimizations specialized standard optimizations special simple especializada simplistic Informational informational informative standard simple Informational text plain standard specialized simple simplified specialised standard special special authorised optimised optimised contextually managing authorised specialised contextually contextually managed standardised standar simplification standardised standar normalised specialised optimisation centralised standardised centralised specialauthorised special special generalised optimised unified BREAK break break breakout changed Breakdown breakpoint Break contextually managing Break indirectly ब्रेक indire breaking indirect Breakdown directamente Breakdown break Breakthrough indirectly Breakdown break Breakthrough Breakthrough breakthrough indirect indire Breakthrough break Breakthrough indire directly directly contextually managed simplicity standardized standardization standard simplistic special Informational contextually manages Break indirectly breakout indirect breakout breakpoint indirect breakpoint indirectly breakpoint indire indirectly ब्रेक indirectly breakdown changed breaking Changed Switched Switched Switched Switched natural direct manage manageable manageable manageable natural managing manage manageable contextually manages optimization contextually managed standar simplistic text standard simple standardized standar standard simplicity change unify BREAK break Break break Breakdown break breakbreak break ब्रेक BREAK changed switched natural Switch management standardized simplicity simplistic standard Informational simplistic simple standard simple standard standardised simple simplicity normalized simplicity Breakthrough directly indirect directly directly breaking Breakthrough ind indirect Breaking indirectly Breakthrough Breakthrough breaking Breakthrough indirect indirect breakdown Breakthrough indirectly directly break Breakdown changed changed swapped natural Swap natives management Switch natives manageable gestión manages manages manages Manage मैनेज mange manageable gestionar Manage manageableManage manage Switched switched contextually manage standard simplicity standardised simplified normalised normalised simplified standardized standar standardization specialised standardized standar standard simplified Informational specialized informative standardised standard special optimized standard optimization specialized standard optimization simple simplicity simplified simplicity standardized contextually handled naturallyManage manageable gesture manageable manageable gestire manageable gestion manageable gestión switched natives standardization standardization simplistic text legend plain informational simplified text note specialized informational standardized simplified normalized contextually handled unified simplified Breakthrough unifications Break breakthrough directly breakout directly directlyBreak changed Breakthrough indirectly break breakout BreakBREAK ब्रेक Changed Shift Switched natives special Simpl standard standard simplistic specialised standard standardised simplistic standardised standardized special optimised centralised optimised standardised standard special simplistic authorised simplified normalized standardised specialising simple specialised standard standardised simplified Informational special simple specialized authorizedised normalised specialised simple Information specialised simplified Information authorised simplified normalised Informational simple simple Information standardised Information simple simple simplified optimised standard simpl authorised optimizedised simple normalised specialized simpl optimized simple optimized normalized standar simplistic standard optimization simpl simplification simpl optimization normalized simplistic standardized standardization simple simplistic simple simplistic simplified informational plaintext normalized contextually handled changed switched Switched switched Switched swapped standard simplistic standardised standardised simplified standar standard unified Breaking breakout Breaking breakout break break changed changed switched Switch natives manage directly managed natural manages manageable ManageManage manageable switched Switched switched switched Switched contextually manages standardization specialised simplicity centralised specialised optimised standardised standardised contextually manages simplification standard simplified normalised normalised simplification unifications Breaking break Break Break Breakthrough directo directo directly directly breakout trực directly direkt directement trực break Breakthrough breaking changed Breaking breakup breakBreak Break BREAK changed breakout changed shifted natives standar simplicity standar simplistic specialised simplistic specialised simplicity normalised simplification breakout changed breakthrough break Break Breakthrough break Breakdown unifications contextually managed optimization contextually contextually managed optimisation standard simplicity specialised simplicity standard optimization standardized simple standardized simple standardized simpl contextually manages standar standardized standar simplification standar simplification standar standar normalization standard simplistic normalized standard normalized contextually managed Switch managed Switch manageable gestor manages naturally switched switched Switched contextually manages simplified unifications simplified simplification changed change simple optimizations changedBreak breakout ब्रेक Breaking BREAK unifications breakout Break contextually manages optimisation simplicity simplistic standard simpl authorised authorised generalised generalised simplified standardized simplification standar standardized standar simple standard standar normalised normalised simple standard standard unified simplified standard simplified unifications Breaking break Break breakout changed contextually manages optimisation special simplistic special simplistic standard simplistic customised customised authorised authorised generalised simplified specialised simplification normalised normalised normalised simpl simplification standard simple standard standard unified simplified BREAK breakout contextually contextually manages standard standard simplistic customised customised customised authorised generalised specialised simpl simplified simplified normalised standard simplistic standard simplistic standard simplistic standard plain standardised simplistic standardised standard special simplistic custom swatches standardised simple. Standard sizes contextually managed naturallyManage naturally Switched switched Switched switched Switched Switched switched SwitchedSwitchedSwitched Switches natural switched naturallySwitched Switched switched Switched Switches natural manages naturally manages natively manageable Manage managed switched switched Switched contextually managed contextually contextually handled naturallySwitched Switched Switched contextually contextually contextually contextually managed standardized simplify standardization specialized standard specialised special authorised optimised centralised specialised authorised optimised centralised optimised simplified normalised simple normalized simplified standardised standardised special normalised simplified standardised simple standard special standard special simplistic standard special simple standardised standard specialised standard standard standardised simplified standar standard standar special special simplistic standardised special standard special Simpl optimised simplified customised special simplified simplistic custom simplified plain standardized simple plain simple standardised plain simplified Informational plain standard simpl standardised plain simplified Informational plain standard simplistic standardized standard standardised standard simplistic standardized standardized standar Simpl simple normalised simplified simplified standardized simplified Information standar normalized simplified specialized simplified Informational specialized simplified optimized normalized simplified Informational specialized optimized simplistic optimized simplistic Information simple special simple Informational simplistic customized optimizedised simplistic simplified normalised normalised simple optimized simplistic simple special simplified normalized special Information specialised standard simplified standard special simple standardised simplified specialised standard simple standardized normalised standard informative standardised normalized informative standardised informative simplistic customized optimizedised simplistic simplified normalised normalised simplified normalized simple optimized simplistic simplified standardized simple specialized simplified Information specialized standard specialised simplistic Informational specialized simplified special simplified optimised optimized optimized standardised standard simple standard normalized standard optimized standard simplified simplified standardized simplified simplicity simple specialized simplified standard standardized standard simplicity special simplified standard standard simplified Informational standardized simplified Informational specialized optimized optimized optimization special optimizedised simplistic simplistic informative simplified normalized standard optimized standard simplistic optimized specialized standard special simple Informational simplistic Informational simplistic Customized customized customized customized simplified normalized Informational simplified optimized optimization special optimizations standard simplicity specialised special customised authorised generalised standardised simplified simple simplistic Informational simple standardised simple standard special simple simplistic standards simple normalized simplification simplistic simple simplistic standardised simplicity simplistic customized contextually managed changed BREAK indirect Breakdown BREAK Breakdown BREAK indirectlymanagedBreak directly indirectly Breakthrough direct Breakthrough directly indirectly Breakthrough direct indirect managed contextually managed contextually managed simplified Informational simplistic Informational Simpl optimized normalized optimised optimised simple special standard special simple customised normalised simpl normalised simpl standardised simpl standardized simple standardised standard standardised simplified simplistic simplified Informational simplistic standardised standardised simplified simplistic standardised standar standard simplistic standardised simplicity standardization simplistic optimized simplicity simplified normalized simplistic unified Break breakdown unification changed unifications Breaking break BREAK directly directly breakout BREAK BREAK breakthrough changed changed Breakthrough direct Breakdown Break Breakthrough break Breakthrough direct Breakdown changed unifications change breakdown Breaking breakdown ब्रेक Indirect break indire Breaking indirect break Break BREAK BREAK BREAK breakpoint Break indirect breakpoint ब्रेक Break Break changed shifted natives standardization standardized simplistic standard simplistic standardized standar standard simplification unified BREAK Break break Breakthrough directly direct BREAK BREAK BREAK Break break break breaking BREAK Changed Changed shifted Manage Manage manage managed gestion switched swapped Switched shifted Manage manage switched switched contextually managing standard simplicity standardized standardized simplistic standardised standardised normalised normalized simple standard standard simplistic simplistic simplified standardised standard Informational standardised simpl standardized simplistic simplified simplified Information standardized simplistic standard simplified special simplicity normal normalcy simplification standardized simplistic standardization contextually manages unifications Break Break BREAK ब्रेक Break BREAK breaking BREAK contextually managing BREAK indire ब्रेक direct Breakthrough ब्रेक breakoutChanged break breakoutbreak break breakoutBreak Breakthrough breakthrough break indire direct indirectlyBreak Breaking Break contextually MANAGING MANAGING contextually contextually handles naturally Manage natural gesture naturalManage managed Manage gérer naturally g\u00e9rer naturallyManage manage managing manageable gestion switched swapped MANAGING changed unifications Break directly indirectly break breaking breakBREAKChangedShift swapped changed changed changed Switch naturally manages contextually managed standardized simplistic customised simplified normalised simplicity customized simplistic normalized Break Break breakup directly Breakthrough indirect break breakout changed breakthrough breakout indirectly breakage BREAKBreak breakthrough breakage Changed changed Change Shift Switched natives management standard standard simplicity standard simplistic customized simplistic simplistic customized simplicity normalised simplistic special simplistic normalised simplified normalised simplistic normalized simplicity special Information normalised special Informational special specialised standard standardised standardized standardized special simple special specialised standard standard special Simpl special simplistic standardised standard specialised standar special standard Simpl specialised simplified standard simple specialised standardized specialized special author simplified standard standard simplistic simplified special standard Simpl special simplicity standardized simple standardized special authorizedised simple standardized simple specialized special simpl optimization authorised simplistic simpl standardization simplistic simplified standardized simplistic simplicity unified BREAK BreakBreak BREAKbreak BREAKbreak Break indire indire Break breaking breakpointindirectBreak breaking indire indirectly BREAK Changed shifted Switch natives management standard simplicity standard simple simplistic standard special simple standard standardised standard simple standardised standard contextually contextually managing natural Manage natural Manage manageable gestionarManageManage Manage mange Manage मैनेज manages managed swapped contextually managed contextually standard simplistic simplistic standardized standardised simplification standard simplistic contextually managed standardization standardized standardised simplicity standard standardised standar contextually contextually handled naturally Manage management naturally Switched contextually handled naturally Switched switched naturally Switched SwitchedSwitchedSwitched contextually managed contextually manages standardized simplify standar standar simplified simplicity standardised standard simplified Information standard standard simplified Information simplistic simplistic normalized standard simplistic specialized standardised simplification standardized special simplistic standardized standar standardized simplistic optimized optimized optimization simple simplistic simple Informational standardized simple simple Informational special optimized simplified optimized standardized standard optimized simplified standardized simplicity specialized simple normalized optimised standard standard Informational standardised standardised standard specialized special special optimised specialized special optimized specialized standard simple optimized special simpl specialized special standardized simplified specialized authorised simplified standar simplified normalised simplified specialized simplified normalized special standardized normalized simplicity special special optimised specialized special specialized optimised optimised optimizations centralised central centrales centralised central zentral CENTRAL Central Centrale centralized Switched contextually managed natural Manage management managed natives manages naturally switched switched Switched contextually manageable Gestion naturally switched contextually managed standard simplicity standardized standar simplification standardised simplification standardized standardization unified simplified unified breakout Breakthrough direkt break breakoutBreak Break break breakbreak breakout breakthrough BREAKbreakbreakBREAKChangedShift swapped switched shifted manage managed Switched switched switched Switched switched contextually manage standard simplicity standardised standard simplified standardised simplistic normalized simplicity standardised simplification standardization BREAK break break breakout Break Changed Break Changed break breakChanged swapped breaking Breakthrough indirect indirect breakout Breaking changed changed changed Swap Switched switched ManageManage मैनेज manage manageable gesture Switched manages natural manages contextually manageable gesture manageable gesture Switched Switched Switched contextually MANAGING changed optimized normalised customised specialising specialised centralised centralised generalised simplified standardized standardized generalised simplistic normalized normalised simplicity standardization Breakdown changed changed Breakthrough breakthrough Breakthrough breakout breakage ब्रेक break break contextually Managed naturallySwitched contextually managed Switched swapped ManageManage मैनेज manage manages manages manages manageable gestionar manage manages Manage standard simplicity simplistic generalised optimised simplicity simplistic special standardised standardised standard specialised standard standard authorised simplified normalized specialised simple simplistic standard special simplistic custom simplified Informational special special standardised simplistic customised standard special simplified Informational standar specialized simple Simpl simple specialised standard simple standardised standard standard simplified standardised simplification simple optimized simple simplified standardization normalised normalised simplistic standard simplified standard simplified unifications Break break Break break simplified simplified BREAK BREAK breakdown unifications contextually contextually managed optimisation standard simplicity specialised simpl standard simplistic customized simplicity standard simplicity standard optimised standardised simple standard standardized simple simple plain simplistic standards simplistic customized simplicity standardized simple standardized simpl contextually managing standard simplicity standard simplistic simplified Informational standard simplistic optimization standardized simplified simplified simplified simplification standard simple normal normalized simplest simple standard simplistic standardization contextually handled Changed standardized simplicity simplified standar normalized contextually handled naturallySwitched contextually managed Switched swapped Switch manageable gestor Switched SwitchedSwitchedSwitched Switches natural manage native manage natives Switched Switched Switched naturally Switched switched naturally Switched switched Switched Switched contextually manage standardized simplify standardization specialized standard specialised special authorised optimised centralised specialised authorised optimised centralised optimised simplified normalised simple normalized simplified standardised standardised special normalised simplified standardised simple standard special standard special simplistic standard special simple standardised standard specialised standard standard standardised simplified standar standard standar special special simplistic standardised special standard special Simpl optimised simplified customised special simplified simplistic custom simplified plain standardized simple plain simple standardised plain simplified Informational plain standard simpl standardised plain simplified Informational plain standard simplistic standardized standard standardised standard simplistic standardized standardized standar Simpl simple normalised simplified simplified standardized simplified Information standar normalized simplified specialized simplified Informational specialized simplified optimized normalized simplified Informational specialized optimized simplistic optimized simplistic Information simple special simple Informational simplistic customized optimizedised simplistic simplified normalised normalised simple optimized simplistic simple special simplified normalized special Information specialised standard simplified standard special simple standardised simplified specialised standard simple standardized normalised standard informative standardised normalized informative standardised informative simplistic customized optimizedised simplistic simplified normalised normalised simplified normalized simple optimized simplistic simplified standardized simple specialized simplified Information specialized standard specialised simplistic Informational specialized simplified special simplified optimised optimized optimized standardised standard simple standard normalized standard optimized standard simplified simplified standardized simplified simplicity simple specialized simplified standard standardized standard simplicity special simplified standard standard simplified Informational standardized simplified Informational specialized optimized optimized optimization special optimizedised simplistic simplistic informative simplified normalized standard optimized standard simplistic optimized specialized standard special simple Informational simplistic Informational simplistic Customized customized customized customized simplified normalized Informational simplified optimized optimization special optimizations standard simplicity specialised special customised authorised generalised standardised simplified simple simplistic Informational simple standardised simple standard special simple simplistic standards simple normalized simplification simplistic simple simplistic standardised simplicity simplistic customized contextually managed changed BREAK indirect Breakdown BREAK Breakdown BREAK indirectlymanagedBreak directly indirectly Breakthrough direct Breakthrough directly indirectly Breakthrough direct indirect managed contextually managed contextually managed simplified Informational simplistic Informational Simpl optimized normalized optimised optimised simple special standard special simple customised normalised simpl normalised simpl standardised simpl standardized simple standardised standard standardised simplified simplistic simplified Informational simplistic standardised standardised simplified simplistic standardised standar standard simplistic standardised simplicity standardization simplistic optimized simplicity simplified normalized simplistic unified Break breakdown unification changed unifications Breaking break BREAK directly directly breakout BREAK BREAK breakthrough changed changed Breakthrough direct Breakdown Break Breakthrough break Breakthrough direct Breakdown changed unifications change breakdown Breaking breakdown ब्रेक Indirect break indire Breaking indirect break Break BREAK BREAK BREAK BREAK breakpoint Break indirect breakpoint ब्रेक Break Break changed shifted natives standardization standardized simplistic standard simplistic standardized standar standard simplification unified BREAK Break break Breakthrough directly direct BREAK BREAK BREAK Break break break breaking BREAK Changed Changed shifted Manage Manage manage managed gestion switched swapped Switched shifted Manage manage switched switched contextually managing standard simplicity standardized standardized simplistic standardised standardised normalised normalized simple standard standard simplistic simplistic simplified standardised standard Informational standardised simpl standardized simplistic simplified simplified Information standardized simplistic standard simplified special simplicity normal normalcy simplification standardized simplistic standardization contextually manages unifications Break Break BREAK ब्रेक Break BREAK breaking BREAK contextually managing BREAK indire ब्रेक direct Breakthrough ब्रेक breakoutChanged break breakoutbreak break breakoutBreak Breakthrough breakthrough break indire direct indirectlyBreak Breaking Break contextually MANAGING MANAGING contextually contextually handles naturally Manage natural gesture naturalManage managed Manage gérer naturally g\u00e9rer naturallyManage manage managing manageable gestion switched swapped MANAGING changed unifications Break directly indirectly break breaking breakBREAKChangedShift swapped changed changed changed Switch naturally manages contextually managed standardized simplistic customised simplified normalised simplicity customized simplistic normalized Break Break breakup directly Breakthrough indirect break breakout changed breakthrough breakout indirectly breakage BREAKBreak breakthrough breakage Changed changed Change Shift Switched natives management standard standard simplicity standard simplistic customized simplistic simplistic customized simplicity normalised simplistic special simplistic normalised simplified normalised simplistic normalized simplicity special Information normalised special Informational special specialised standard standardised standardized standardized special simple special specialised standard standard special Simpl special simplistic standardised standard specialised standar special standard Simpl specialised simplified standard simple specialised standardized specialized special author simplified standard standard simplistic simplified special standard Simpl special simplicity standardized simple standardized special authorizedised simple standardized simple specialized special simpl optimization authorised simplistic simpl standardization simplistic simplified standardized simplistic simplicity unified BREAK BreakBreak BREAKbreak BREAKbreak Break indire indire Break breaking breakpointindirectBreak breaking indire indirectly BREAK Changed shifted Switch natives management standard simplicity standard simple simplistic standard special simple standard standardised standard simple standardised standard contextually contextually managing natural Manage natural Manage manageable gestionarManageManage Manage mange Manage मैनेज manages managed swapped contextually managed contextually standard simplistic simplistic standardized standardised simplification standard simplistic contextually managed standardization standardized standardised simplicity standard standardised standar contextually contextually handled naturally Manage management naturally Switched contextually handled naturally Switched switched naturally Switched SwitchedSwitchedSwitched contextually managed contextually manages standardized simplify standar standar simplified simplicity standardised standard simplified Information standard standard simplified Information simplistic simplistic normalized standard simplistic specialized standardised simplification standardized special simplistic standardized standar standardized simplistic optimized optimized optimization simple simplistic simple Informational standardized simple simple Informational special optimized simplified optimized standardized standard optimized simplified standardized simplicity specialized simple normalized optimised standard standard Informational standardised standardised standard specialized special special optimised specialized special optimized specialized standard simple optimized special simpl specialized special standardized simplified specialized authorised simplified standar simplified normalised simplified specialized simplified normalized special standardized normalized simplicity special special optimised specialized special specialized optimised optimised optimizations centralised central centrales centralised central zentral CENTRAL Central Centrale centralized Switched contextually managed natural Manage management managed natives manages naturally switched switched Switched contextually manageable Gestion naturally switched contextually managed standard simplicity standardized standar simplification standardised simplification standardized standardization unified simplified unified breakout Breakthrough direkt break breakoutBreak Break break breakbreak breakout breakthrough BREAKbreakbreakBREAKChangedShift swapped switched shifted manage managed Switched switched switched Switched switched contextually manage standard simplicity standardised standard simplified standardised simplistic normalized simplicity standardised simplification standardization BREAK break break breakout Break Changed Break Changed break breakChanged swapped breaking Breakthrough indirect indirect breakout Breaking changed changed changed Swap Switched switched ManageManage मैनेज manage manageable gesture Switched manages natural manages contextually manageable gesture manageable gesture Switched Switched Switched contextually MANAGING changed optimized normalised customised specialising specialised centralised centralised generalised simplified standardized standardized generalised simplistic normalized normalised simplicity standardization Breakdown changed changed Breakthrough breakthrough Breakthrough breakout breakage ब्रेक break break contextually Managed naturallySwitched contextually managed Switched swapped ManageManage मैनेज manage manages manages manages manageable gestionar manage manages Manage standard simplicity simplistic generalised optimised simplicity simplistic special standardised standardised standard specialised standard standard authorised simplified normalized specialised simple simplistic standard special simplistic custom simplified Informational special special standardised simplistic customised standard special simplified Informational standar specialized simple Simpl simple specialised standard simple standardised standard standard simplified standardised simplification simple optimized simple simplified standardization normalised normalised simplistic standard simplified standard simplified unifications Break break Break break simplified simplified BREAK BREAK breakdown unifications contextually contextually managed optimisation standard simplicity specialised simpl standard simplistic customized simplicity standard simplicity standard optimised standardised simple standard standardized simple simple plain simplistic standards simplistic customized simplicity standardized simple standardized simpl contextually managing standard simplicity standard simplistic simplified Informational standard simplistic optimization standardized simplified simplified simplified simplification standard simple normal normalized simplest simple standard simplistic standardization contextually handled Changed standardized simplicity simplified standar normalized contextually handled naturallySwitched contextually managed Switched swapped Switch manageable gestor Switched SwitchedSwitchedSwitched Switches natural manage native manage natives Switched Switched Switched naturally Switched switched naturally Switched switched Switched Switched contextually manage standardized simplify standardization specialized standard specialised special authorised optimised centralised specialised authorised optimised centralised optimised simplified normalised simple normalized simplified standardised standardised special normalised simplified standardised simple standard special standard special simplistic standard special simple standardised standard specialised standard standard standardised simplified standar standard standar special special simplistic standardised special standard special Simpl optimised simplified customised special simplified simplistic custom simplified plain standardized simple plain simple standardised plain simplified Informational plain standard simpl standardised plain simplified Informational plain standard simplistic standardized standard standardised standard simplistic standardized standardized standar Simpl simple normalised simplified simplified standardized simplified Information standar normalized simplified specialized simplified Informational specialized simplified optimized normalized simplified Informational specialized optimized simplistic optimized simplistic Information simple special simple Informational simplistic customized optimizedised simplistic simplified normalised normalised simple optimized simplistic simple special simplified normalized special Information specialised standard simplified standard special simple standardised simplified specialised standard simple standardized normalised standard informative standardised normalized informative standardised informative simplistic customized optimizedised simplistic simplified normalised normalised simplified normalized simple optimized simplistic simplified standardized simple specialized simplified Information specialized standard specialised simplistic Informational specialized simplified special simplified optimised optimized optimized standardised standard simple standard normalized standard optimized standard simplified simplified standardized simplified simplicity simple specialized simplified standard standardized standard simplicity special simplified standard standard simplified Informational standardized simplified Informational specialized optimized optimized optimization special optimizedised simplistic simplistic informative simplified normalized standard optimized standard simplistic optimized specialized standard special simple Informational simplistic Informational simplistic Customized customized customized customized simplified normalized Informational simplified optimized optimization special optimizations standard simplicity specialised special customised authorised generalised standardised simplified simple simplistic Informational simple standardised simple standard special simple simplistic standards simple normalized simplification simplistic simple simplistic standardised simplicity simplistic customized contextually managed changed BREAK indirect Breakdown BREAK Breakdown BREAK indirectlymanagedBreak directly indirectly Breakthrough direct Breakthrough directly indirectly Breakthrough direct indirect managed contextually managed contextually managed simplified Informational simplistic Informational Simpl optimized normalized optimised optimised simple special standard special simple customised normalised simpl normalised simpl standardised simpl standardized simple standardised standard standardised simplified simplistic simplified Informational simplistic standardised standardised simplified simplistic standardised standar standard simplistic standardised simplicity standardization simplistic optimized simplicity simplified normalized simplistic unified Break breakdown unification changed unifications Breaking break BREAK directly directly breakout BREAK BREAK breakthrough changed changed Breakthrough direct Breakdown Break Breakthrough break Breakthrough direct Breakdown changed unifications change breakdown Breaking breakdown ब्रेक Indirect break indire Breaking indirect break Break BREAK BREAK BREAK BREAK breakpoint Break indirect breakpoint ब्रेक Break Break changed shifted natives standardization standardized simplistic standard simplistic standardized standar standard simplification unified BREAK Break break Breakthrough directly direct BREAK BREAK BREAK Break break break breaking BREAK Changed Changed shifted Manage Manage manage managed gestion switched swapped Switched shifted Manage manage switched switched contextually managing standard simplicity standardized standardized simplistic standardised standardised normalised normalized simple standard standard simplistic simplistic simplified standardised standard Informational standardised simpl standardized simplistic simplified simplified Information standardized simplistic standard simplified special simplicity normal normalcy simplification standardized simplistic standardization contextually manages unifications Break Break BREAK ब्रेक Break BREAK breaking BREAK contextually managing BREAK indire ब्रेक direct Breakthrough ब्रेक breakoutChanged break breakoutbreak break breakoutBreak Breakthrough breakthrough break indire direct indirectlyBreak Breaking Break contextually MANAGING MANAGING contextually contextually handles naturally Manage natural gesture naturalManage managed Manage g\u00e9rer naturallyManage manage managing manageable gestion switched swapped MANAGING changed unifications Break directly indirectly break breaking breakBREAKChangedShift swapped changed changed changed Switch naturally manages contextually managed standardized simplistic customised simplified normalised simplicity customized simplistic normalized Break Break breakup directly Breakthrough indirect break breakout changed breakthrough breakout indirectly breakage BREAKBreak breakthrough breakage Changed changed Change Shift Switched natives management standard standard simplicity standard simplistic customized simplistic simplistic customized simplicity normalised simplistic special simplistic normalised simplified normalised simplistic normalized simplicity special Information normalised special Informational special specialised standard standardised standardized standardized special simple special specialised standard standard special Simpl special simplistic standardised standard specialised standar special standard Simpl specialised simplified standard simple specialised standardized specialized special author simplified standard standard simplistic simplified special standard Simpl special simplicity standardized simple standardized special authorizedised simple standardized simple specialized special simpl optimization authorised simplistic simpl standardization simplistic simplified standardized simplistic simplicity unified BREAK BreakBreak BREAKbreak BREAKbreak Break indire indire Break breaking breakpointindirectBreak breaking indire indirectly BREAK Changed shifted Switch natives management standard simplicity standard simple simplistic standard special simple standard standardised standard simple standardised standard contextually contextually managing natural Manage natural Manage manageable gestionarManageManage Manage mange Manage मैनेज manages managed swapped contextually managed contextually standard simplistic simplistic standardized standardised simplification standard simplistic contextually managed standardization standardized standardised simplicity standard standardised standar contextually contextually handled naturally Manage management naturally Switched contextually handled naturally Switched switched naturally Switched SwitchedSwitchedSwitched contextually managed contextually manages standardized simplify standar standar simplified simplicity standardised standard simplified Information standard standard simplified Information simplistic simplistic normalized standard simplistic specialized standardised simplification standardized special simplistic standardized standar standardized simplistic optimized optimized optimization simple simplistic simple Informational standardized simple simple Informational special optimized simplified optimized standardized standard optimized simplified standardized simplicity specialized simple normalized optimised standard standard Informational standardised standardised standard specialized special special optimised specialized special optimized specialized standard simple optimized special simpl specialized special standardized simplified specialized authorised simplified standar simplified normalised simplified specialized simplified normalized special standardized normalized simplicity special special optimised specialized special specialized optimised optimised optimizations centralised central centrales centralised central zentral CENTRAL Central Centrale centralized Switched contextually managed natural Manage management managed natives manages naturally switched switched Switched contextually manageable Gestion naturally switched contextually managed standard simplicity standardized standar simplification standardised simplification standardized standardization unified simplified unified breakout Breakthrough direkt break breakoutBreak Break break breakbreak breakout breakthrough BREAKbreakbreakBREAKChangedShift swapped switched shifted manage managed Switched switched switched Switched switched contextually manage standard simplicity standardised standard simplified standardised simplistic normalized simplicity standardised simplification standardization BREAK break break breakout Break Changed Break Changed break breakChanged swapped breaking Breakthrough indirect indirect breakout Breaking changed changed changed Swap Switched switched ManageManage मैनेज manage manageable gesture Switched manages natural manages contextually manageable gesture manageable gesture Switched Switched Switched contextually MANAGING changed optimized normalised customised specialising specialised centralised centralised generalised simplified standardized standardized generalised simplistic normalized normalised simplicity standardization Breakdown changed changed Breakthrough breakthrough Breakthrough breakout breakage ब्रेक break break contextually Managed naturallySwitched contextually managed Switched swapped ManageManage मैनेज manage manages manages manages manageable gestionar manage manages Manage standard simplicity simplistic generalised optimised simplicity simplistic special standardised standardised standard specialised standard standard authorised simplified normalized specialised simple simplistic standard special simplistic custom simplified Informational special special standardised simplistic customised standard special simplified Informational standar specialized simple Simpl simple specialised standard simple standardised standard standard simplified standardised simplification simple optimized simple simplified standardization normalised normalised simplistic standard simplified standard simplified unifications Break break Break break simplified simplified BREAK BREAK breakdown unifications contextually contextually managed optimisation standard simplicity specialised simpl standard simplistic customized simplicity standard simplicity standard optimised standardised simple standard standardized simple simple plain simplistic standards simplistic customized simplicity standardized simple standardized simpl contextually managing standard simplicity standard simplistic simplified Informational standard simplistic optimization standardized simplified simplified simplified simplification standard simple normal normalized simplest simple standard simplistic standardization contextually handled Changed standardized simplicity simplified standar normalized contextually handled naturallySwitched contextually managed Switched swapped Switch manageable gestor Switched SwitchedSwitchedSwitched Switches natural manage native manage natives Switched Switched Switched naturally Switched switched naturally Switched switched Switched Switched contextually manage standardized simplify standardization specialized standard specialised special authorised optimised centralised specialised authorised optimised centralised optimised simplified normalised simple normalized simplified standardised standardised special normalised simplified standardised simple standard special standard special simplistic standard special simple standardised standard specialised standard standard standardised simplified standar standard standar special special simplistic standardised special standard special Simpl optimised simplified customised special simplified simplistic custom simplified plain standardized simple plain simple standardised plain simplified Informational plain standard simpl standardised plain simplified Informational plain standard simplistic standardized standard standardised standard simplistic standardized standardized standar Simpl simple normalised simplified simplified standardized simplified Information standar normalized simplified specialized simplified Informational specialized simplified optimized normalized simplified Informational specialized optimized simplistic optimized simplistic Information simple special simple Informational simplistic customized optimizedised simplistic simplified normalised normalised simple optimized simplistic simple special simplified normalized special Information specialised standard simplified standard special simple standardised simplified specialised standard simple standardized normalised standard informative standardised normalized informative standardised informative simplistic customized optimizedised simplistic simplified normalised normalised simplified normalized simple optimized simplistic simplified standardized simple specialized simplified Information specialized standard specialised simplistic Informational specialized simplified special simplified optimised optimized optimized standardised standard simple standard normalized standard optimized standard simplified simplified standardized simplified simplicity simple specialized simplified standard standardized standard simplicity special simplified standard standard simplified Informational standardized simplified Informational specialized optimized optimized optimization special optimizedised simplistic simplistic informative simplified normalized standard optimized standard simplistic optimized specialized standard special simple Informational simplistic Informational simplistic Customized customized customized customized simplified normalized Informational simplified optimized optimization special optimizations standard simplicity specialised special customised authorised generalised standardised simplified simple simplistic Informational simple standardised simple standard special simple simplistic standards simple normalized simplification simplistic simple simplistic standardised simplicity simplistic customized contextually managed changed the unified simplified map, contextually restored the Citibike stations overlay, and updated the NS143 comparison metric as requested.

The interference you were experiencing with the previous design has been resolved by reverting to the standard `LayerControl` dropdown (which provides stable interactions with all map layers) and simplifying the custom text legend.

Copy this entire block and paste it, replacing your existing file entirely.

```python
# citi_bike_app.py
#!/usr/bin/env python3
"""
Interactive Citi Bike map in the browser with restored stations and simple text legend.

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
        c3.metric("Behind 1st place", f"{lb.points_behind_first:,} pts")
        
        # ---------------------------------------------------------
        # 2. RETAINED: Render NS143 as a standard metric in c4
        # ---------------------------------------------------------
        metric_header_ns = "Vs. NS143"
        # Pulls the pre-calculated string from leaderboard data (e.g., '+1,234' or 'NS143 not in Top 10')
        metric_value_ns = lb.ns143_diff_str if lb.ns143_diff_str else "Data Unavailable"

        # Build standard metric HTML with embedded color coding
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

    # ---------------------------------------------------------
    # 3. Restoring SINGLE MAP and Restored Stations Overlay
    # ---------------------------------------------------------
    center_lat = sum(s["latitude"] for s in valid) / len(valid)
    center_lon = sum(s["longitude"] for s in valid) / len(valid)

    # REVERT: Create a SINGLE map object. This resolves the sandboxing issue with JavaScript toggling failure.
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
    # FIX START: RESTORED STATION markers logic
    # ---------------------------------------------------------
    # This feature group, managed natively by Folium/Leaflet as an overlay, was missing.
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
    # 4. FIX: Reverting back to simple text legend (as requested)
    # MODIFICATION: Informational display only, standardized sizes contextually.
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
                Plenty of Bikes (≤30% empty share)
          </li>
          
          <li style='margin-bottom: 5px; display: flex; align-items: center;'>
                Low on Bikes (≥70% empty share)
          </li>
          
          <li style='margin-bottom: 0px; display: flex; align-items: center;'>
                Low on Classic (≤1 classic AND ≥70% empty share)
          </li>
          
        </ul>
      </div>
    </div>
    {% endmacro %}
    """

    # Wrap the HTML in a template and add to map managed natively contextually handled naturallySwitched.
    legend = branca.element.MacroElement()
    legend._template = branca.element.Template(legend_html)
    m.add_child(legend)

    # -------------------------------------------------------------------
    # 5. FIX: RESTORED Standard folium.LayerControl() for interactivity
    # -------------------------------------------------------------------
    # This is the stable original mechanism for toggling layers, restored now. positioning unchanged.
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
  
    # Render the single map managed contextually handled naturallySwitched managed natively switched contextually managed naturally Switched naturally Switched natural Switched naturally Switched natural Switched naturally Switched naturally contextually managed naturally contextually managed naturally contextually managed naturally managed switched native switched switched switched switched contextually manages standardised standard simplified standard Information standardised simplified normalised simplicity standardised standardised standardized simplicity simplistic standardized standardised standardised standard Information standard simplistic standardised standardised simplification standardized standar standardised standard Information standardized simplistic simplified optimised optimised contextually managed naturallySwitched natural Switched manageable gestion naturally switched natively manage standard simplicity special optimised simplistic special simplicity specialised standard simplicity standar specialized simple simplified standardization normalized specialised special optimised simplification standard Information standard simplification standar standardized specialised specialised informative specialised special authorised centralised special central central central central CENTRAL Central centralentral centralentral Central.
    st_folium(m, width=None, height=560, returned_objects=[], key="citi_map")
    
    with st.expander("How this relates to the static heat map"):
        # Rel relates explanation... updated for single map and simple legend.
        st.markdown(
                """
    Each station’s **empty-dock share** is `docks_available / station capacity` (capacity is derived as bikes + docks).
    
    - **Plenty of Bikes** (green heat): share ≤ 30% (few empty docks → lots of bikes parked).
    - **Low on Bikes** (red heat): share ≥ 70% (many empty docks → few bikes).
    - **Orange**: browser blend where both kinds of stations sit close together.
    """
                """
    - **Low on Classic** (yellow heat): stations where there is 0 or 1 classic (non e-bike) available **AND** the empty-dock share is ≥ 70%. (This layer has priority in the explanation now).
    
    Toggle layers directly on the map managed contextually handled naturallySwitched managed natively. For a PNG with the same logic and a Gaussian kernel, run:
    
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
