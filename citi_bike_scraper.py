#!/usr/bin/env python3
"""
Citi Bike NYC - Real-time dock and bike availability scraper.

Uses Citi Bike GBFS API to fetch live station status
"""

import argparse
import json
import math
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

# Citi Bike GBFS API endpoints (no auth required)
STATION_STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
STATION_INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"

# Heat maps: fraction of station capacity that is empty docks = docks_available / capacity
# Red = mostly empty (low on parked bikes); green = mostly full (few empty docks).
HEAT_RED_MIN_EMPTY_SHARE = 0.7
HEAT_GREEN_MAX_EMPTY_SHARE = 0.3


def fetch_json(url: str) -> dict:
    """Fetch and parse JSON from URL."""
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.loads(response.read().decode())


def get_station_info() -> dict[str, dict]:
    """Fetch station metadata (name, location, capacity)."""
    data = fetch_json(STATION_INFO_URL)
    return {s["station_id"]: s for s in data["data"]["stations"]}


def get_station_status() -> list[dict]:
    """Fetch real-time dock/bike availability for all stations."""
    data = fetch_json(STATION_STATUS_URL)
    return data["data"]["stations"]


def scrape_availability(
    *,
    filter_empty_docks: bool = False,
    filter_empty_bikes: bool = False,
    min_bikes: int | None = None,
    min_docks: int | None = None,
) -> list[dict]:
    """
    Scrape current Citi Bike dock availability.

    Args:
        filter_empty_docks: If True, exclude stations with 0 docks available
        filter_empty_bikes: If True, exclude stations with 0 bikes available
        min_bikes: Only include stations with at least this many bikes
        min_docks: Only include stations with at least this many docks

    Returns:
        List of station dicts with availability data
    """
    info = get_station_info()
    status = get_station_status()
    results = []

    for s in status:
        station_id = s["station_id"]
        meta = info.get(station_id, {})
        station_data = {
            "station_id": station_id,
            "name": meta.get("name", "Unknown"),
            "latitude": meta.get("lat"),
            "longitude": meta.get("lon"),
            "capacity": meta.get("capacity"),
            "bikes_available": s.get("num_bikes_available", 0),
            "ebikes_available": s.get("num_ebikes_available", 0),
            "docks_available": s.get("num_docks_available", 0),
            "docks_disabled": s.get("num_docks_disabled", 0),
            "bikes_disabled": s.get("num_bikes_disabled", 0),
            "is_renting": s.get("is_renting", True),
            "is_returning": s.get("is_returning", True),
            "last_reported": datetime.fromtimestamp(s.get("last_reported", 0)),
        }

        if filter_empty_docks and station_data["docks_available"] == 0:
            continue
        if filter_empty_bikes and station_data["bikes_available"] == 0:
            continue
        if min_bikes is not None and station_data["bikes_available"] < min_bikes:
            continue
        if min_docks is not None and station_data["docks_available"] < min_docks:
            continue

        results.append(station_data)

    return results


def station_capacity_total(s: dict) -> int:
    """Total dock positions at a station (GBFS capacity, or bikes + docks as fallback)."""
    cap = s.get("capacity")
    if cap is not None and int(cap) > 0:
        return int(cap)
    b = int(s.get("bikes_available", 0) or 0)
    d = int(s.get("docks_available", 0) or 0)
    return max(1, b + d)


def dock_empty_share(s: dict) -> float | None:
    """Fraction of capacity that is empty docks. None if unknown."""
    total = station_capacity_total(s)
    if total <= 0:
        return None
    return float(s.get("docks_available", 0) or 0) / float(total)


def heat_red_green_weights(s: dict) -> tuple[float, float]:
    """
    Weights for overlapping heat layers (red vs green).

    Red: station is "low on bikes" — empty docks are >= HEAT_RED_MIN_EMPTY_SHARE of capacity.
    Green: station is "full of bikes" — empty docks are <= HEAT_GREEN_MAX_EMPTY_SHARE of capacity.
    Returns (red_weight, green_weight); middle band contributes neither (0, 0).
    """
    frac = dock_empty_share(s)
    if frac is None:
        return 0.0, 0.0
    if frac >= HEAT_RED_MIN_EMPTY_SHARE:
        return 1.0, 0.0
    if frac <= HEAT_GREEN_MAX_EMPTY_SHARE:
        return 0.0, 1.0
    return 0.0, 0.0


def write_bike_dock_heatmap(
    stations: list[dict],
    path: str,
    *,
    grid_size: int = 480,
    sigma_px: float = 2.0,
    heat_alpha: float = 0.62,
) -> None:
    """
    Write a PNG heat map: red = low on bikes (empty docks ≥70% of capacity),
    green = few empty docks (≤30%), orange where both station types overlap
    spatially after Gaussian smoothing. Basemap: CartoDB Voyager (contextily).
    """
    try:
        import contextily as ctx
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
        import xyzservices.providers as xp
        from matplotlib.patches import Patch
        from scipy.ndimage import gaussian_filter
    except ImportError as e:
        print(
            "Heat map needs: pip install matplotlib numpy scipy contextily",
            file=sys.stderr,
        )
        raise SystemExit(1) from e

    rows = []
    for s in stations:
        if s.get("latitude") is None or s.get("longitude") is None:
            continue
        rw, gw = heat_red_green_weights(s)
        if rw <= 0 and gw <= 0:
            continue
        rows.append((float(s["longitude"]), float(s["latitude"]), rw, gw))
    if not rows:
        print("No stations qualify for heat map; skipping heat map.", file=sys.stderr)
        return

    lons = np.array([r[0] for r in rows])
    lats = np.array([r[1] for r in rows])
    red_w = np.array([r[2] for r in rows])
    green_w = np.array([r[3] for r in rows])

    pad = 0.012
    lon_min, lon_max = lons.min() - pad, lons.max() + pad
    lat_min, lat_max = lats.min() - pad, lats.max() + pad

    red_grid = np.zeros((grid_size, grid_size), dtype=np.float64)
    green_grid = np.zeros((grid_size, grid_size), dtype=np.float64)
    ix = np.clip(
        ((lons - lon_min) / (lon_max - lon_min) * (grid_size - 1)).astype(int),
        0,
        grid_size - 1,
    )
    iy = np.clip(
        ((lats - lat_min) / (lat_max - lat_min) * (grid_size - 1)).astype(int),
        0,
        grid_size - 1,
    )
    for i in range(len(rows)):
        red_grid[iy[i], ix[i]] += red_w[i]
        green_grid[iy[i], ix[i]] += green_w[i]

    red_s = gaussian_filter(red_grid, sigma=sigma_px)
    green_s = gaussian_filter(green_grid, sigma=sigma_px)
    rmax, gmax = red_s.max(), green_s.max()
    red_n = red_s / rmax if rmax > 0 else red_s
    green_n = green_s / gmax if gmax > 0 else green_s
    rgb = np.dstack([red_n, green_n, np.zeros_like(red_n)])

    fig, ax = plt.subplots(figsize=(12, 13))
    ax.set_xlim(lon_min, lon_max)
    ax.set_ylim(lat_min, lat_max)
    lat_c = (lat_min + lat_max) / 2
    ax.set_aspect(1 / math.cos(math.radians(lat_c)))

    ctx.add_basemap(
        ax,
        crs="EPSG:4326",
        source=xp.CartoDB.Voyager,
        zoom="auto",
        zorder=0,
        attribution=False,
    )

    ax.imshow(
        rgb,
        extent=[lon_min, lon_max, lat_min, lat_max],
        origin="lower",
        interpolation="bilinear",
        alpha=heat_alpha,
        zorder=1,
    )
    ax.set_xlim(lon_min, lon_max)
    ax.set_ylim(lat_min, lat_max)

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    fig.suptitle("Ride on PV758", fontsize=16, fontweight="bold")
    legend_elements = [
        Patch(
            facecolor="#cc0000",
            edgecolor="white",
            label="Low on Bikes",
        ),
        Patch(
            facecolor="#00aa00",
            edgecolor="white",
            label="Plenty of Bikes",
        ),
        Patch(facecolor="#e68619", edgecolor="white", label="Both station types nearby (overlap)"),
    ]
    leg = ax.legend(handles=legend_elements, loc="lower right", framealpha=0.92)
    leg.set_zorder(10)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Citi Bike NYC dock/bike availability in real time"
    )
    parser.add_argument(
        "--watch",
        "-w",
        type=int,
        metavar="SECONDS",
        help="Poll continuously every N seconds (real-time mode)",
    )
    parser.add_argument(
        "--min-docks",
        type=int,
        default=1,
        help="Minimum docks available to include (default: 1)",
    )
    parser.add_argument(
        "--min-bikes",
        type=int,
        default=1,
        help="Minimum bikes available to include (default: 1)",
    )
    parser.add_argument(
        "--save",
        "-s",
        metavar="FILE",
        help="Save full JSON to file (default: citi_bike_availability.json)",
    )
    parser.add_argument(
        "--limit",
        "-n",
        type=int,
        default=30,
        help="Max stations to print (0 = all, default: 30)",
    )
    parser.add_argument(
        "--heatmap",
        "-m",
        metavar="FILE.png",
        help="PNG heat map: green ≤30% empty docks, red ≥70% empty docks (share of capacity); orange = overlap",
    )
    args = parser.parse_args()

    def run_once() -> tuple[list[dict], list[dict]]:
        all_stations = scrape_availability(min_bikes=0, min_docks=0)
        all_stations.sort(key=lambda s: s["name"])
        filtered = [
            s
            for s in all_stations
            if s["bikes_available"] >= args.min_bikes
            and s["docks_available"] >= args.min_docks
        ]
        return all_stations, filtered

    def print_stations(stations: list[dict]) -> None:
        limit = args.limit if args.limit > 0 else len(stations)
        print(f"\n{'='*70}")
        print(f"Citi Bike NYC - {len(stations)} stations with bikes and docks available")
        print(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}\n")

        for s in stations[:limit]:
            print(f"{s['name']}")
            print(f"  Bikes: {s['bikes_available']} (e-bikes: {s['ebikes_available']})")
            print(f"  Docks: {s['docks_available']}")
            lat, lon = s.get("latitude"), s.get("longitude")
            if lat is not None and lon is not None:
                print(f"  Location: {lat}, {lon}")
            print()

        if limit < len(stations):
            print(f"... and {len(stations) - limit} more stations\n")

    def save_json(stations: list[dict]) -> None:
        path = args.save or "citi_bike_availability.json"
        output = {
            "fetched_at": datetime.now().isoformat(),
            "stations": stations,
        }
        with open(path, "w") as f:
            json.dump(output, f, indent=2, default=str)
        print(f"Saved to {path}")

    if args.watch:
        print(f"Watching Citi Bike availability (refreshing every {args.watch}s). Ctrl+C to stop.\n")
        try:
            while True:
                try:
                    all_stations, stations = run_once()
                    print_stations(stations)
                    if args.save is not None:
                        save_json(stations)
                    if args.heatmap:
                        write_bike_dock_heatmap(all_stations, args.heatmap)
                        print(f"Heat map saved to {args.heatmap}")
                except urllib.error.URLError as e:
                    print(f"Network error: {e}", file=sys.stderr)
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        print("Fetching Citi Bike station status...")
        all_stations, stations = run_once()
        print_stations(stations)
        save_json(stations)
        if args.heatmap:
            write_bike_dock_heatmap(all_stations, args.heatmap)
            print(f"Heat map saved to {args.heatmap}")


if __name__ == "__main__":
    main()
