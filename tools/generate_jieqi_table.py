# tools/generate_jieqi_table.py
# JIEQI_GENERATOR_VERSION=skyfield_root_finding_final

import json
import os
from datetime import datetime, timedelta, timezone

from skyfield.api import load, load_file
from skyfield.framelib import ecliptic_frame
import numpy as np

# -----------------------------
# Config
# -----------------------------
START_YEAR = int(os.getenv("JIEQI_START_YEAR", "1900"))
END_YEAR = int(os.getenv("JIEQI_END_YEAR", "2052"))
OUTPUT_PATH = os.getenv("JIEQI_OUTPUT", os.path.join("data", "jieqi_1900_2052.json"))
APPEND = os.getenv("JIEQI_APPEND", "true").lower() in ("1", "true", "yes", "y")

KST = timezone(timedelta(hours=9))

JIEQI_24 = [
    ("소한", 285), ("대한", 300), ("입춘", 315), ("우수", 330), ("경칩", 345),
    ("춘분", 0), ("청명", 15), ("곡우", 30), ("입하", 45), ("소만", 60),
    ("망종", 75), ("하지", 90), ("소서", 105), ("대서", 120), ("입추", 135),
    ("처서", 150), ("백로", 165), ("추분", 180), ("한로", 195), ("상강", 210),
    ("입동", 225), ("소설", 240), ("대설", 255), ("동지", 270),
]

# -----------------------------
# Utilities
# -----------------------------
def _ensure_dir(path):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _load_existing(path):
    if not (APPEND and os.path.exists(path)):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def sun_lon_deg(eph, ts, t):
    e = eph["earth"].at(t)
    s = e.observe(eph["sun"]).apparent()
    lon, _, _ = s.frame_latlon(ecliptic_frame)
    return lon.degrees % 360.0


def find_crossing(eph, ts, target_deg, t0, t1):
    """Binary search where sun longitude crosses target_deg."""
    def f(t):
        v = sun_lon_deg(eph, ts, t) - target_deg
        return (v + 540) % 360 - 180  # wrap to [-180,180]

    lo, hi = t0, t1
    for _ in range(60):
        mid = ts.from_datetime(
            lo.utc_datetime() + (hi.utc_datetime() - lo.utc_datetime()) / 2
        )
        if f(lo) * f(mid) <= 0:
            hi = mid
        else:
            lo = mid
    return hi


# -----------------------------
# Core
# -----------------------------
def generate_year(eph, ts, year):
    results = []

    # Search the whole year safely
    t_start = ts.utc(year, 1, 1)
    t_end = ts.utc(year + 1, 1, 1)

    # Sample daily to bracket crossings
    days = np.arange(0, 366)
    times = ts.utc(year, 1, 1 + days)

    lons = [sun_lon_deg(eph, ts, t) for t in times]

    for name, deg in JIEQI_24:
        hit = None
        for i in range(len(times) - 1):
            a = (lons[i] - deg + 540) % 360 - 180
            b = (lons[i + 1] - deg + 540) % 360 - 180
            if a == 0 or a * b < 0:
                hit = find_crossing(eph, ts, deg, times[i], times[i + 1])
                break

        if hit is None:
            raise RuntimeError(f"{year} {name} not found")

        utc_dt = hit.utc_datetime().replace(tzinfo=timezone.utc)
        kst_dt = utc_dt.astimezone(KST)

        results.append({
            "name": name,
            "degree": deg,
            "utc": utc_dt.isoformat().replace("+00:00", "Z"),
            "kst": kst_dt.isoformat(),
        })

    return results


def generate():
    if os.path.exists("de421.bsp"):
        eph = load_file("de421.bsp")
    else:
        eph = load("de421.bsp")

    ts = load.timescale()
    data = _load_existing(OUTPUT_PATH)

    for year in range(START_YEAR, END_YEAR + 1):
        print(f"[JIEQI] year {year}", flush=True)
        data[str(year)] = generate_year(eph, ts, year)
        _ensure_dir(OUTPUT_PATH)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    print("[OK] jieqi generation complete", flush=True)


if __name__ == "__main__":
    generate()
