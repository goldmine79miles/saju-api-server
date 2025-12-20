# tools/generate_jieqi_table.py
# JIEQI_GENERATOR_VERSION=official_skyfield_exact

import json
import os
from datetime import datetime, timezone, timedelta

from skyfield.api import load, load_file
from skyfield.framelib import ecliptic_frame

# -----------------------------
# Config (env)
# -----------------------------
START_YEAR = int(os.getenv("JIEQI_START_YEAR", "1900"))
END_YEAR = int(os.getenv("JIEQI_END_YEAR", "2052"))
OUTPUT_PATH = os.getenv("JIEQI_OUTPUT", os.path.join("data", "jieqi_1900_2052.json"))
APPEND = os.getenv("JIEQI_APPEND", "true").lower() in ("1", "true", "yes", "y")

KST = timezone(timedelta(hours=9))

# -----------------------------
# 24 Solar Terms (degrees)
# -----------------------------
JIEQI_24 = [
    ("소한", 285),
    ("대한", 300),
    ("입춘", 315),
    ("우수", 330),
    ("경칩", 345),
    ("춘분", 0),
    ("청명", 15),
    ("곡우", 30),
    ("입하", 45),
    ("소만", 60),
    ("망종", 75),
    ("하지", 90),
    ("소서", 105),
    ("대서", 120),
    ("입추", 135),
    ("처서", 150),
    ("백로", 165),
    ("추분", 180),
    ("한로", 195),
    ("상강", 210),
    ("입동", 225),
    ("소설", 240),
    ("대설", 255),
    ("동지", 270),
]

# -----------------------------
# Utilities
# -----------------------------
def _ensure_dir(path: str):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _load_existing(path: str) -> dict:
    if not (APPEND and os.path.exists(path)):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def sun_ecliptic_longitude_deg(eph, ts, t):
    earth = eph["earth"]
    sun = eph["sun"]
    astrometric = earth.at(t).observe(sun)
    lon, _, _ = astrometric.apparent().frame_latlon(ecliptic_frame)
    return lon.degrees % 360.0


def find_exact_time(eph, ts, target_deg, t_start, t_end):
    """
    Find exact time when sun's ecliptic longitude == target_deg
    using binary search (monotonic in short interval).
    """
    lo = t_start
    hi = t_end

    for _ in range(60):
        mid = ts.from_datetime(
            lo.utc_datetime() + (hi.utc_datetime() - lo.utc_datetime()) / 2
        )
        lon = sun_ecliptic_longitude_deg(eph, ts, mid)

        # normalize comparison
        diff = (lon - target_deg + 540) % 360 - 180
        if diff < 0:
            lo = mid
        else:
            hi = mid

    return hi


# -----------------------------
# Core
# -----------------------------
def generate_year(eph, ts, year: int):
    results = []

    for name, degree in JIEQI_24:
        # search window: ±3 days around expected date
        approx_day = int(((degree % 360) / 360) * 365)
        base = ts.utc(year, 1, 1 + approx_day)

        t_start = ts.utc(base.utc_datetime() - timedelta(days=3))
        t_end = ts.utc(base.utc_datetime() + timedelta(days=3))

        hit = find_exact_time(eph, ts, degree, t_start, t_end)

        utc_dt = hit.utc_datetime().replace(tzinfo=timezone.utc)
        kst_dt = utc_dt.astimezone(KST)

        results.append({
            "name": name,
            "degree": degree,
            "utc": utc_dt.isoformat().replace("+00:00", "Z"),
            "kst": kst_dt.isoformat(),
        })

    if len(results) != 24:
        raise RuntimeError(f"{year}: expected 24 jieqi, got {len(results)}")

    return results


def generate():
    print(f"[JIEQI] RANGE={START_YEAR}..{END_YEAR}", flush=True)

    # load ephemeris
    if os.path.exists("de421.bsp"):
        eph = load_file("de421.bsp")
    else:
        eph = load("de421.bsp")

    ts = load.timescale()

    existing = _load_existing(OUTPUT_PATH)
    result = dict(existing)

    for year in range(START_YEAR, END_YEAR + 1):
        print(f"[JIEQI] year {year}", flush=True)
        result[str(year)] = generate_year(eph, ts, year)

        _ensure_dir(OUTPUT_PATH)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

    print("[OK] jieqi generation complete", flush=True)


if __name__ == "__main__":
    generate()
