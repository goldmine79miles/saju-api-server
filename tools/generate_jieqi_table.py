# tools/generate_jieqi_table.py
# JIEQI_GENERATOR_VERSION=skyfield_almanac_discrete_v1

import json
import os
from datetime import datetime, timezone, timedelta

from skyfield.api import load, load_file
from skyfield.framelib import ecliptic_frame
from skyfield import almanac

# -----------------------------
# Config (env)
# -----------------------------
START_YEAR = int(os.getenv("JIEQI_START_YEAR", "1900"))
END_YEAR = int(os.getenv("JIEQI_END_YEAR", "2052"))
OUTPUT_PATH = os.getenv("JIEQI_OUTPUT", os.path.join("data", "jieqi_1900_2052.json"))
APPEND = os.getenv("JIEQI_APPEND", "true").lower() in ("1", "true", "yes", "y")

KST = timezone(timedelta(hours=9))

# -----------------------------
# 24 Solar Terms (ecliptic longitude degrees)
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

NAME_BY_DEG = {deg: name for name, deg in JIEQI_24}
ALL_DEGS = sorted(NAME_BY_DEG.keys())  # [0,15,...,345]

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


def _sun_ecl_lon_deg(eph, t) -> float:
    """Sun apparent ecliptic longitude in degrees [0,360)."""
    e = eph["earth"].at(t)
    s = e.observe(eph["sun"]).apparent()
    lon, lat, dist = s.frame_latlon(ecliptic_frame)
    return float(lon.degrees % 360.0)


def _deg_round_to_15(lon_deg: float) -> int:
    """Round longitude to nearest 15-degree boundary."""
    # e.g. 359.999 -> 0, 0.001 -> 0, 14.999 -> 15
    k = int(round(lon_deg / 15.0)) % 24
    return int((k * 15) % 360)


def _build_term_index_func(eph):
    """
    Discrete function for almanac.find_discrete:
    returns which 15-degree "sector" the sun is in: 0..23
    sector changes exactly at each 15-degree boundary.
    """
    def term_index(t):
        lon = _sun_ecl_lon_deg(eph, t)
        return int(lon // 15.0)  # 0..23
    return term_index


def generate_year(eph, ts, year: int):
    # Search within [Jan 1, Jan 1 next year)
    t0 = ts.utc(year, 1, 1, 0, 0, 0)
    t1 = ts.utc(year + 1, 1, 1, 0, 0, 0)

    f = _build_term_index_func(eph)

    # Find all sector changes (there should be 24 changes per year)
    times, values = almanac.find_discrete(t0, t1, f)

    hits = {}  # degree -> hit_time(UTC datetime)

    for t in times:
        lon = _sun_ecl_lon_deg(eph, t)
        deg = _deg_round_to_15(lon)

        # Only keep degrees that are real jieqi targets (all are multiples of 15)
        if deg not in NAME_BY_DEG:
            continue

        # keep first occurrence in the interval
        if deg not in hits:
            hits[deg] = t.utc_datetime().replace(tzinfo=timezone.utc)

    # We expect all 24 degrees to appear in a year interval.
    # If something is missing (edge numeric case), widen by 1 day and retry once.
    if len(hits) != 24:
        t0w = ts.utc((datetime(year, 1, 1, tzinfo=timezone.utc) - timedelta(days=1)))
        t1w = ts.utc((datetime(year + 1, 1, 1, tzinfo=timezone.utc) + timedelta(days=1)))
        times2, _ = almanac.find_discrete(t0w, t1w, f)
        for t in times2:
            dt = t.utc_datetime().replace(tzinfo=timezone.utc)
            if not (datetime(year, 1, 1, tzinfo=timezone.utc) <= dt < datetime(year + 1, 1, 1, tzinfo=timezone.utc)):
                continue
            lon = _sun_ecl_lon_deg(eph, t)
            deg = _deg_round_to_15(lon)
            if deg in NAME_BY_DEG and deg not in hits:
                hits[deg] = dt

    if len(hits) != 24:
        missing = [d for d in ALL_DEGS if d not in hits]
        raise RuntimeError(f"year={year} missing terms: {missing} (got {len(hits)}/24)")

    # Output in the project’s preferred order (소한..동지)
    results = []
    for name, deg in JIEQI_24:
        utc_dt = hits[deg]
        kst_dt = utc_dt.astimezone(KST)
        results.append({
            "name": name,
            "degree": int(deg),
            "utc": utc_dt.isoformat().replace("+00:00", "Z"),
            "kst": kst_dt.isoformat(),
        })

    return results


def generate():
    print(f"[JIEQI] OUTPUT_PATH={OUTPUT_PATH}", flush=True)
    print(f"[JIEQI] RANGE={START_YEAR}..{END_YEAR} APPEND={APPEND}", flush=True)

    # Load ephemeris (prefer local file)
    if os.path.exists("de421.bsp"):
        eph = load_file("de421.bsp")
    else:
        root_bsp = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "de421.bsp"))
        eph = load_file(root_bsp) if os.path.exists(root_bsp) else load("de421.bsp")

    ts = load.timescale()

    existing = _load_existing(OUTPUT_PATH)
    result = dict(existing) if isinstance(existing, dict) else {}

    bad_years = []
    total = END_YEAR - START_YEAR + 1
    for i, y in enumerate(range(START_YEAR, END_YEAR + 1), start=1):
        print(f"[JIEQI] processing year {y} ({i}/{total})", flush=True)
        try:
            result[str(y)] = generate_year(eph, ts, y)
            print(f"[JIEQI] year {y} ok (24 items)", flush=True)
        except Exception as e:
            print(f"[JIEQI][ERR] year {y} failed: {e}", flush=True)
            bad_years.append(y)

        _ensure_dir(OUTPUT_PATH)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

    if bad_years:
        print(f"[SUMMARY] bad_years_count={len(bad_years)} years={bad_years[:20]}{'...' if len(bad_years)>20 else ''}", flush=True)
        raise SystemExit(1)

    print("[OK] all years have 24 items", flush=True)


if __name__ == "__main__":
    generate()
    print("[JIEQI] generator script reached END", flush=True)
