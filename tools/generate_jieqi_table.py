# tools/generate_jieqi_table.py
# JIEQI_GENERATOR_VERSION=independent_24terms_v1_kst_year_filter

import json
import math
import os
from datetime import datetime, timedelta, timezone

import numpy as np
from skyfield.api import load
from skyfield.framelib import ecliptic_frame

# -----------------------------
# Config
# -----------------------------
START_YEAR = int(os.getenv("JIEQI_START_YEAR", "1900"))
END_YEAR   = int(os.getenv("JIEQI_END_YEAR", "2052"))  # inclusive
OUTPUT_PATH = os.getenv("JIEQI_OUTPUT", "data/jieqi_1900_2052.json")

# de421 safe margin (avoid ephemeris edges)
EPH_MARGIN_DAYS = int(os.getenv("JIEQI_EPH_MARGIN_DAYS", "7"))

# Speed knobs
SCAN_STEP_HOURS = int(os.getenv("JIEQI_SCAN_STEP_HOURS", "6"))
BISECT_ITERS    = int(os.getenv("JIEQI_BISECT_ITERS", "32"))

# Search window around a Gregorian year, then select by KST year
YEAR_PAD_DAYS = int(os.getenv("JIEQI_YEAR_PAD_DAYS", "3"))

KST = timezone(timedelta(hours=9))
UTC = timezone.utc
TAU = 2.0 * math.pi

# 24 solar terms (Korean) at 15-degree increments
# NOTE: This list is fine; we will compute each term independently.
JIEQI = [
    ("춘분",   0),
    ("청명",  15),
    ("곡우",  30),
    ("입하",  45),
    ("소만",  60),
    ("망종",  75),
    ("하지",  90),
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
    ("소한", 285),
    ("대한", 300),
    ("입춘", 315),
    ("우수", 330),
    ("경칩", 345),
]

TARGET_RADS = {deg: math.radians(deg) % TAU for _, deg in JIEQI}


def _dt_utc(y, m, d, hh=0, mm=0, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=UTC)


def _to_iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _kst_iso(dt_utc: datetime) -> str:
    return dt_utc.astimezone(KST).isoformat()


def _wrap_diff(a: float, b: float) -> float:
    """Signed smallest difference a-b wrapped to (-pi, +pi]."""
    d = (a - b) % TAU
    if d > math.pi:
        d -= TAU
    return d


def clamp_range(start_dt: datetime, end_dt: datetime):
    """
    Clamp search window inside DE421 safe range with margins.
    de421 known coverage: 1899-07-29 through 2053-10-09 (UTC)
    """
    eph_min = _dt_utc(1899, 7, 29) + timedelta(days=EPH_MARGIN_DAYS)
    eph_max = _dt_utc(2053, 10, 9) - timedelta(days=EPH_MARGIN_DAYS)

    if start_dt < eph_min:
        start_dt = eph_min
    if end_dt > eph_max:
        end_dt = eph_max
    if start_dt >= end_dt:
        raise RuntimeError(f"Clamped range invalid: {start_dt} >= {end_dt}")
    return start_dt, end_dt


def sun_ecliptic_lon_rad(eph, t):
    """
    Geocentric ecliptic longitude of the Sun (NO observe(), NO light-time).
    """
    geo = (eph["sun"] - eph["earth"]).at(t)
    lat, lon, dist = geo.frame_latlon(ecliptic_frame)
    return np.deg2rad(lon.degrees) % TAU


def find_crossings_for_deg(ts, eph, target_rad: float, start_dt: datetime, end_dt: datetime):
    """
    Coarse scan + bisection on sign change of wrapped diff.
    Returns list of UTC datetimes.
    """
    step = timedelta(hours=SCAN_STEP_HOURS)

    start_dt, end_dt = clamp_range(start_dt, end_dt)

    dts = []
    cur = start_dt
    while cur < end_dt:
        dts.append(cur)
        cur += step
    if dts[-1] < end_dt:
        dts.append(end_dt)

    times = ts.from_datetimes(dts)
    lons = sun_ecliptic_lon_rad(eph, times)
    diffs = np.array([_wrap_diff(float(lon), target_rad) for lon in lons], dtype=np.float64)

    hits = []
    for i in range(len(dts) - 1):
        d0 = diffs[i]
        d1 = diffs[i + 1]

        if d0 == 0.0:
            hits.append(dts[i])
            continue

        if (d0 < 0 and d1 > 0) or (d0 > 0 and d1 < 0):
            t0 = dts[i]
            t1 = dts[i + 1]

            # bisection (scalar)
            for _ in range(BISECT_ITERS):
                mid = t0 + (t1 - t0) / 2
                lon_mid = float(sun_ecliptic_lon_rad(eph, ts.from_datetime(mid)))
                d_mid = _wrap_diff(lon_mid, target_rad)

                if d_mid == 0.0:
                    t0 = t1 = mid
                    break

                lon_t0 = float(sun_ecliptic_lon_rad(eph, ts.from_datetime(t0)))
                d_t0 = _wrap_diff(lon_t0, target_rad)

                if (d_t0 < 0 and d_mid > 0) or (d_t0 > 0 and d_mid < 0):
                    t1 = mid
                else:
                    t0 = mid

            hits.append(t0 + (t1 - t0) / 2)

    # normalize seconds precision
    out = []
    for dt_hit in hits:
        out.append(dt_hit.replace(microsecond=0))
    return out


def _pick_hit_for_kst_year(hits, year: int):
    """
    Choose the hit whose KST year matches `year`.
    If multiple, pick the earliest.
    """
    candidates = [h for h in hits if h.astimezone(KST).year == year]
    candidates.sort()
    if candidates:
        return candidates[0]
    return None


def generate():
    print("JIEQI_GENERATOR_VERSION=independent_24terms_v1_kst_year_filter")
    ts = load.timescale()
    eph = load("de421.bsp")

    result = {}
    bad_years = []

    for y in range(START_YEAR, END_YEAR + 1):
        print(f"[JIEQI] processing year {y}", flush=True)
        
        # Search window: [Jan 1 - pad, next Jan 1 + pad], then select by KST year
        start_dt = _dt_utc(y, 1, 1) - timedelta(days=YEAR_PAD_DAYS)
        end_dt   = _dt_utc(y + 1, 1, 1) + timedelta(days=YEAR_PAD_DAYS)
        start_dt, end_dt = clamp_range(start_dt, end_dt)

        year_items = []
        ok = True

        for name, deg in JIEQI:
            hits = find_crossings_for_deg(ts, eph, TARGET_RADS[deg], start_dt, end_dt)
            picked = _pick_hit_for_kst_year(hits, y)

            if picked is None:
                # one more fallback: widen by a week (rare KST boundary weirdness)
                s2 = start_dt - timedelta(days=7)
                e2 = end_dt + timedelta(days=7)
                s2, e2 = clamp_range(s2, e2)
                hits2 = find_crossings_for_deg(ts, eph, TARGET_RADS[deg], s2, e2)
                picked = _pick_hit_for_kst_year(hits2, y)

            if picked is None:
                ok = False
                print(f"[WARN] {y} -> term missing: {name}({deg}) hits={len(hits)}")
                continue

            year_items.append({
                "name": name,
                "deg": deg,
                "utc": _to_iso(picked),
                "kst": _kst_iso(picked),
            })

        # Validate: must be exactly 24 unique degrees and times must be in KST year y
        if ok and len(year_items) == 24 and len({it["deg"] for it in year_items}) == 24:
            # sort by time
            year_items.sort(key=lambda x: x["kst"])

            # sanity: every item belongs to the requested KST year
            for it in year_items:
                kst_year = datetime.fromisoformat(it["kst"]).year
                if kst_year != y:
                    ok = False
                    print(f"[WARN] {y} -> KST year mismatch: {it['name']} kst={it['kst']}")
                    break

        if not ok or len(year_items) != 24:
            bad_years.append(y)
            result[str(y)] = []
            continue

        result[str(y)] = year_items

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    if bad_years:
        print(f"[SUMMARY] bad_years_count={len(bad_years)} years={bad_years[:20]}{'...' if len(bad_years)>20 else ''}")
        raise SystemExit(1)

    print("[OK] all years have 24 items")


if __name__ == "__main__":
    generate()
    print("[JIEQI] generator script reached END", flush=True)
