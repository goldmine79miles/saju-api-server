# tools/generate_jieqi_table.py
# JIEQI_GENERATOR_VERSION=timeline_slice_v4_fast_ipchun_anchor_no_observe

import json
import math
import os
from dataclasses import dataclass
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
SCAN_STEP_HOURS = int(os.getenv("JIEQI_SCAN_STEP_HOURS", "6"))   # was 3
BISECT_ITERS    = int(os.getenv("JIEQI_BISECT_ITERS", "30"))     # was 50

# Window policy (fast)
# 1) Find Ipchun in a tight window each year
IPCHUN_SEARCH_START = (2, 1)   # Feb 1
IPCHUN_SEARCH_END   = (3, 1)   # Mar 1  (exclusive-ish)
# 2) After Ipchun found, only search events in [ipchun - pad, ipchun + horizon]
POST_IPCHUN_PAD_DAYS = int(os.getenv("JIEQI_POST_IPCHUN_PAD_DAYS", "2"))
POST_IPCHUN_HORIZON_DAYS = int(os.getenv("JIEQI_POST_IPCHUN_HORIZON_DAYS", "400"))

KST = timezone(timedelta(hours=9))
UTC = timezone.utc
TAU = 2.0 * math.pi

# 24 solar terms (Korean) at 15-degree increments
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

IPCHUN_DEG = 315
IPCHUN_RAD = TARGET_RADS[IPCHUN_DEG]


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


def find_ipchun_for_year(ts, eph, year: int):
    """
    Find Ipchun (315°) for target year using tight window around early Feb.
    Choose the first hit whose KST year == year (safety).
    """
    s_m, s_d = IPCHUN_SEARCH_START
    e_m, e_d = IPCHUN_SEARCH_END

    start_dt = _dt_utc(year, s_m, s_d)
    end_dt   = _dt_utc(year, e_m, e_d)

    hits = find_crossings_for_deg(ts, eph, IPCHUN_RAD, start_dt, end_dt)

    # pick hit that matches KST year
    for h in hits:
        if h.astimezone(KST).year == year:
            return h

    # fallback: if only one hit exists, return it
    if len(hits) == 1:
        return hits[0]

    return None


def build_year_events_from_ipchun(ts, eph, ipchun_dt_utc: datetime):
    """
    After ipchun, search only small window: [ipchun - pad, ipchun + horizon]
    Collect all 24 term crossings in that window.
    """
    start_dt = ipchun_dt_utc - timedelta(days=POST_IPCHUN_PAD_DAYS)
    end_dt   = ipchun_dt_utc + timedelta(days=POST_IPCHUN_HORIZON_DAYS)

    start_dt, end_dt = clamp_range(start_dt, end_dt)

    raw = []
    for name, deg in JIEQI:
        hits = find_crossings_for_deg(ts, eph, TARGET_RADS[deg], start_dt, end_dt)
        for dt_hit in hits:
            raw.append((dt_hit, name, deg))

    # sort by time
    raw.sort(key=lambda x: x[0])

    # dedupe exact duplicates (time+deg)
    seen = set()
    dedup = []
    for dt_hit, name, deg in raw:
        key = (dt_hit.isoformat(), deg)
        if key in seen:
            continue
        seen.add(key)
        dedup.append((dt_hit, name, deg))

    return dedup


def slice_24_from_ipchun(events, ipchun_dt_utc: datetime):
    """
    In the events list, find the ipchun nearest to ipchun_dt_utc (same deg),
    then take 24 consecutive events starting there.
    """
    # find index of ipchun event closest to anchor
    best_i = None
    best_abs = None
    for i, (dt_utc, name, deg) in enumerate(events):
        if deg != IPCHUN_DEG:
            continue
        delta = abs((dt_utc - ipchun_dt_utc).total_seconds())
        if best_abs is None or delta < best_abs:
            best_abs = delta
            best_i = i

    if best_i is None:
        return None

    sliced = events[best_i: best_i + 24]
    if len(sliced) != 24:
        return None

    degs = [deg for _, _, deg in sliced]
    if len(set(degs)) != 24:
        return None

    out = []
    for dt_utc, name, deg in sliced:
        out.append({
            "name": name,
            "deg": deg,
            "utc": _to_iso(dt_utc),
            "kst": _kst_iso(dt_utc),
        })

    # sort by kst (should already be)
    out.sort(key=lambda x: x["kst"])
    return out


def generate():
    print("JIEQI_GENERATOR_VERSION=timeline_slice_v4_fast_ipchun_anchor_no_observe")
    ts = load.timescale()
    eph = load("de421.bsp")

    result = {}
    bad_years = []

    for y in range(START_YEAR, END_YEAR + 1):
        ipchun = find_ipchun_for_year(ts, eph, y)
        if ipchun is None:
            bad_years.append(y)
            print(f"[WARN] {y} -> ipchun not found")
            result[str(y)] = []
            continue

        events = build_year_events_from_ipchun(ts, eph, ipchun)
        sliced = slice_24_from_ipchun(events, ipchun)

        if sliced is None or len(sliced) != 24:
            bad_years.append(y)
            print(f"[WARN] {y} -> slice failed (events={len(events)})")
            result[str(y)] = []
            continue

        result[str(y)] = sliced

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    if bad_years:
        print(f"[SUMMARY] bad_years_count={len(bad_years)} years={bad_years[:20]}{'...' if len(bad_years)>20 else ''}")
        raise SystemExit(1)

    print("[OK] all years have 24 items")


if __name__ == "__main__":
    generate()
