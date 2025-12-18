# tools/generate_jieqi_table.py
# JIEQI_GENERATOR_VERSION=timeline_slice_v3_no_observe_rangeclamp

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

# de421 covers 1899-07-29 through 2053-10-09 (UTC) for Skyfield's segment
# We add margin so internal computations never approach edges.
EPH_MARGIN_DAYS = int(os.getenv("JIEQI_EPH_MARGIN_DAYS", "7"))

# Search resolution (coarse scan step in hours)
SCAN_STEP_HOURS = int(os.getenv("JIEQI_SCAN_STEP_HOURS", "3"))

# Root find iterations
BISECT_ITERS = int(os.getenv("JIEQI_BISECT_ITERS", "50"))

KST = timezone(timedelta(hours=9))
UTC = timezone.utc
TAU = 2.0 * math.pi

# 24 solar terms (Korean) at 15-degree ecliptic longitude increments
# Convention: 0° = 춘분, 15° = 청명, ..., 315° = 우수, 330° = 경칩, 345° = 춘분 직전 = "??"
# In East Asian 24 절기 commonly used mapping:
# 0   춘분, 15  청명, 30  곡우, 45  입하, 60  소만, 75  망종,
# 90  하지, 105 소서, 120 대서, 135 입추, 150 처서, 165 백로,
# 180 추분, 195 한로, 210 상강, 225 입동, 240 소설, 255 대설,
# 270 동지, 285 소한, 300 대한, 315 입춘, 330 우수, 345 경칩
#
# NOTE: Some sources define 0° as 춘분 indeed. This matches typical astronomical definitions.
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

NAME_BY_DEG = {deg: name for name, deg in JIEQI}
DEG_BY_NAME = {name: deg for name, deg in JIEQI}
TARGET_RADS = {deg: math.radians(deg) % TAU for _, deg in JIEQI}


@dataclass
class Event:
    name: str
    deg: int
    utc_iso: str
    kst_iso: str
    kst_year_for_group: int  # 사주 기준(입춘 기준 슬라이스)용 group year


def _dt_utc(y, m, d, hh=0, mm=0, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=UTC)


def _to_iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _kst_iso(dt_utc: datetime) -> str:
    return dt_utc.astimezone(KST).isoformat()


def _wrap_diff(a: float, b: float) -> float:
    """Return signed smallest difference a-b wrapped to (-pi, +pi]."""
    d = (a - b) % TAU
    if d > math.pi:
        d -= TAU
    return d


def sun_ecliptic_lon_rad(eph, t):
    """
    Geocentric true ecliptic longitude of the Sun (NO observe(), NO light-time).
    Using vector difference (sun - earth).at(t).
    """
    geo = (eph["sun"] - eph["earth"]).at(t)
    lat, lon, dist = geo.frame_latlon(ecliptic_frame)
    return np.deg2rad(lon.degrees) % TAU


def clamp_range(ts, eph, start_dt: datetime, end_dt: datetime):
    """
    Clamp search window inside ephemeris safe range with margins.
    """
    # Skyfield ephemeris has segments; but DE421 boundaries are known.
    # We'll hardcode safe boundaries derived from the known error message range.
    eph_min = _dt_utc(1899, 7, 29, 0, 0, 0) + timedelta(days=EPH_MARGIN_DAYS)
    eph_max = _dt_utc(2053, 10, 9, 0, 0, 0) - timedelta(days=EPH_MARGIN_DAYS)

    if start_dt < eph_min:
        start_dt = eph_min
    if end_dt > eph_max:
        end_dt = eph_max
    if start_dt >= end_dt:
        raise RuntimeError(f"Clamped range invalid: {start_dt} >= {end_dt}")
    return start_dt, end_dt


def find_crossings_for_deg(ts, eph, target_rad: float, start_dt: datetime, end_dt: datetime):
    """
    Find times when Sun ecliptic longitude crosses target_rad within [start_dt, end_dt).
    Coarse scan + bisection on sign change of wrapped difference.
    """
    step = timedelta(hours=SCAN_STEP_HOURS)

    # build coarse times list
    dts = []
    cur = start_dt
    while cur < end_dt:
        dts.append(cur)
        cur += step
    if dts[-1] < end_dt:
        dts.append(end_dt)

    times = ts.from_datetimes(dts)
    lons = sun_ecliptic_lon_rad(eph, times)

    # signed diff to target in (-pi, pi]
    diffs = np.array([_wrap_diff(float(lon), target_rad) for lon in lons], dtype=np.float64)

    hits = []
    for i in range(len(dts) - 1):
        d0 = diffs[i]
        d1 = diffs[i + 1]

        # exact hit (rare)
        if d0 == 0.0:
            hits.append(dts[i])
            continue

        # sign change indicates crossing
        if (d0 < 0 and d1 > 0) or (d0 > 0 and d1 < 0):
            t0 = dts[i]
            t1 = dts[i + 1]
            # bisection on scalar time
            for _ in range(BISECT_ITERS):
                mid = t0 + (t1 - t0) / 2
                lon_mid = float(sun_ecliptic_lon_rad(eph, ts.from_datetime(mid)))
                d_mid = _wrap_diff(lon_mid, target_rad)
                if d_mid == 0.0:
                    t0 = t1 = mid
                    break
                # keep interval that contains sign change
                # evaluate at t0
                lon_t0 = float(sun_ecliptic_lon_rad(eph, ts.from_datetime(t0)))
                d_t0 = _wrap_diff(lon_t0, target_rad)
                if (d_t0 < 0 and d_mid > 0) or (d_t0 > 0 and d_mid < 0):
                    t1 = mid
                else:
                    t0 = mid
            hits.append(t0 + (t1 - t0) / 2)

    return hits


def collect_events_timeline(ts, eph, year: int):
    """
    Collect events across a 2-year window around 'year', then slice by 입춘 to produce year-based 24 terms.
    Strategy:
      - Window: [year-1-03-01, year+1+03-01) (wide enough)
      - Clamp to ephemeris safe range
      - Find all crossings for all 24 targets
      - Deduplicate by KST timestamp string
      - Sort by UTC time
      - Then create year->24 slice using Ipchun (입춘, 315°)
    """
    # wide scan window to avoid missing boundary terms
    start_dt = _dt_utc(year - 1, 3, 1)
    end_dt   = _dt_utc(year + 1, 3, 1)

    start_dt, end_dt = clamp_range(ts, eph, start_dt, end_dt)

    raw = []
    for name, deg in JIEQI:
        hits = find_crossings_for_deg(ts, eph, TARGET_RADS[deg], start_dt, end_dt)
        for dt_hit in hits:
            raw.append((dt_hit, name, deg))

    # sort
    raw.sort(key=lambda x: x[0])

    # dedupe by KST timestamp string (seconds precision)
    seen = set()
    dedup = []
    for dt_hit, name, deg in raw:
        kst = dt_hit.astimezone(KST).replace(microsecond=0)
        key = (kst.isoformat(), deg)
        # allow same moment different deg? should not happen; but we also guard with deg
        if key in seen:
            continue
        seen.add(key)
        dedup.append((dt_hit.replace(microsecond=0), name, deg))

    return dedup


def slice_24_from_ipchun(events, target_year: int):
    """
    From timeline events list (sorted), pick 24 consecutive terms starting at the Ipchun(입춘) that belongs to target_year.
    Rule:
      - Find 입춘 events (deg=315) whose KST year == target_year
      - Take that idx as start, then take next 24 events (including 입춘)
    """
    ipchun_deg = 315

    idx = None
    for i, (dt_utc, name, deg) in enumerate(events):
        if deg != ipchun_deg:
            continue
        kst_year = dt_utc.astimezone(KST).year
        if kst_year == target_year:
            idx = i
            break

    if idx is None:
        return None

    slice_events = events[idx: idx + 24]
    if len(slice_events) != 24:
        return None

    # validate uniqueness of deg in slice (should be exactly 24 distinct)
    degs = [deg for _, _, deg in slice_events]
    if len(set(degs)) != 24:
        return None

    # enforce order exactly as time order (already)
    out = []
    for dt_utc, name, deg in slice_events:
        out.append({
            "name": name,
            "deg": deg,
            "utc": _to_iso(dt_utc.astimezone(UTC)),
            "kst": _kst_iso(dt_utc.astimezone(UTC)),
        })
    return out


def generate():
    print("JIEQI_GENERATOR_VERSION=timeline_slice_v3_no_observe_rangeclamp")
    ts = load.timescale()
    eph = load("de421.bsp")

    result = {}
    bad_years = []

    for y in range(START_YEAR, END_YEAR + 1):
        timeline = collect_events_timeline(ts, eph, y)
        sliced = slice_24_from_ipchun(timeline, y)

        if sliced is None:
            bad_years.append(y)
            print(f"[WARN] {y} -> slice failed (timeline={len(timeline)})")
            # still store empty so keys exist
            result[str(y)] = []
            continue

        # final sanity: sort by kst
        sliced.sort(key=lambda x: x["kst"])
        result[str(y)] = sliced

        if len(sliced) != 24:
            bad_years.append(y)
            print(f"[WARN] {y} -> {len(sliced)} items (expected 24)")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    if bad_years:
        print(f"[SUMMARY] bad_years_count={len(bad_years)}")
        # fail the action so you notice
        raise SystemExit(1)

    print("[OK] all years have 24 items")


if __name__ == "__main__":
    generate()
