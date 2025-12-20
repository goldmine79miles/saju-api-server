# tools/generate_jieqi_table.py
# JIEQI_GENERATOR_VERSION=chunkable_append_v2

import json
import os
from datetime import datetime, timedelta, timezone

import numpy as np
from skyfield.api import load, load_file
from skyfield.framelib import ecliptic_frame

# -----------------------------
# Config (env)
# -----------------------------
START_YEAR = int(os.getenv("JIEQI_START_YEAR", "1900"))
END_YEAR = int(os.getenv("JIEQI_END_YEAR", "2052"))

# main.py에서 env["JIEQI_OUTPUT"]로 넘겨줌. 없으면 기본 경로.
OUTPUT_PATH = os.getenv("JIEQI_OUTPUT", os.path.join("data", "jieqi_1900_2052.json"))

# ✅ append 모드 (기본 true): 기존 JSON이 있으면 읽어서, 해당 연도만 갱신/추가
APPEND = os.getenv("JIEQI_APPEND", "true").lower() in ("1", "true", "yes", "y")

# 샘플링 간격(시간). 줄이면 더 정확하지만 느려짐. 6시간이 실무적으로 충분.
STEP_HOURS = int(os.getenv("JIEQI_STEP_HOURS", "6"))

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


def _sun_ecl_lon_deg(eph, ts, dt_utc: datetime) -> float:
    """Sun ecliptic longitude in degrees [0,360)."""
    t = ts.from_datetime(dt_utc)
    e = eph["earth"].at(t)
    s = e.observe(eph["sun"]).apparent()
    lon, lat, dist = s.frame_latlon(ecliptic_frame)
    deg = lon.degrees % 360.0
    return float(deg)


def _unwrap_degrees(deg_series):
    """Unwrap circular degrees to monotonic increasing (allow >360)."""
    out = [float(deg_series[0])]
    for i in range(1, len(deg_series)):
        prev = out[-1]
        cur = float(deg_series[i])
        # if it wrapped (e.g., 359 -> 1), add 360
        if cur < (prev - 180.0):
            cur += 360.0
        # if weird jump backward even without wrap, still force monotonic by adding 360
        while cur < prev:
            cur += 360.0
        out.append(cur)
    return out


def _binary_search_crossing(eph, ts, t0: datetime, t1: datetime, target_unwrapped: float, base_wrap: float):
    """
    Find time where unwrapped lon hits target_unwrapped between t0,t1.
    base_wrap: reference for wrapping/unwrapping inside the window.
    """
    lo = t0
    hi = t1

    # helper: compute "unwrapped" lon near this window
    def lon_unwrapped(dt_utc):
        lon = _sun_ecl_lon_deg(eph, ts, dt_utc)
        # unwrap around base_wrap
        v = lon
        while v < base_wrap:
            v += 360.0
        return v

    # do 50 iterations, enough precision
    for _ in range(50):
        mid = lo + (hi - lo) / 2
        v = lon_unwrapped(mid)
        if v < target_unwrapped:
            lo = mid
        else:
            hi = mid

    return hi


def generate_year(eph, ts, year: int):
    """
    Generate 24 jieqi for a given year.
    Strategy:
    - Sample sun ecliptic longitude every STEP_HOURS from Jan 1 to Jan 1 next year
    - Unwrap to monotonic
    - For each target degree, find bracket and refine with binary search
    """
    start = datetime(year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    step = timedelta(hours=STEP_HOURS)
    times = []
    cur = start
    while cur <= end:
        times.append(cur)
        cur += step

    # compute lon degrees
    lons = [_sun_ecl_lon_deg(eph, ts, dt) for dt in times]
    lons_u = _unwrap_degrees(lons)

    # ensure targets are reached within this year span
    results = []

    # base for local unwrapping near bracket (use the left bracket lon)
    for name, target in JIEQI_24:
        # lift target into the unwrapped space
        target_u = float(target)
        while target_u < lons_u[0]:
            target_u += 360.0

        # find bracket index
        idx = None
        for i in range(len(lons_u) - 1):
            if lons_u[i] <= target_u <= lons_u[i + 1]:
                idx = i
                break

        if idx is None:
            raise RuntimeError(f"failed to bracket term {name}({target}) for year={year}")

        t0 = times[idx]
        t1 = times[idx + 1]
        base_wrap = lons_u[idx]

        hit = _binary_search_crossing(eph, ts, t0, t1, target_u, base_wrap)

        utc_iso = hit.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        kst_iso = hit.astimezone(KST).isoformat()

        results.append({
            "name": name,
            "degree": int(target),
            "utc": utc_iso,
            "kst": kst_iso,
        })

    # sanity: must be 24 items
    if len(results) != 24:
        raise RuntimeError(f"year={year} got {len(results)} items (expected 24)")
    return results


def generate():
    print(f"[JIEQI] OUTPUT_PATH={OUTPUT_PATH}", flush=True)
    print(f"[JIEQI] RANGE={START_YEAR}..{END_YEAR} APPEND={APPEND} STEP_HOURS={STEP_HOURS}", flush=True)

    # load ephemeris
    # ✅ de421.bsp는 레포 루트에 있다고 가정 (/app/de421.bsp)
    # 없으면 load('de421.bsp')가 다운로드를 시도할 수 있으니, 반드시 load_file 우선.
    eph = None
    if os.path.exists("de421.bsp"):
        eph = load_file("de421.bsp")
    else:
        # fallback (혹시 다른 경로로 들어갔을 때)
        root_bsp = os.path.join(os.path.dirname(__file__), "..", "de421.bsp")
        root_bsp = os.path.normpath(root_bsp)
        if os.path.exists(root_bsp):
            eph = load_file(root_bsp)
        else:
            # 최후 fallback (환경에 따라 이미 캐시돼있을 수도 있음)
            eph = load("de421.bsp")

    ts = load.timescale()

    existing = _load_existing(OUTPUT_PATH)
    result = dict(existing) if isinstance(existing, dict) else {}

    bad_years = []
    total = END_YEAR - START_YEAR + 1
    for i, y in enumerate(range(START_YEAR, END_YEAR + 1), start=1):
        print(f"[JIEQI] processing year {y} ({i}/{total})", flush=True)
        try:
            items = generate_year(eph, ts, y)
            result[str(y)] = items
            print(f"[JIEQI] year {y} ok (24 items)", flush=True)
        except Exception as e:
            print(f"[JIEQI][ERR] year {y} failed: {e}", flush=True)
            bad_years.append(y)

        # ✅ 매년 저장(중간에 죽어도 누적 결과 남김)
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
