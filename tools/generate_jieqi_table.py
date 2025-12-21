# tools/generate_jieqi_table.py
# JIEQI_GENERATOR_VERSION=skyfield_root_finding_final_hardcoded_de421_bounds

import json
import os
from datetime import datetime, timedelta, timezone

import numpy as np
from skyfield.api import load

# -----------------------------
# Config (env override)
# -----------------------------
START_YEAR = int(os.getenv("JIEQI_START_YEAR", "1900"))
END_YEAR = int(os.getenv("JIEQI_END_YEAR", "2052"))

OUTPUT_PATH = os.getenv("JIEQI_OUTPUT", os.path.join("data", "jieqi_1900_2052.json"))
APPEND = os.getenv("JIEQI_APPEND", "true").lower() in ("1", "true", "yes", "y")

KST = timezone(timedelta(hours=9))

# de421.bsp Skyfield error message bounds (hard-coded for compatibility)
# "ephemeris segment only covers dates 1899-07-29 through 2053-10-09"
DE421_START_UTC = (1899, 7, 29, 0, 0, 0)
DE421_END_UTC = (2053, 10, 9, 0, 0, 0)
COVERAGE_SAFETY_DAYS = 2.0  # keep away from edges (TT/TDB boundary jitter)

# 24절기: 태양 황경 기준(도)
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
# Helpers
# -----------------------------
def _ensure_parent_dir(path: str):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _load_existing(path: str) -> dict:
    if APPEND and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}


def _save_json_atomic(path: str, data: dict):
    _ensure_parent_dir(path)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _sun_ecl_lon_deg(eph, ts, dt_utc: datetime) -> float:
    """태양 황경(도). dt_utc는 tz-aware UTC datetime."""
    earth = eph["earth"]
    sun = eph["sun"]
    t = ts.from_datetime(dt_utc)
    lon = earth.at(t).observe(sun).apparent().ecliptic_latlon()[1].degrees
    return lon % 360.0


def _to_utc_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# -----------------------------
# Core: generate_year
# -----------------------------
def generate_year(eph, ts, year: int):
    """
    안정형 절기 생성기
    - 넉넉한 탐색 구간 + 6시간 샘플링 + unwrap
    - deg + 360*k 후보 전부 탐색 → 교차 브래킷 → 이진 탐색
    - KST 기준 year에 속하는 절기만 채택

    🔥 중요:
    - Codespaces/skyfield 환경에서 eph.coverage/segment 속성이 제각각이라
      de421의 커버리지 경계를 "하드코딩"해서 TT 기준으로 클램프한다.
    """
    UTC = timezone.utc

    # 넉넉한 탐색 구간
    dt0 = datetime(year - 2, 12, 1, 0, 0, tzinfo=UTC)
    dt1 = datetime(year + 1, 1, 31, 0, 0, tzinfo=UTC)

    # de421 커버리지(Time)
    eph_start_t = ts.utc(*DE421_START_UTC)
    eph_end_t = ts.utc(*DE421_END_UTC)

    # TT 기준 비교 + 안전마진(일)
    t0 = ts.from_datetime(dt0)
    t1 = ts.from_datetime(dt1)

    if t0.tt < eph_start_t.tt:
        dt0 = _to_utc_aware((eph_start_t + COVERAGE_SAFETY_DAYS).utc_datetime())
    if t1.tt > eph_end_t.tt:
        dt1 = _to_utc_aware((eph_end_t - COVERAGE_SAFETY_DAYS).utc_datetime())

    if dt0 >= dt1:
        raise RuntimeError(
            f"{year} search range invalid after clamp: dt0={dt0.isoformat()} dt1={dt1.isoformat()}"
        )

    # 6시간 샘플링
    step = timedelta(hours=6)
    dts = []
    cur = dt0
    while cur <= dt1:
        dts.append(cur)
        cur += step

    earth = eph["earth"]
    sun = eph["sun"]

    times = ts.from_datetimes(dts)
    lon = (earth.at(times).observe(sun).apparent().ecliptic_latlon()[1].degrees) % 360.0

    # unwrap: 359 -> 0 점프 제거
    lon_unwrapped = np.rad2deg(np.unwrap(np.deg2rad(lon)))
    min_lon = float(np.min(lon_unwrapped))
    max_lon = float(np.max(lon_unwrapped))

    results = []

    for name, deg in JIEQI_24:
        deg = float(deg)

        # 가능한 360*k 후보들 열거
        k_min = int(np.floor((min_lon - deg) / 360.0)) - 1
        k_max = int(np.ceil((max_lon - deg) / 360.0)) + 1

        best = None  # (kst_dt, utc_dt)

        for k in range(k_min, k_max + 1):
            target = deg + 360.0 * k
            diff = lon_unwrapped - target

            # 부호 변화 구간 찾기
            idx = None
            for i in range(len(diff) - 1):
                if diff[i] == 0:
                    idx = i
                    break
                if diff[i] * diff[i + 1] < 0:
                    idx = i
                    break
            if idx is None:
                continue

            left_dt = dts[idx]
            right_dt = dts[idx + 1]

            def f(dt: datetime) -> float:
                l0 = _sun_ecl_lon_deg(eph, ts, dt)  # 0~360
                l_cont = l0 + 360.0 * round((target - l0) / 360.0)  # target 근처 연속값
                return l_cont - target

            fl = f(left_dt)
            fr = f(right_dt)
            if fl * fr > 0:
                continue

            # 이진 탐색
            for _ in range(60):
                mid_dt = left_dt + (right_dt - left_dt) / 2
                fm = f(mid_dt)
                if fm == 0:
                    left_dt = right_dt = mid_dt
                    break
                if fl * fm <= 0:
                    right_dt = mid_dt
                    fr = fm
                else:
                    left_dt = mid_dt
                    fl = fm

            utc_dt = right_dt
            kst_dt = utc_dt.astimezone(KST)

            if kst_dt.year != year:
                continue

            if best is None or kst_dt < best[0]:
                best = (kst_dt, utc_dt)

        if best is None:
            raise RuntimeError(f"{year} {name} not found")

        kst_dt, utc_dt = best
        results.append(
            {
                "name": name,
                "degree": int(deg),
                "utc": utc_dt.isoformat().replace("+00:00", "Z"),
                "kst": kst_dt.isoformat(),
            }
        )

    results.sort(key=lambda x: x["utc"])
    return results


# -----------------------------
# Main generate loop
# -----------------------------
def generate():
    print(f"[JIEQI] output={OUTPUT_PATH} append={APPEND}", flush=True)
    print(f"[JIEQI] years: {START_YEAR}..{END_YEAR}", flush=True)

    eph = load("de421.bsp")
    ts = load.timescale()

    data = _load_existing(OUTPUT_PATH)

    for year in range(START_YEAR, END_YEAR + 1):
        print(f"[JIEQI] year {year}", flush=True)
        print(f"[DEBUG] calling generate_year({year})", flush=True)

        year_data = generate_year(eph, ts, year)

        if not isinstance(year_data, list) or len(year_data) != 24:
            raise RuntimeError(
                f"{year} returned {len(year_data) if isinstance(year_data, list) else 'non-list'} items"
            )

        data[str(year)] = year_data
        _save_json_atomic(OUTPUT_PATH, data)

        print(f"[DEBUG] generate_year({year}) returned {len(year_data)} items", flush=True)

    print("[OK] jieqi generation complete", flush=True)


if __name__ == "__main__":
    generate()
