# tools/generate_jieqi_table.py
# JIEQI_GENERATOR_VERSION=skyfield_root_finding_final

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
    # ecliptic_latlon()[1] = longitude
    lon = earth.at(t).observe(sun).apparent().ecliptic_latlon()[1].degrees
    return lon % 360.0


def _closest_target_deg(base_cont: float, target_deg_0_360: float) -> float:
    """
    base_cont(연속값, unwrap 기준) 근처의 동치각(target + 360*k) 중 가장 가까운 값을 선택.
    """
    t = float(target_deg_0_360)
    k = round((base_cont - t) / 360.0)
    cand = t + 360.0 * k
    # 주변도 확인
    for kk in (k - 1, k + 1):
        c2 = t + 360.0 * kk
        if abs(c2 - base_cont) < abs(cand - base_cont):
            cand = c2
    return cand


# -----------------------------
# Core: generate_year
# -----------------------------
def generate_year(eph, ts, year: int):
    """
    Fix: '1900 소한 not found' 같은 실패를 막기 위해
    - 탐색 구간을 (전년도 12/15 ~ 다음해 1/15)까지 확장
    - 태양 황경을 unwrap 해서 0/360 경계에서 끊기는 문제 제거
    - 6시간 단위 샘플링으로 교차 구간을 더 안정적으로 찾음
    - 교차 구간을 찾은 뒤 이진 탐색으로 시각 정밀화
    """
    UTC = timezone.utc

    # 넉넉한 탐색 구간(연초 절기 누락 방지)
    dt0 = datetime(year - 1, 12, 15, 0, 0, tzinfo=UTC)
    dt1 = datetime(year + 1, 1, 15, 0, 0, tzinfo=UTC)

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
    base = float(lon_unwrapped[0])

    results = []

    for name, deg in JIEQI_24:
        # 타겟을 base 근처 연속값으로 매핑
        target = _closest_target_deg(base, float(deg))
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
            raise RuntimeError(f"{year} {name} not found")

        left_dt = dts[idx]
        right_dt = dts[idx + 1]

        def f(dt: datetime) -> float:
            # dt UTC aware
            l0 = _sun_ecl_lon_deg(eph, ts, dt)  # 0~360
            # base 근처 연속값으로 매핑
            k = round((base - l0) / 360.0)
            l_cont = l0 + 360.0 * k
            return l_cont - target

        fl = f(left_dt)
        fr = f(right_dt)

        # 극히 드물게 타겟 동치각이 어긋나 같은 부호가 될 수 있어 ±360 재시도
        if fl * fr > 0:
            fixed = False
            for bump in (360.0, -360.0):
                target2 = target + bump

                def f2(dt: datetime) -> float:
                    l0 = _sun_ecl_lon_deg(eph, ts, dt)
                    k = round((base - l0) / 360.0)
                    l_cont = l0 + 360.0 * k
                    return l_cont - target2

                fl2 = f2(left_dt)
                fr2 = f2(right_dt)
                if fl2 * fr2 <= 0:
                    target = target2
                    f = f2
                    fl, fr = fl2, fr2
                    fixed = True
                    break

            if not fixed and fl * fr > 0:
                raise RuntimeError(f"{year} {name} bracket failed")

        # 이진 탐색(충분히 많이)
        for _ in range(60):
            mid_dt = left_dt + (right_dt - left_dt) / 2
            fm = f(mid_dt)
            if fm == 0:
                left_dt = right_dt = mid_dt
                fl = fr = 0
                break
            if fl * fm <= 0:
                right_dt = mid_dt
                fr = fm
            else:
                left_dt = mid_dt
                fl = fm

        utc_dt = right_dt
        kst_dt = utc_dt.astimezone(KST)

        results.append(
            {
                "name": name,
                "degree": int(deg),
                "utc": utc_dt.isoformat().replace("+00:00", "Z"),
                "kst": kst_dt.isoformat(),
            }
        )

    # 정렬(시간순)
    results.sort(key=lambda x: x["utc"])
    return results


# -----------------------------
# Main generate loop
# -----------------------------
def generate():
    print(f"[JIEQI] output={OUTPUT_PATH} append={APPEND}", flush=True)
    print(f"[JIEQI] years: {START_YEAR}..{END_YEAR}", flush=True)

    # ephemeris load (de421)
    eph = load("de421.bsp")
    ts = load.timescale()

    data = _load_existing(OUTPUT_PATH)

    for year in range(START_YEAR, END_YEAR + 1):
        print(f"[JIEQI] year {year}", flush=True)
        print(f"[DEBUG] calling generate_year({year})", flush=True)

        year_data = generate_year(eph, ts, year)

        # 보장: 24개
        if not isinstance(year_data, list) or len(year_data) != 24:
            raise RuntimeError(f"{year} returned {len(year_data) if isinstance(year_data, list) else 'non-list'} items")

        data[str(year)] = year_data

        # ✅ 연도마다 저장(중간에 죽어도 누적 유지)
        _save_json_atomic(OUTPUT_PATH, data)
        print(f"[DEBUG] generate_year({year}) returned {len(year_data)} items", flush=True)

    print("[OK] jieqi generation complete", flush=True)


if __name__ == "__main__":
    generate()
