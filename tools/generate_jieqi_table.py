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
    lon = earth.at(t).observe(sun).apparent().ecliptic_latlon()[1].degrees
    return lon % 360.0


def _to_utc_aware(dt: datetime) -> datetime:
    """Ensure timezone-aware UTC datetime."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# -----------------------------
# Core: generate_year
# -----------------------------
def generate_year(eph, ts, year: int):
    """
    안정형 절기 생성기 (not found 방지)
    - 탐색 구간을 넓게: (year-2)/12/01 ~ (year+1)/01/31
      * 연초 절기(소한/대한/입춘) 누락 방지
    - 6시간 샘플링 + unwrap으로 0/360 경계 문제 제거
    - 각 절기 타겟(deg)에 대해 360*k 후보를 전부 탐색
      -> 구간 내 교차가 존재하는 k를 찾아 브래킷 형성
    - 이진탐색으로 교차 시각 정밀화
    - KST 기준 year에 속하는 이벤트만 채택

    🔥 중요:
    - Skyfield는 ephemeris 범위를 TT 기준으로 체크함.
    - datetime으로 "경계값"을 맞춰도 TT 변환에서 튕길 수 있음.
    - 그래서 dt0/dt1을 eph.coverage(start/end)로 "Time(tt) 비교"로 클램프 + 안전마진 필요.
    """
    UTC = timezone.utc

    # 🔥 넉넉한 탐색 구간 (연초/연말 절기 누락 방지)
    dt0 = datetime(year - 2, 12, 1, 0, 0, tzinfo=UTC)
    dt1 = datetime(year + 1, 1, 31, 0, 0, tzinfo=UTC)

    # 🔥 ephemeris coverage로 클램프 (Time 기준 + 안전 마진)
    # - Skyfield는 TT 기준으로 범위를 체크하므로 datetime 비교만으로는 경계에서 튕길 수 있음
    # - 경계 떨림 방지: start는 +2일, end는 -2일 안전 마진
    eph_start_t = eph.coverage.start
    eph_end_t = eph.coverage.end

    t0 = ts.from_datetime(dt0)
    t1 = ts.from_datetime(dt1)

    safety = timedelta(days=2)

    if t0.tt < eph_start_t.tt:
        dt0 = _to_utc_aware((eph_start_t + safety).utc_datetime())
    if t1.tt > eph_end_t.tt:
        dt1 = _to_utc_aware((eph_end_t - safety).utc_datetime())

    if dt0 >= dt1:
        raise RuntimeError(
            f"{year} search range invalid after clamp: dt0={dt0.isoformat()} dt1={dt1.isoformat()} "
            f"(eph={_to_utc_aware(eph_start_t.utc_datetime()).isoformat()}..{_to_utc_aware(eph_end_t.utc_datetime()).isoformat()})"
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

    # unwrap: 359 -> 0 점프 제거 (연속 시퀀스로)
    lon_unwrapped = np.rad2deg(np.unwrap(np.deg2rad(lon)))
    min_lon = float(np.min(lon_unwrapped))
    max_lon = float(np.max(lon_unwrapped))

    results = []

    for name, deg in JIEQI_24:
        deg = float(deg)

        # 이 구간에서 가능한 360*k 후보들을 넉넉히 열거
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
                # target 근처 연속값으로 매핑
                l_cont = l0 + 360.0 * round((target - l0) / 360.0)
                return l_cont - target

            fl = f(left_dt)
            fr = f(right_dt)

            # 브래킷 실패면 이 k는 스킵
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

            # ✅ 해당 연도(KST 기준)에 속하는 절기만 채택
            if kst_dt.year != year:
                continue

            # 절기 1개만 필요 → 가장 이른 것 채택
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

    # 시간순 정렬
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

        # ✅ 연도마다 저장(중간에 죽어도 누적 유지)
        _save_json_atomic(OUTPUT_PATH, data)

        print(f"[DEBUG] generate_year({year}) returned {len(year_data)} items", flush=True)

    print("[OK] jieqi generation complete", flush=True)


if __name__ == "__main__":
    generate()
