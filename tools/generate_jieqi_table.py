import json
import datetime as dt
from pathlib import Path
from math import floor

from skyfield.api import load
from skyfield.framelib import ecliptic_frame

KST = dt.timezone(dt.timedelta(hours=9))

# 24절기: 태양 황경(도) 기준 (사주 실무에서 흔히 쓰는 순서: 입춘 기준으로 흐름 잡음)
JIEQI_DEF = [
    ("춘분", 0), ("청명", 15), ("곡우", 30), ("입하", 45),
    ("소만", 60), ("망종", 75), ("하지", 90), ("소서", 105),
    ("대서", 120), ("입추", 135), ("처서", 150), ("백로", 165),
    ("추분", 180), ("한로", 195), ("상강", 210), ("입동", 225),
    ("소설", 240), ("대설", 255), ("동지", 270), ("소한", 285),
    ("대한", 300), ("입춘", 315), ("우수", 330), ("경칩", 345),
]
NAME_BY_DEG = {deg: name for name, deg in JIEQI_DEF}

# 생성 범위
START_YEAR = 1900
END_YEAR = 2052

def sun_lon_deg(eph, t):
    """태양 황경(0~360)"""
    earth = eph["earth"]
    sun = eph["sun"]
    ast = earth.at(t).observe(sun).apparent()
    lon, _, _ = ast.frame_latlon(ecliptic_frame)
    return float(lon.degrees % 360.0)

def unwrap_lons(lons_mod):
    """0~360 배열을 단조증가 unwrapped로 변환"""
    out = []
    offset = 0.0
    prev = float(lons_mod[0])
    out.append(prev)
    for x in lons_mod[1:]:
        x = float(x)
        if x < prev:  # 360->0 래핑
            offset += 360.0
        out.append(x + offset)
        prev = x
    return out

def lon_unwrapped_near_left(eph, t, left_u):
    """
    단일 시각의 황경(0~360)을 '구간 왼쪽 unwrapped 값' 기준으로 같은 브랜치로 맞춤.
    (브랜치 튐 방지 핵심)
    """
    lon = sun_lon_deg(eph, t)
    k = round((left_u - lon) / 360.0)
    return lon + 360.0 * k

def bisect_unwrapped(ts, eph, t0, t1, lon0_u, lon1_u, target_u, max_iter=80):
    """
    [t0,t1]에서 unwrapped lon = target_u 되는 시각을 이분법으로 찾기.
    전제: lon0_u <= target_u <= lon1_u
    """
    if lon1_u < lon0_u:
        lon1_u = lon0_u
    if not (lon0_u <= target_u <= lon1_u):
        return None

    a_t, b_t = t0, t1
    a_lon, b_lon = float(lon0_u), float(lon1_u)

    for _ in range(max_iter):
        m_t = ts.tt_jd((a_t.tt + b_t.tt) / 2.0)
        m_lon = lon_unwrapped_near_left(eph, m_t, a_lon)

        if abs(m_lon - target_u) < 1e-10:
            return m_t

        if m_lon < target_u:
            a_t, a_lon = m_t, m_lon
        else:
            b_t, b_lon = m_t, m_lon

    return ts.tt_jd((a_t.tt + b_t.tt) / 2.0)

def collect_events(ts, eph, start_utc, end_utc, step_hours=12):
    """
    기간 내 모든 15도 경계(=절기) 교차 이벤트를 시간순으로 수집.
    - 스텝은 12시간이면 충분 (태양 이동량 ~0.5도/12h 수준)
    """
    step = dt.timedelta(hours=step_hours)
    points = []
    cur = start_utc
    while cur <= end_utc:
        points.append(cur)
        cur += step

    times = ts.from_datetimes(points)

    earth = eph["earth"]
    sun = eph["sun"]
    ast = earth.at(times).observe(sun).apparent()
    lon, _, _ = ast.frame_latlon(ecliptic_frame)
    lons_mod = (lon.degrees % 360.0)
    lons_u = unwrap_lons(lons_mod)

    events = []
    seen = set()  # (deg_mod, locdate, kst) 중복 방지

    for i in range(1, len(lons_u)):
        a_u = float(lons_u[i - 1])
        b_u = float(lons_u[i])
        if b_u < a_u:
            b_u = a_u

        # 이 구간에서 15도 단위 경계를 몇 개 넘는지 계산
        m0 = floor(a_u / 15.0)
        m1 = floor(b_u / 15.0)

        if m1 <= m0:
            continue

        # (m0+1 .. m1) 경계 통과
        for m in range(m0 + 1, m1 + 1):
            target_u = 15.0 * m
            deg_mod = int(target_u % 360.0)
            if deg_mod not in NAME_BY_DEG:
                continue

            rt = bisect_unwrapped(ts, eph, times[i - 1], times[i], a_u, b_u, target_u)
            if rt is None:
                continue

            kst_dt = rt.utc_datetime().replace(tzinfo=dt.timezone.utc).astimezone(KST)
            key = (deg_mod, kst_dt.strftime("%Y%m%d"), kst_dt.strftime("%H%M"))
            if key in seen:
                continue
            seen.add(key)

            events.append({
                "dateName": NAME_BY_DEG[deg_mod],
                "locdate": kst_dt.strftime("%Y%m%d"),
                "kst": kst_dt.strftime("%H%M"),
                "sunLongitude": deg_mod,
                "source": "table",
                "_ts": kst_dt,  # 내부 정렬용(출력 시 제거)
            })

    events.sort(key=lambda x: x["_ts"])
    return events

def build_year_24(events, year):
    """
    '해당 연도 KST의 입춘'을 시작점으로 24개 연속 절기를 리턴.
    """
    # 해당 연도(KST) 입춘 찾기
    start_idx = None
    for idx, e in enumerate(events):
        if e["dateName"] == "입춘" and e["_ts"].year == year:
            start_idx = idx
            break

    if start_idx is None:
        return []

    slice_ = events[start_idx:start_idx + 24]
    if len(slice_) < 24:
        return []

    out = []
    for e in slice_:
        out.append({
            "dateName": e["dateName"],
            "locdate": e["locdate"],
            "kst": e["kst"],
            "sunLongitude": e["sunLongitude"],
            "source": "table",
        })
    return out

def main():
    ts = load.timescale()
    eph = load("de421.bsp")

    print("JIEQI_GENERATOR_VERSION=timeline_slice_v1")

    data = {}
    bad = []

    for year in range(START_YEAR, END_YEAR + 1):
        # 2년+α 윈도우 (입춘 기준 24개를 안정적으로 자르기 위함)
        start = dt.datetime(year - 1, 1, 1, tzinfo=dt.timezone.utc)
        end   = dt.datetime(year + 1, 12, 31, tzinfo=dt.timezone.utc)

        events = collect_events(ts, eph, start, end, step_hours=12)
        items = build_year_24(events, year)

        data[str(year)] = items
        if len(items) != 24:
            bad.append((year, len(items)))
            print(f"[WARN] {year} -> {len(items)} items")

    out = Path("data/jieqi_1900_2052.json")
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print("generated:", out)
    print("bad years:", bad[:30], f"... total={len(bad)}")

    # 품질 강제(원하면 바로 켜세요): 24개 아니면 액션 실패
    # if bad:
    #     raise SystemExit(f"Bad years exist: {bad[:10]} ... total={len(bad)}")

if __name__ == "__main__":
    main()
