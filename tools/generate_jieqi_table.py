import json
import datetime as dt
from pathlib import Path

from skyfield.api import load
from skyfield.framelib import ecliptic_frame

KST = dt.timezone(dt.timedelta(hours=9))

# 24절기 태양황경(도) 정의: 0=춘분, 90=하지, 180=추분, 270=동지
JIEQI_DEF = [
    ("춘분", 0), ("청명", 15), ("곡우", 30), ("입하", 45),
    ("소만", 60), ("망종", 75), ("하지", 90), ("소서", 105),
    ("대서", 120), ("입추", 135), ("처서", 150), ("백로", 165),
    ("추분", 180), ("한로", 195), ("상강", 210), ("입동", 225),
    ("소설", 240), ("대설", 255), ("동지", 270), ("소한", 285),
    ("대한", 300), ("입춘", 315), ("우수", 330), ("경칩", 345),
]

def diff(a, b):
    # a-b 를 -180~+180 범위로 감싸서 부호 변화를 안정적으로 잡음
    return (a - b + 180.0) % 360.0 - 180.0

def sun_lons_deg(eph, times):
    """Time 배열(벡터) 입력 → 태양 황경(도) 배열 반환"""
    earth = eph["earth"]
    sun = eph["sun"]
    ast = earth.at(times).observe(sun).apparent()
    lon, _, _ = ast.frame_latlon(ecliptic_frame)
    return (lon.degrees % 360.0)

def sun_lon_deg(eph, t):
    """단일 Time 입력 → 태양 황경(도) float 반환"""
    earth = eph["earth"]
    sun = eph["sun"]
    ast = earth.at(t).observe(sun).apparent()
    lon, _, _ = ast.frame_latlon(ecliptic_frame)
    return float(lon.degrees % 360.0)

def find_root(ts, eph, t0, t1, target, max_iter=60):
    """[t0, t1] 사이에서 황경=target 교차 시각을 이분법으로 찾음"""
    f0 = diff(sun_lon_deg(eph, t0), target)
    f1 = diff(sun_lon_deg(eph, t1), target)

    if f0 == 0:
        return t0
    if f1 == 0:
        return t1
    if f0 * f1 > 0:
        return None

    a, b = t0, t1
    fa, fb = f0, f1
    m = None

    for _ in range(max_iter):
        m = ts.tt_jd((a.tt + b.tt) / 2.0)
        fm = diff(sun_lon_deg(eph, m), target)

        if abs(fm) < 1e-7:
            return m

        if fa * fm > 0:
            a, fa = m, fm
        else:
            b, fb = m, fm

    return m

def calc_year(ts, eph, year):
    """
    해당 '연도'에 대해 1일 간격으로 스캔하며
    각 절기의 교차 구간을 찾아 이분법으로 시간 정밀화.
    """
    # UTC 기준으로 1년 범위 생성
    start = dt.datetime(year, 1, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(year + 1, 1, 1, tzinfo=dt.timezone.utc)

    days = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += dt.timedelta(days=1)

    times = ts.from_datetimes(days)
    lons = sun_lons_deg(eph, times)  # numpy array

    out = []
    for name, target in JIEQI_DEF:
        prev = diff(float(lons[0]), target)
        found = None

        for i in range(1, len(lons)):
            curd = diff(float(lons[i]), target)

            # 부호가 바뀌면(교차) 그 하루 구간에 절기가 있음
            if prev * curd < 0:
                found = find_root(ts, eph, times[i - 1], times[i], target)
                break

            prev = curd

        if found is None:
            # 해당 연도에서 교차를 못 찾으면 스킵
            continue

        # KST로 변환하여 저장
        kst = found.utc_datetime().replace(tzinfo=dt.timezone.utc).astimezone(KST)
        out.append({
            "dateName": name,
            "locdate": kst.strftime("%Y%m%d"),
            "kst": kst.strftime("%H%M"),
            "sunLongitude": target,
        })

    return out

def main():
    ts = load.timescale()
    eph = load("de421.bsp")

    # ✅ de421은 2053-10-09까지만 커버 → 2053년 전체 계산 시 터질 수 있음
    # 그래서 "완전한 연도"인 2052까지만 생성
    START_YEAR = 1900
    END_YEAR = 2052

    data = {}
    for year in range(START_YEAR, END_YEAR + 1):
        data[str(year)] = calc_year(ts, eph, year)

    out = Path(f"data/jieqi_{START_YEAR}_{END_YEAR}.json")
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print("generated:", out)

if __name__ == "__main__":
    main()
