import json
import datetime as dt
from pathlib import Path

from skyfield.api import load
from skyfield.framelib import ecliptic_frame

KST = dt.timezone(dt.timedelta(hours=9))

# 24절기 = 태양황경 기준 (도)
JIEQI_DEF = [
    ("춘분", 0), ("청명", 15), ("곡우", 30), ("입하", 45),
    ("소만", 60), ("망종", 75), ("하지", 90), ("소서", 105),
    ("대서", 120), ("입추", 135), ("처서", 150), ("백로", 165),
    ("추분", 180), ("한로", 195), ("상강", 210), ("입동", 225),
    ("소설", 240), ("대설", 255), ("동지", 270), ("소한", 285),
    ("대한", 300), ("입춘", 315), ("우수", 330), ("경칩", 345),
]

def diff(a, b):
    # 각도 차이를 -180~+180 범위로 정규화
    return (a - b + 180.0) % 360.0 - 180.0

def sun_lons_deg(eph, times):
    """times: Skyfield Time (스칼라 or 배열) -> 태양 황경(deg)"""
    earth = eph["earth"]
    sun = eph["sun"]
    ast = earth.at(times).observe(sun).apparent()
    lon, _, _ = ast.frame_latlon(ecliptic_frame)
    return lon.degrees % 360.0

def sun_lon_deg(eph, t):
    """t: Skyfield Time (스칼라) -> float"""
    return float(sun_lons_deg(eph, t))

def find_root(ts, eph, t0, t1, target, max_iter=60):
    """t0~t1 사이에 target(황경) 교차가 있으면 이분법으로 시각 찾기"""
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
    start = dt.datetime(year, 1, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(year + 1, 1, 1, tzinfo=dt.timezone.utc)

    days = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += dt.timedelta(days=1)

    times = ts.from_datetimes(days)
    lons = sun_lons_deg(eph, times)  # 배열

    out = []
    for name, target in JIEQI_DEF:
        prev = diff(float(lons[0]), target)
        found = None

        for i in range(1, len(lons)):
            curd = diff(float(lons[i]), target)
            if prev * curd < 0:
                found = find_root(ts, eph, times[i - 1], times[i], target)
                break
            prev = curd

        # ✅ 여기 핵심: 절대 "if found:" 같은 boolean 체크 금지
        if found is None:
            continue

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

    # de421 범위 안전하게: 1900~2052까지만 생성
    data = {}
    for year in range(1900, 2053):  # 2052 포함
        data[str(year)] = calc_year(ts, eph, year)

    out = Path("data/jieqi_1900_2052.json")
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print("generated:", out)

if __name__ == "__main__":
    main()
