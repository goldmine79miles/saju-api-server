import json
import datetime as dt
from pathlib import Path

from skyfield.api import load
from skyfield.framelib import ecliptic_frame

KST = dt.timezone(dt.timedelta(hours=9))

JIEQI_DEF = [
    ("춘분", 0), ("청명", 15), ("곡우", 30), ("입하", 45),
    ("소만", 60), ("망종", 75), ("하지", 90), ("소서", 105),
    ("대서", 120), ("입추", 135), ("처서", 150), ("백로", 165),
    ("추분", 180), ("한로", 195), ("상강", 210), ("입동", 225),
    ("소설", 240), ("대설", 255), ("동지", 270), ("소한", 285),
    ("대한", 300), ("입춘", 315), ("우수", 330), ("경칩", 345),
]

def diff(a, b):
    return (a - b + 180.0) % 360.0 - 180.0

def sun_lons_deg(ts, eph, times):
    # ✅ 핵심: Time 배열을 for로 돌리지 않고 “한 번에” 계산(벡터 방식)
    earth = eph["earth"]
    sun = eph["sun"]
    ast = earth.at(times).observe(sun).apparent()
    lon, _, _ = ast.frame_latlon(ecliptic_frame)
    return (lon.degrees % 360.0)

def sun_lon_deg(ts, eph, t):
    # 단일 Time
    return float(sun_lons_deg(ts, eph, t).item())

def find_root(ts, eph, t0, t1, target, max_iter=60):
    f0 = diff(sun_lon_deg(ts, eph, t0), target)
    f1 = diff(sun_lon_deg(ts, eph, t1), target)

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
        fm = diff(sun_lon_deg(ts, eph, m), target)
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
    lons = sun_lons_deg(ts, eph, times)  # numpy array

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

    data = {}
    for year in range(1900, 2054):
        data[str(year)] = calc_year(ts, eph, year)

    out = Path("data/jieqi_1900_2100.json")
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print("generated:", out)

if __name__ == "__main__":
    main()
