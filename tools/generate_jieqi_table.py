import json
import datetime as dt
from pathlib import Path
from math import ceil, floor

from skyfield.api import load
from skyfield.framelib import ecliptic_frame

# ====== 설정 ======
KST = dt.timezone(dt.timedelta(hours=9))

# 24절기(태양 황경 기준) - 15도 간격
# 사주 기준: 소한부터 시작
JIEQI_DEF = [
    ("소한", 285), ("대한", 300), ("입춘", 315), ("우수", 330), ("경칩", 345),
    ("춘분", 0), ("청명", 15), ("곡우", 30), ("입하", 45),
    ("소만", 60), ("망종", 75), ("하지", 90), ("소서", 105),
    ("대서", 120), ("입추", 135), ("처서", 150), ("백로", 165),
    ("추분", 180), ("한로", 195), ("상강", 210), ("입동", 225),
    ("소설", 240), ("대설", 255), ("동지", 270),
]

START_YEAR = 1900
END_YEAR = 2052

# ====== 천문 유틸 ======
def sun_lon_deg(eph, t):
    earth = eph["earth"]
    sun = eph["sun"]
    ast = earth.at(t).observe(sun).apparent()
    lon, _, _ = ast.frame_latlon(ecliptic_frame)
    return float(lon.degrees % 360.0)

def unwrap_lons(lons_mod):
    out = []
    offset = 0.0
    prev = float(lons_mod[0])
    out.append(prev)
    for x in lons_mod[1:]:
        x = float(x)
        if x < prev:
            offset += 360.0
        out.append(x + offset)
        prev = x
    return out

def lon_unwrapped_near(eph, t, ref):
    lon = sun_lon_deg(eph, t)
    k = round((ref - lon) / 360.0)
    return lon + 360.0 * k

def bisect_root_unwrapped(ts, eph, t0, t1, target, max_iter=80):
    lon0 = lon_unwrapped_near(eph, t0, target)
    lon1 = lon_unwrapped_near(eph, t1, target)
    if lon1 < lon0:
        lon1 += 360.0

    f0 = lon0 - target
    f1 = lon1 - target
    if f0 > 0 or f1 < 0:
        return None

    a, b = t0, t1
    for _ in range(max_iter):
        m = ts.tt_jd((a.tt + b.tt) / 2.0)
        lonm = lon_unwrapped_near(eph, m, target)
        fm = lonm - target
        if abs(fm) < 1e-10:
            return m
        if fm < 0:
            a = m
        else:
            b = m
    return ts.tt_jd((a.tt + b.tt) / 2.0)

# ====== 연도 계산 ======
def calc_year(ts, eph, year):
    start = dt.datetime(year - 1, 12, 15, tzinfo=dt.timezone.utc)
    end   = dt.datetime(year + 1,  1, 15, tzinfo=dt.timezone.utc)

    step = dt.timedelta(hours=6)
    points = []
    cur = start
    while cur <= end:
        points.append(cur)
        cur += step

    times = ts.from_datetimes(points)

    earth = eph["earth"]
    sun = eph["sun"]
    ast = earth.at(times).observe(sun).apparent()
    lon, _, _ = ast.frame_latlon(ecliptic_frame)
    lons_u = unwrap_lons(lon.degrees % 360.0)

    out = []

    for name, target in JIEQI_DEF:
        found = None
        for i in range(1, len(lons_u)):
            a = lons_u[i - 1]
            b = lons_u[i]
            if b < a:
                b = a

            k_min = ceil((a - target) / 360.0)
            k_max = floor((b - target) / 360.0)
            if k_min > k_max:
                continue

            for k in range(k_min, k_max + 1):
                t_u = target + 360.0 * k
                rt = bisect_root_unwrapped(ts, eph, times[i - 1], times[i], t_u)
                if rt is None:
                    continue

                found = rt.utc_datetime().replace(
                    tzinfo=dt.timezone.utc
                ).astimezone(KST)
                break

            if found is not None:
                break

        if found is None:
            continue

        out.append({
            "dateName": name,
            "locdate": found.strftime("%Y%m%d"),
            "kst": found.strftime("%H%M"),
            "sunLongitude": target,
            "source": "table",
        })

    out.sort(key=lambda x: (x["locdate"], x["kst"]))
    return out

def main():
    ts = load.timescale()
    eph = load("de421.bsp")

    data = {}
    bad = []

    for year in range(START_YEAR, END_YEAR + 1):
        items = calc_year(ts, eph, year)
        data[str(year)] = items
        if len(items) != 24:
            print(f"[WARN] {year} -> {len(items)}")
            bad.append(year)

    out = Path("data/jieqi_1900_2052.json")
    out.parent.mkdir(exist_ok=True)
    out.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print("generated:", out)
    print("bad years:", bad)

if __name__ == "__main__":
    main()
