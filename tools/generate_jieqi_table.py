import json
import datetime as dt
from pathlib import Path

from skyfield.api import load
from skyfield.framelib import ecliptic_frame

# ====== 설정 ======
KST = dt.timezone(dt.timedelta(hours=9))

# 24절기 (사주 실무에서 쓰는 순서: 소한 시작)
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

# ====== 유틸 ======
def wrap_diff(a, target):
    """각도 차이를 (-180, 180]로 정규화"""
    return (a - target + 180.0) % 360.0 - 180.0

def sun_lon_deg(eph, t):
    """태양 황경(도) 단일 시간"""
    earth = eph["earth"]
    sun = eph["sun"]
    ast = earth.at(t).observe(sun).apparent()
    lon, _, _ = ast.frame_latlon(ecliptic_frame)
    return float(lon.degrees % 360.0)

def bisect_root(ts, eph, t0, t1, target, max_iter=90):
    """
    t0~t1 사이에서 황경=target 되는 시각을 이분법으로 찾기
    - wrap_diff 기준으로 부호가 바뀌는 구간만 들어온다고 가정
    """
    f0 = wrap_diff(sun_lon_deg(eph, t0), target)
    f1 = wrap_diff(sun_lon_deg(eph, t1), target)

    # 루트 보장 안 되면 None
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
        fm = wrap_diff(sun_lon_deg(eph, m), target)

        if abs(fm) < 1e-9:
            return m

        if fa * fm > 0:
            a, fa = m, fm
        else:
            b, fb = m, fm

    return ts.tt_jd((a.tt + b.tt) / 2.0)

def calc_year(ts, eph, year):
    """
    - 스캔 범위 확대: 전년도 12/15 ~ 다음해 1/15
    - 2시간 간격 스캔(정확도/안정성 ↑, 시간은 늘지만 액션에서만 돌림)
    - 각 절기는 1번만 찾고, 찾은 시간은 KST로 변환 후 year 필터
    """
    start = dt.datetime(year - 1, 12, 15, tzinfo=dt.timezone.utc)
    end   = dt.datetime(year + 1,  1, 15, tzinfo=dt.timezone.utc)

    step = dt.timedelta(hours=2)
    points = []
    cur = start
    while cur <= end:
        points.append(cur)
        cur += step

    times = ts.from_datetimes(points)

    # 벡터 황경 계산
    earth = eph["earth"]
    sun = eph["sun"]
    ast = earth.at(times).observe(sun).apparent()
    lon, _, _ = ast.frame_latlon(ecliptic_frame)
    lons = (lon.degrees % 360.0)

    out = []
    used_keys = set()  # (locdate, kst) 중복 방지

    for name, target in JIEQI_DEF:
        found_time = None

        prev = wrap_diff(float(lons[0]), target)
        for i in range(1, len(lons)):
            curd = wrap_diff(float(lons[i]), target)

            # 정확히 0을 찍으면 그 지점이 후보
            if prev == 0:
                found_time = times[i - 1]
                break

            # 부호 변화 구간에서만 root 탐색
            if prev * curd < 0:
                rt = bisect_root(ts, eph, times[i - 1], times[i], target)
                if rt is not None:
                    found_time = rt
                break

            prev = curd

        if found_time is None:
            continue

        kst_dt = found_time.utc_datetime().replace(tzinfo=dt.timezone.utc).astimezone(KST)
        if kst_dt.year != year:
            # 연도 밖이면 버림 (1979에 1978 들어오는 거 여기서 차단)
            continue

        key = (kst_dt.strftime("%Y%m%d"), kst_dt.strftime("%H%M"))
        if key in used_keys:
            # 같은 시각이 여러 절기에 재사용되면 = 계산이 깨진 것 → 버림
            continue
        used_keys.add(key)

        out.append({
            "dateName": name,
            "locdate": kst_dt.strftime("%Y%m%d"),
            "kst": kst_dt.strftime("%H%M"),
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
            print(f"[WARN] {year} -> {len(items)} items")
            bad.append((year, len(items)))

    out = Path("data/jieqi_1900_2052.json")
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print("generated:", out)
    print("bad years:", bad[:30], f"... total={len(bad)}")

if __name__ == "__main__":
    main()
