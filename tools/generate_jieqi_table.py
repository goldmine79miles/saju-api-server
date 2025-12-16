import json
import datetime as dt
from pathlib import Path

from skyfield.api import load
from skyfield.framelib import ecliptic_frame

# ====== 설정 ======
KST = dt.timezone(dt.timedelta(hours=9))

# 24절기(태양 황경 기준) - 15도 간격
# 기준: 춘분=0°, 청명=15°, ... , 경칩=345°
JIEQI_DEF = [
    ("춘분", 0), ("청명", 15), ("곡우", 30), ("입하", 45),
    ("소만", 60), ("망종", 75), ("하지", 90), ("소서", 105),
    ("대서", 120), ("입추", 135), ("처서", 150), ("백로", 165),
    ("추분", 180), ("한로", 195), ("상강", 210), ("입동", 225),
    ("소설", 240), ("대설", 255), ("동지", 270), ("소한", 285),
    ("대한", 300), ("입춘", 315), ("우수", 330), ("경칩", 345),
]

# de421 범위: 대략 1899-07-29 ~ 2053-10-09
START_YEAR = 1900
END_YEAR = 2052  # 포함(<=2052)

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

def bisect_root(ts, eph, t0, t1, target, max_iter=80):
    """t0~t1 사이에서 황경=target 되는 시각을 이분법으로 찾기"""
    f0 = wrap_diff(sun_lon_deg(eph, t0), target)
    f1 = wrap_diff(sun_lon_deg(eph, t1), target)

    # 부호가 같으면 루트 보장 안 됨
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

        if abs(fm) < 1e-8:
            return m

        if fa * fm > 0:
            a, fa = m, fm
        else:
            b, fb = m, fm

    return m

def calc_year(ts, eph, year):
    """
    핵심 포인트:
    - '해당 연도 1/1~다음해 1/1'만 보면 경계 누락이 생김
    - 절기는 전후 며칠 걸쳐 정확히 지나가므로
      [전년도 12/15 ~ 다음해 1/15] 넓게 스캔해서
      '결과 locdate가 year인 것만' 필터한다.
    """
    start = dt.datetime(year - 1, 12, 15, tzinfo=dt.timezone.utc)
    end   = dt.datetime(year + 1,  1, 15, tzinfo=dt.timezone.utc)

    # 6시간 간격으로 스캔(24절기면 6시간이면 충분히 브라켓 잡힘)
    step = dt.timedelta(hours=6)
    points = []
    cur = start
    while cur <= end:
        points.append(cur)
        cur += step

    times = ts.from_datetimes(points)

    # 벡터로 황경 계산
    earth = eph["earth"]
    sun = eph["sun"]
    ast = earth.at(times).observe(sun).apparent()
    lon, _, _ = ast.frame_latlon(ecliptic_frame)
    lons = (lon.degrees % 360.0)

    out = []
    for name, target in JIEQI_DEF:
        found_time = None

        prev = wrap_diff(float(lons[0]), target)
        for i in range(1, len(lons)):
            curd = wrap_diff(float(lons[i]), target)

            # 0도 래핑에서도 부호변화가 깨지지 않게 wrap_diff 사용
            if prev == 0:
                # 바로 적중한 포인트
                found_time = times[i - 1]
                break

            if prev * curd < 0:
                found_time = bisect_root(ts, eph, times[i - 1], times[i], target)
                break

            prev = curd

        if found_time is None:
            continue

        kst = found_time.utc_datetime().replace(tzinfo=dt.timezone.utc).astimezone(KST)
        # year에 해당하는 것만 남김
        if kst.year != year:
            continue

        out.append({
            "dateName": name,
            "locdate": kst.strftime("%Y%m%d"),
            "kst": kst.strftime("%H%M"),
            "sunLongitude": target,
            "source": "table",
        })

    # 날짜 기준 정렬
    out.sort(key=lambda x: (x["locdate"], x["kst"]))

    return out

def main():
    ts = load.timescale()
    eph = load("de421.bsp")

    data = {}
    for year in range(START_YEAR, END_YEAR + 1):
        items = calc_year(ts, eph, year)
        data[str(year)] = items
        # 안전장치: 24개가 아니면 눈에 띄게 표시(그래도 파일은 생성)
        if len(items) != 24:
            print(f"[WARN] {year} -> {len(items)} items")

    out = Path("data/jieqi_1900_2052.json")
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print("generated:", out)

if __name__ == "__main__":
    main()
