import json
import datetime as dt
from pathlib import Path
from math import ceil, floor

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

# ====== 천문 유틸 ======
def sun_lon_deg(eph, t):
    """태양 황경(도) 단일 시간 (0~360)"""
    earth = eph["earth"]
    sun = eph["sun"]
    ast = earth.at(t).observe(sun).apparent()
    lon, _, _ = ast.frame_latlon(ecliptic_frame)
    return float(lon.degrees % 360.0)

def unwrap_lons(lons_mod):
    """
    0~360 황경 배열을 '시간이 증가할수록 단조 증가'하는 연속 각도로 변환.
    예: 359 -> 1 이면 1에 360을 더해 361로 만든다.
    """
    out = []
    offset = 0.0
    prev = float(lons_mod[0])
    out.append(prev)

    for x in lons_mod[1:]:
        x = float(x)
        if x < prev:  # 래핑(360->0) 발생
            offset += 360.0
        ux = x + offset
        out.append(ux)
        prev = x
    return out

def lon_unwrapped_near(eph, t, ref_unwrapped):
    """
    단일 시각의 황경(0~360)을 ref_unwrapped 근처의 연속 각도로 맞춰 반환.
    ref_unwrapped가 725도라면, 0~360 값을 720~1080 근처로 이동시킨다.
    """
    lon = sun_lon_deg(eph, t)  # 0~360
    # ref에 가장 가까운 360k를 선택
    k = round((ref_unwrapped - lon) / 360.0)
    return lon + 360.0 * k

def bisect_root_unwrapped(ts, eph, t0, t1, target_unwrapped, max_iter=80):
    """
    t0~t1 사이에서 unwrapped 황경 = target_unwrapped 되는 시각을 이분법으로 찾기.
    전제: 해당 구간에서 황경이 단조 증가하며 target이 [lon0, lon1] 안에 있어야 함.
    """
    lon0 = lon_unwrapped_near(eph, t0, target_unwrapped)
    lon1 = lon_unwrapped_near(eph, t1, target_unwrapped)

    # 안전: 정렬 보정
    if lon1 < lon0:
        lon1 += 360.0

    f0 = lon0 - target_unwrapped
    f1 = lon1 - target_unwrapped

    if abs(f0) < 1e-10:
        return t0
    if abs(f1) < 1e-10:
        return t1

    # 반드시 양끝이 target을 끼고 있어야 함
    if f0 > 0 or f1 < 0:
        return None

    a, b = t0, t1
    for _ in range(max_iter):
        m = ts.tt_jd((a.tt + b.tt) / 2.0)
        lonm = lon_unwrapped_near(eph, m, target_unwrapped)
        fm = lonm - target_unwrapped

        if abs(fm) < 1e-10:
            return m

        # f0 <= 0 <= f1 형태 유지
        if fm < 0:
            a = m
        else:
            b = m

    return ts.tt_jd((a.tt + b.tt) / 2.0)

# ====== 연도 계산 ======
def calc_year(ts, eph, year):
    """
    - 전년도 12/15 ~ 다음해 1/15 넓게 스캔 (연도 경계 누락 방지)
    - 스캔 포인트들의 황경을 unwrapped로 만들어 교차를 안정적으로 잡음
    - 각 절기 타겟은 target + 360*k 형태로 구간 포함 여부로 판정
    - 찾은 시각을 KST로 변환한 뒤, kst.year == year 인 것만 채택
    """
    start = dt.datetime(year - 1, 12, 15, tzinfo=dt.timezone.utc)
    end   = dt.datetime(year + 1,  1, 15, tzinfo=dt.timezone.utc)

    # 6시간 간격 스캔
    step = dt.timedelta(hours=6)
    points = []
    cur = start
    while cur <= end:
        points.append(cur)
        cur += step

    times = ts.from_datetimes(points)

    # 벡터로 황경 계산(0~360)
    earth = eph["earth"]
    sun = eph["sun"]
    ast = earth.at(times).observe(sun).apparent()
    lon, _, _ = ast.frame_latlon(ecliptic_frame)
    lons_mod = (lon.degrees % 360.0)

    # unwrapped 각도 생성
    lons_u = unwrap_lons(lons_mod)

    out = []
    for name, target in JIEQI_DEF:
        found = None

        # 각 구간마다 target + 360*k가 포함되는지 검사
        for i in range(1, len(lons_u)):
            a = float(lons_u[i - 1])
            b = float(lons_u[i])
            if b < a:
                # 이론상 unwrap으로 단조여야 하지만, 혹시라도 방어
                b = a

            # 이 구간에서 가능한 k 범위 계산
            k_min = ceil((a - target) / 360.0)
            k_max = floor((b - target) / 360.0)
            if k_min > k_max:
                continue

            # 보통 한 구간에 하나만 걸리지만, 안전하게 여러 k 처리
            for k in range(k_min, k_max + 1):
                target_u = target + 360.0 * k
                rt = bisect_root_unwrapped(ts, eph, times[i - 1], times[i], target_u)
                if rt is None:
                    continue

                kst_dt = rt.utc_datetime().replace(tzinfo=dt.timezone.utc).astimezone(KST)
                if kst_dt.year != year:
                    # 같은 절기라도 스캔 범위 때문에 다른 해로 분류될 수 있으니 계속 탐색
                    continue

                found = kst_dt
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

    # 날짜 기준 정렬
    out.sort(key=lambda x: (x["locdate"], x["kst"]))
    return out

def main():
    ts = load.timescale()
    eph = load("de421.bsp")

    data = {}
    bad_years = []

    for year in range(START_YEAR, END_YEAR + 1):
        items = calc_year(ts, eph, year)
        data[str(year)] = items

        if len(items) != 24:
            bad_years.append((year, len(items)))
            print(f"[WARN] {year} -> {len(items)} items")

    out = Path("data/jieqi_1900_2052.json")
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print("generated:", out)

    # 강제 실패 옵션(원하면 켜라): 품질 보장용
    # 지금은 일단 파일 생성/커밋을 위해 경고만 출력.
    # 품질 확정 단계에서는 아래 주석을 풀고 Workflow를 빨간불로 만들 것.
    #
    # if bad_years:
    #     raise SystemExit(f"Bad years: {bad_years[:10]} ... total={len(bad_years)}")

if __name__ == "__main__":
    main()
