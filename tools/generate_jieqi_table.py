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
        if x < prev:  # 래핑(360->0)
            offset += 360.0
        out.append(x + offset)
        prev = x
    return out

def lon_unwrapped_near(eph, t, ref_u):
    """
    단일 시각의 황경(0~360)을 ref_u 근처의 연속 각도로 맞춰 반환.
    ref_u를 '구간 unwrapped'로 주면, 같은 branch로 안정적으로 붙는다.
    """
    lon = sun_lon_deg(eph, t)  # 0~360
    k = round((ref_u - lon) / 360.0)
    return lon + 360.0 * k

def bisect_root_in_interval(ts, eph, t0, t1, lon0_u, lon1_u, target_u, max_iter=80):
    """
    t0~t1 구간에서 unwrapped 황경 = target_u 되는 시각을 이분법으로 찾기.
    - lon0_u, lon1_u 는 이미 계산된 스캔 포인트의 unwrapped 황경(신뢰값)
    - 중간점 황경만 lon_unwrapped_near로 같은 branch에 붙여 계산
    """
    # 반드시 끼고 있어야 함: lon0 <= target <= lon1
    if not (lon0_u <= target_u <= lon1_u):
        return None

    a_t, b_t = t0, t1
    a_lon, b_lon = lon0_u, lon1_u

    # 중간점 unwrap 기준은 "현재 구간 중앙 황경"으로 잡으면 안정적
    for _ in range(max_iter):
        m_t = ts.tt_jd((a_t.tt + b_t.tt) / 2.0)
        ref = (a_lon + b_lon) / 2.0
        m_lon = lon_unwrapped_near(eph, m_t, ref)

        if abs(m_lon - target_u) < 1e-10:
            return m_t

        if m_lon < target_u:
            a_t, a_lon = m_t, m_lon
        else:
            b_t, b_lon = m_t, m_lon

    return ts.tt_jd((a_t.tt + b_t.tt) / 2.0)

# ====== 연도 계산 ======
def calc_year(ts, eph, year):
    """
    사주 기준(A안): '해당 해'를 행정연도(kst.year)로 자르지 않고,
    스캔 범위(전년도 12/15 ~ 다음해 1/15) 안에서 24절기를 "절기 정의 순서"대로 24개 고정 산출.
    """
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

    lons_mod = (lon.degrees % 360.0)
    lons_u = unwrap_lons(lons_mod)

    out = []

    for name, target in JIEQI_DEF:
        found_kst = None

        for i in range(1, len(lons_u)):
            a_u = float(lons_u[i - 1])
            b_u = float(lons_u[i])

            # (방어) 이론상 단조 증가지만 혹시라도…
            if b_u < a_u:
                b_u = a_u

            # target + 360*k 가 [a_u, b_u] 안에 들어오는 k를 찾는다
            k_min = ceil((a_u - target) / 360.0)
            k_max = floor((b_u - target) / 360.0)
            if k_min > k_max:
                continue

            # 보통 한 구간에 하나지만 안전하게 범위 처리
            for k in range(k_min, k_max + 1):
                target_u = target + 360.0 * k

                rt = bisect_root_in_interval(
                    ts, eph,
                    times[i - 1], times[i],
                    a_u, b_u,
                    target_u
                )
                if rt is None:
                    continue

                found_kst = rt.utc_datetime().replace(
                    tzinfo=dt.timezone.utc
                ).astimezone(KST)
                break

            if found_kst is not None:
                break

        if found_kst is None:
            continue

        out.append({
            "dateName": name,
            "locdate": found_kst.strftime("%Y%m%d"),
            "kst": found_kst.strftime("%H%M"),
            "sunLongitude": target,
            "source": "table",
        })

    # 결과는 날짜 순으로 정렬 (사주 기준 흐름만 원하면 이 정렬 제거해도 됨)
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
            missing = [name for name, _ in JIEQI_DEF if name not in {x["dateName"] for x in items}]
            print(f"[WARN] {year} -> {len(items)} items, missing={missing}")
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
