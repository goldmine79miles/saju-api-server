import json
import datetime as dt
from pathlib import Path
from math import ceil, floor

from skyfield.api import load
from skyfield.framelib import ecliptic_frame

# ====== 설정 ======
KST = dt.timezone(dt.timedelta(hours=9))

# 24절기(태양 황경 기준) - 15도 간격
# 사주 기준(A안): 소한부터 시작(24개 고정)
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
    0~360 황경 배열을 시간 증가에 따라 단조 증가하는 연속 각도로 변환.
    예: 359 -> 1 이면 1에 360을 더해 361로 만든다.
    """
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
    단일 시각 황경(0~360)을 '구간 왼쪽 unwrapped 값(left_u)' 기준으로 같은 브랜치로 맞춘다.
    (이게 핵심: 중간 ref 기준으로 하면 브랜치가 튀어서 1978로 점프하는 버그가 생김)
    """
    lon = sun_lon_deg(eph, t)
    k = round((left_u - lon) / 360.0)
    return lon + 360.0 * k

def bisect_root_in_interval(ts, eph, t0, t1, lon0_u, lon1_u, target_u, max_iter=80):
    """
    t0~t1 구간에서 unwrapped 황경 = target_u 되는 시각을 이분법으로 찾기.
    - lon0_u, lon1_u: 스캔에서 이미 계산한 신뢰 가능한 unwrapped 값
    - 중간점만 '왼쪽 값 기준'으로 unwrap 해서 같은 브랜치 유지
    """
    if lon1_u < lon0_u:
        lon1_u = lon0_u

    # 타겟이 구간에 포함되어야 함
    if not (lon0_u <= target_u <= lon1_u):
        return None

    a_t, b_t = t0, t1
    a_lon, b_lon = float(lon0_u), float(lon1_u)

    for _ in range(max_iter):
        m_t = ts.tt_jd((a_t.tt + b_t.tt) / 2.0)

        # ★ 핵심: 왼쪽 값(a_lon) 기준으로 같은 브랜치에 고정
        m_lon = lon_unwrapped_near_left(eph, m_t, a_lon)

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
    - 스캔 범위: 전년도 12/15 ~ 다음해 1/15 (연도 경계 커버)
    - A안(사주 기준): 24절기를 '정의 순서'대로 24개 고정 산출
    - 중복/이전해 점프 방지: 절기 하나 찾으면 그 이후 구간에서만 다음 절기를 찾음
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
    start_i = 1  # 다음 절기는 여기부터 찾음(중복/점프 방지)

    for name, target in JIEQI_DEF:
        found_kst = None
        found_i = None

        for i in range(start_i, len(lons_u)):
            lon0_u = float(lons_u[i - 1])
            lon1_u = float(lons_u[i])
            if lon1_u < lon0_u:
                lon1_u = lon0_u

            # target + 360*k 가 [lon0_u, lon1_u] 안에 들어오는 k 찾기
            k_min = ceil((lon0_u - target) / 360.0)
            k_max = floor((lon1_u - target) / 360.0)
            if k_min > k_max:
                continue

            # 보통 한 구간에 하나지만 안전하게 범위 처리
            for k in range(k_min, k_max + 1):
                target_u = target + 360.0 * k
                rt = bisect_root_in_interval(
                    ts, eph,
                    times[i - 1], times[i],
                    lon0_u, lon1_u,
                    target_u
                )
                if rt is None:
                    continue

                found_kst = rt.utc_datetime().replace(
                    tzinfo=dt.timezone.utc
                ).astimezone(KST)
                found_i = i
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

        # 다음 절기는 "현재 찾은 구간 이후"에서만 탐색
        if found_i is not None:
            start_i = max(found_i, start_i)

    # 보기 좋게 시간순 정렬
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
            names = [x["dateName"] for x in items]
            missing = [n for n, _ in JIEQI_DEF if n not in set(names)]
            dups = sorted({n for n in names if names.count(n) > 1})
            print(f"[WARN] {year} -> {len(items)} items, missing={missing}, dups={dups}")
            bad.append((year, len(items)))

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
