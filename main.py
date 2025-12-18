from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from datetime import datetime, date
from zoneinfo import ZoneInfo
import requests
import json
import os

app = FastAPI(
    title="Saju API Server",
    version="1.2.0"  # day pillar added
)

# =========================
# Paths / Env
# =========================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JIEQI_TABLE_PATH = os.path.join(BASE_DIR, "data", "jieqi_1900_2052.json")
KASI_SERVICE_KEY = os.getenv("KASI_SERVICE_KEY")
KST = ZoneInfo("Asia/Seoul")

# =========================
# Utils
# =========================

def load_jieqi_table():
    with open(JIEQI_TABLE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def _parse_dt_any(value):
    """
    Jieqi JSON datetime value 파싱:
    - ISO: "1979-02-04T19:04:05+09:00", "1979-02-04T10:04:05Z"
    - date: "1979-02-04"
    - epoch: 1234567890 / 1234567890123
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        v = float(value)
        if v >= 1_000_000_000_000:
            v = v / 1000.0
        return datetime.fromtimestamp(v, tz=KST)

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"

        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
            else:
                dt = dt.astimezone(KST)
            return dt
        except Exception:
            pass

        try:
            return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=KST)
        except Exception:
            return None

    return None

def find_ipchun_dt(jieqi_list):
    """
    해당 연도 절기 리스트에서 '입춘(立春)' 시각을 찾아 KST datetime으로 반환.
    ✅ JSON 스키마: { name, deg, utc, kst } 형태 지원 (kst 우선)
    """
    if not isinstance(jieqi_list, list):
        raise ValueError("jieqi_list is not a list")

    def _is_ipchun(item):
        candidates = [
            item.get("name"),
            item.get("label"),
            item.get("title"),
            item.get("jieqi"),
            item.get("key"),
            item.get("code"),
        ]
        candidates = [c for c in candidates if isinstance(c, str)]
        joined = " ".join(candidates)

        if "입춘" in joined:
            return True
        if "立春" in joined:
            return True
        if "IPCHUN" in joined.upper():
            return True
        return False

    dt_keys_priority = ["kst", "utc", "dt", "datetime", "time", "at", "iso", "when", "timestamp", "ts"]

    for item in jieqi_list:
        if not isinstance(item, dict):
            continue
        if not _is_ipchun(item):
            continue

        for k in dt_keys_priority:
            if k in item:
                dt = _parse_dt_any(item.get(k))
                if dt:
                    return dt

        for _, v in item.items():
            if isinstance(v, dict):
                for kk in dt_keys_priority:
                    if kk in v:
                        dt = _parse_dt_any(v.get(kk))
                        if dt:
                            return dt

    raise ValueError("입춘(立春) datetime not found in jieqi table")

def normalize_birth_time(birth_time: str):
    """
    A안 정책: 시간은 optional.
    - unknown/빈값/형식오류 => time_applied False
    - 정상(HH:MM) => time_applied True
    """
    if not isinstance(birth_time, str):
        return None, False
    s = birth_time.strip()
    if not s or s.lower() == "unknown":
        return None, False
    try:
        t = datetime.strptime(s, "%H:%M")
        return (t.hour, t.minute), True
    except Exception:
        return None, False

def parse_birth_dt_kst(birth: str, birth_time: str):
    """
    A안 정책:
    - time_applied=False면 시각은 00:00으로 내부 처리하되,
      meta에 time_applied를 명확히 남긴다.
    """
    base_date = datetime.strptime(birth, "%Y-%m-%d")
    (hm, time_applied) = normalize_birth_time(birth_time)

    hh, mm = (0, 0)
    if hm is not None:
        hh, mm = hm

    dt = datetime(base_date.year, base_date.month, base_date.day, hh, mm, tzinfo=KST)
    return dt, time_applied

# =========================
# KASI (optional, fallback-safe)
# =========================

def fetch_jieqi_from_kasi(year: int):
    """
    KASI가 살아있는지 확인만 하는 용도.
    실패하면 예외를 던져 fallback으로 전환된다.
    """
    if not KASI_SERVICE_KEY:
        raise RuntimeError("KASI key missing")

    url = "https://apis.data.go.kr/B090041/openapi/service/LrsrCldInfoService/getSolCalInfo"
    params = {
        "serviceKey": KASI_SERVICE_KEY,
        "solYear": year,
        "solMonth": 1,
        "solDay": 1,
        "numOfRows": 10,
        "pageNo": 1,
    }

    r = requests.get(url, params=params, timeout=3)
    r.raise_for_status()
    return True

# =========================
# Core (jieqi)
# =========================

def get_jieqi_with_fallback(year: str):
    """
    1) KASI 먼저 시도 (헬스 체크용)
    2) 실패하면 JSON 절기 테이블 사용
    """
    source = "json"
    fallback = True

    try:
        fetch_jieqi_from_kasi(int(year))
        source = "kasi"
        fallback = False
    except Exception:
        source = "json"
        fallback = True

    table = load_jieqi_table()
    year_data = table.get(year)

    if not year_data:
        raise ValueError(f"No jieqi data for year {year}")

    return source, fallback, year_data

def resolve_saju_year(birth_dt_kst: datetime, birth_year_jieqi_list: list) -> int:
    """
    입춘 기준 사주 연도:
      - 출생일시 < 해당 해 입춘 시각 -> birth_year - 1
      - 출생일시 >= 해당 해 입춘 시각 -> birth_year
    """
    ipchun_dt = find_ipchun_dt(birth_year_jieqi_list)
    y = birth_dt_kst.year
    return y if birth_dt_kst >= ipchun_dt else y - 1

# =========================
# Core (Day Pillar)
# =========================

STEMS = ["甲","乙","丙","丁","戊","己","庚","辛","壬","癸"]
BRANCHES = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"]

def gregorian_to_jdn(y: int, m: int, d: int) -> int:
    """
    Gregorian calendar date -> Julian Day Number (JDN)
    """
    a = (14 - m) // 12
    y2 = y + 4800 - a
    m2 = m + 12 * a - 3
    return d + (153 * m2 + 2) // 5 + 365 * y2 + y2 // 4 - y2 // 100 + y2 // 400 - 32045

def get_day_pillar(local_date: date):
    """
    일주(일간/일지) 계산 (간지 코드만)
    기준:
      - 1984-02-02(Gregorian) = 甲子(갑자일)로 맞춰 offset 적용
      - day_index = (JDN + 47) % 60  (0=甲子 ... 59=癸亥)
    """
    jdn = gregorian_to_jdn(local_date.year, local_date.month, local_date.day)
    day_index = (jdn + 47) % 60
    stem = STEMS[day_index % 10]
    branch = BRANCHES[day_index % 12]
    return {
        "stem": stem,
        "branch": branch,
        "ganji": stem + branch,
        "index60": day_index
    }

# =========================
# API
# =========================

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/api/saju/calc")
def calc_saju(
    birth: str = Query(..., description="YYYY-MM-DD"),
    calendar: str = Query("solar", description="solar or lunar"),
    birth_time: str = Query("unknown", description="HH:MM (optional)"),
    gender: str = Query("unknown", description="male or female (optional)"),
):
    try:
        if gender not in ("male", "female", "unknown"):
            return JSONResponse(
                status_code=400,
                content={"error": "gender must be male or female"}
            )

        # 1) 출생 datetime (KST) + 시간 적용 여부
        birth_dt, time_applied = parse_birth_dt_kst(birth, birth_time)

        # 2) 출생 '행정연도' 절기 가져오기 (입춘 비교는 출생연도 입춘으로 판정)
        birth_year = str(birth_dt.year)
        source, fallback, jieqi_list = get_jieqi_with_fallback(birth_year)

        # 3) 입춘 기준 사주연도 계산
        saju_year = resolve_saju_year(birth_dt, jieqi_list)
        ipchun_dt = find_ipchun_dt(jieqi_list)

        # 4) ✅ 일주 계산 (로컬 날짜 기준)
        day_pillar = get_day_pillar(birth_dt.date())

        return {
            "input": {
                "birth": birth,
                "calendar": calendar,
                "birth_time": birth_time,
                "gender": gender
            },
            "pillars": {
                "day": day_pillar
            },
            "jieqi": {
                "year": birth_year,
                "count": len(jieqi_list),
                "items": jieqi_list
            },
            "meta": {
                "source": source,
                "fallback": fallback,
                "birth_dt_kst": birth_dt.isoformat(),
                "ipchun_dt_kst": ipchun_dt.isoformat(),
                "saju_year": saju_year,
                "year_rule": "ipchun_boundary",
                "time_policy": "optional",
                "time_applied": time_applied,
                "day_rule": "gregorian_jdn_offset47"
            }
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )
