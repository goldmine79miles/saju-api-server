from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
import json
import os

app = FastAPI(
    title="Saju API Server",
    version="1.0.0"
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

    # epoch number
    if isinstance(value, (int, float)):
        v = float(value)
        if v >= 1_000_000_000_000:  # ms
            v = v / 1000.0
        return datetime.fromtimestamp(v, tz=KST)

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None

        # Z -> +00:00
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"

        # ISO
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
            else:
                dt = dt.astimezone(KST)
            return dt
        except Exception:
            pass

        # date only
        try:
            return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=KST)
        except Exception:
            return None

    return None

def find_ipchun_dt(jieqi_list):
    """
    해당 연도 절기 리스트에서 '입춘(立春)' 시각을 찾아 KST datetime으로 반환.
    ✅ 네 JSON 스키마: { name, deg, utc, kst } 형태를 우선 지원.
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

    # ✅ dt 키 후보에 kst/utc 추가, kst 우선
    dt_keys_priority = ["kst", "utc", "dt", "datetime", "time", "at", "iso", "when", "timestamp", "ts"]

    for item in jieqi_list:
        if not isinstance(item, dict):
            continue
        if not _is_ipchun(item):
            continue

        # 1) 우선순위 키에서 바로 찾기
        for k in dt_keys_priority:
            if k in item:
                dt = _parse_dt_any(item.get(k))
                if dt:
                    return dt

        # 2) 중첩 dict도 지원 (혹시 모를 케이스)
        for _, v in item.items():
            if isinstance(v, dict):
                for kk in dt_keys_priority:
                    if kk in v:
                        dt = _parse_dt_any(v.get(kk))
                        if dt:
                            return dt

    raise ValueError("입춘(立春) datetime not found in jieqi table")

def parse_birth_dt_kst(birth: str, birth_time: str):
    """
    birth(YYYY-MM-DD) + birth_time(HH:MM or 'unknown') -> KST aware datetime
    정책:
      - birth_time이 unknown/비정상일 경우 00:00 처리
    """
    base_date = datetime.strptime(birth, "%Y-%m-%d")

    hh, mm = 0, 0
    if isinstance(birth_time, str) and birth_time != "unknown":
        try:
            t = datetime.strptime(birth_time, "%H:%M")
            hh, mm = t.hour, t.minute
        except Exception:
            hh, mm = 0, 0

    return datetime(base_date.year, base_date.month, base_date.day, hh, mm, tzinfo=KST)

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
# Core
# =========================

def get_jieqi_with_fallback(year: str):
    """
    1) KASI 먼저 시도
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
# API
# =========================

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/api/saju/calc")
def calc_saju(
    birth: str = Query(..., description="YYYY-MM-DD"),
    calendar: str = Query("solar", description="solar or lunar"),
    birth_time: str = Query("unknown", description="HH:MM (e.g. 10:00)"),
    gender: str = Query("unknown", description="male or female"),
):
    try:
        if gender not in ("male", "female", "unknown"):
            return JSONResponse(
                status_code=400,
                content={"error": "gender must be male or female"}
            )

        # 1) 출생 datetime (KST)
        birth_dt = parse_birth_dt_kst(birth, birth_time)

        # 2) 출생 '행정연도' 절기 가져오기 (입춘 비교는 출생연도 입춘으로 판정)
        birth_year = str(birth_dt.year)
        source, fallback, jieqi_list = get_jieqi_with_fallback(birth_year)

        # 3) 사주연도 계산
        saju_year = resolve_saju_year(birth_dt, jieqi_list)
        ipchun_dt = find_ipchun_dt(jieqi_list)

        return {
            "input": {
                "birth": birth,
                "calendar": calendar,
                "birth_time": birth_time,
                "gender": gender
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
                "year_rule": "ipchun_boundary"
            }
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )
