from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from datetime import datetime
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

# =========================
# Utils
# =========================

def load_jieqi_table():
    with open(JIEQI_TABLE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

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
        # 기본 검증
        if gender not in ("male", "female", "unknown"):
            return JSONResponse(
                status_code=400,
                content={"error": "gender must be male or female"}
            )

        birth_date = datetime.strptime(birth, "%Y-%m-%d")
        year = str(birth_date.year)

        source, fallback, jieqi_list = get_jieqi_with_fallback(year)

        return {
            "input": {
                "birth": birth,
                "calendar": calendar,
                "birth_time": birth_time,
                "gender": gender
            },
            "jieqi": {
                "year": year,
                "count": len(jieqi_list),
                "items": jieqi_list
            },
            "meta": {
                "source": source,
                "fallback": fallback
            }
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )
