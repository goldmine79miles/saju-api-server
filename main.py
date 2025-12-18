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
# KASI (optional)
# =========================

def fetch_jieqi_from_kasi(year: int):
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

    # ⚠️ 실제 절기 파싱은 아직 미구현
    # 여기선 "KASI가 살아있다" 확인 용도
    return {"kasi_alive": True}

# =========================
# Core
# =========================

def get_jieqi_with_fallback(year: str):
    # 1️⃣ KASI 먼저 시도
    try:
        fetch_jieqi_from_kasi(int(year))
        source = "kasi"
    except Exception:
        source = "json"

    # 2️⃣ 실제 데이터는 JSON 테이블 사용
    table = load_jieqi_table()
    year_data = table.get(year)

    if not year_data:
        raise ValueError(f"No jieqi data for year {year}")

    return source, year_data

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
):
    try:
        birth_date = datetime.strptime(birth, "%Y-%m-%d")
        year = str(birth_date.year)

        source, jieqi_list = get_jieqi_with_fallback(year)

        return {
            "input": {
                "birth": birth,
                "calendar": calendar
            },
            "jieqi": {
                "year": year,
                "count": len(jieqi_list),
                "items": jieqi_list
            },
            "meta": {
                "source": source,
                "fallback": source == "json"
            }
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )
