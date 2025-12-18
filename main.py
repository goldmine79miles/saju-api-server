from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from datetime import datetime
import json
import os

app = FastAPI(
    title="Saju API Server",
    version="1.0.0"
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JIEQI_TABLE_PATH = os.path.join(BASE_DIR, "data", "jieqi_1900_2052.json")

# =========================
# 기본 유틸
# =========================

def load_jieqi_table():
    with open(JIEQI_TABLE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# =========================
# KASI 관련 (완전 무력화)
# =========================

def fetch_lunar_info(*args, **kwargs):
    """
    ❌ KASI API 호출 제거
    ⭕️ 임시 더미 반환
    """
    return {
        "calendar": "solar",
        "note": "KASI disabled (temporary)"
    }

def fetch_jieqi_from_kasi(*args, **kwargs):
    """
    ❌ 사용 안 함
    """
    return None

# =========================
# 핵심 로직
# =========================

def build_jieqi_list(year: str):
    table = load_jieqi_table()
    year_data = table.get(year)

    if not year_data:
        raise ValueError(f"No jieqi data for year {year}")

    return year_data

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
        # 날짜 파싱
        birth_date = datetime.strptime(birth, "%Y-%m-%d")
        year = str(birth_date.year)

        # ❌ KASI 호출 안 함
        lunar_info = fetch_lunar_info(birth_date)

        # ✅ 절기 테이블만 사용
        jieqi_list = build_jieqi_list(year)

        return {
            "input": {
                "birth": birth,
                "calendar": calendar
            },
            "lunar_info": lunar_info,
            "jieqi": {
                "year": year,
                "count": len(jieqi_list),
                "items": jieqi_list
            },
            "meta": {
                "source": "precomputed_jieqi_table",
                "kasi": "disabled"
            }
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )
