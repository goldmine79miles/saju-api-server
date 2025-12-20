from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from datetime import datetime, date
from zoneinfo import ZoneInfo
import json
import os
import sys
import subprocess
import threading
from pathlib import Path

print("[BOOT] main.py LOADED ✅", os.path.abspath(__file__), flush=True)

app = FastAPI(
    title="Saju API Server",
    version="1.7.4"  # jieqi loader fixed (absolute path + boot log)
)

# ==================================================
# 🔒 PATH FIX (가장 중요)
# ==================================================

# 프로젝트 루트 기준 (main.py 기준 아님)
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
JIEQI_TABLE_PATH = DATA_DIR / "jieqi_1900_2052.json"

KST = ZoneInfo("Asia/Seoul")

# ==================================================
# Jieqi table helpers
# ==================================================

def _is_jieqi_table_usable(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return False

        for y in ("1979", "2000"):
            items = data.get(y)
            if isinstance(items, list) and len(items) == 24:
                return True

        for _, items in data.items():
            if isinstance(items, list) and len(items) == 24:
                return True

        return False
    except Exception:
        return False


def load_jieqi_table():
    if not JIEQI_TABLE_PATH.exists():
        raise FileNotFoundError(
            f"[JIEQI] file missing: {JIEQI_TABLE_PATH} (cwd={Path.cwd()})"
        )

    with JIEQI_TABLE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


# ==================================================
# 🔍 BOOT CHECK (여기서 다 잡힘)
# ==================================================

@app.on_event("startup")
def _startup():
    print("[BOOT] startup event fired ✅", flush=True)
    print(f"[JIEQI] path 붙잡음 → {JIEQI_TABLE_PATH}", flush=True)

    if not JIEQI_TABLE_PATH.exists():
        print("[JIEQI] ❌ file NOT FOUND", flush=True)
        return

    try:
        data = load_jieqi_table()
        print(
            f"[JIEQI] loaded OK ✅ years={len(data)} "
            f"1979_count={len(data.get('1979', []))}",
            flush=True
        )
    except Exception as e:
        print(f"[JIEQI] ❌ load failed: {e}", flush=True)


# ==================================================
# Utils (jieqi)
# ==================================================

def _parse_dt_any(value):
    if value is None:
        return None
    if isinstance(value, str):
        s = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.astimezone(KST) if dt.tzinfo else dt.replace(tzinfo=KST)
    return None

def _pick_item_dt(item):
    for k in ("kst", "utc"):
        if k in item:
            dt = _parse_dt_any(item.get(k))
            if dt:
                return dt
    return None

def find_ipchun_dt(jieqi_list):
    for item in jieqi_list:
        if item.get("name") in ("입춘", "立春"):
            return _pick_item_dt(item)
    raise ValueError("입춘 not found")

def get_jieqi_with_fallback(year: str):
    table = load_jieqi_table()
    year_data = table.get(year)
    if not year_data:
        raise ValueError(f"No jieqi for {year}")
    return "json", True, year_data


# ==================================================
# Pillars (day/year only)
# ==================================================

STEMS = ["甲","乙","丙","丁","戊","己","庚","辛","壬","癸"]
BRANCHES = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"]

def gregorian_to_jdn(y, m, d):
    a = (14 - m) // 12
    y2 = y + 4800 - a
    m2 = m + 12 * a - 3
    return d + (153*m2+2)//5 + 365*y2 + y2//4 - y2//100 + y2//400 - 32045

def get_day_pillar(dt: date):
    idx = (gregorian_to_jdn(dt.year, dt.month, dt.day) + 47) % 60
    return {
        "stem": STEMS[idx % 10],
        "branch": BRANCHES[idx % 12],
        "ganji": STEMS[idx % 10] + BRANCHES[idx % 12],
        "index60": idx
    }

def get_year_pillar(year: int):
    idx = (year - 1984) % 60
    return {
        "stem": STEMS[idx % 10],
        "branch": BRANCHES[idx % 12],
        "ganji": STEMS[idx % 10] + BRANCHES[idx % 12],
        "index60": idx
    }


# ==================================================
# API
# ==================================================

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/saju/calc")
def calc_saju(
    birth: str = Query(...),
    calendar: str = Query("solar"),
    birth_time: str = Query("unknown"),
    gender: str = Query("unknown"),
):
    try:
        birth_date = datetime.strptime(birth, "%Y-%m-%d")
        time_applied = birth_time != "unknown"
        if time_applied:
            hh, mm = map(int, birth_time.split(":"))
        else:
            hh, mm = 0, 0

        birth_dt = datetime(
            birth_date.year, birth_date.month, birth_date.day,
            hh, mm, tzinfo=KST
        )

        source, fallback, jieqi_this = get_jieqi_with_fallback(str(birth_dt.year))
        ipchun_dt = find_ipchun_dt(jieqi_this)

        saju_year = birth_dt.year if birth_dt >= ipchun_dt else birth_dt.year - 1

        year_pillar = get_year_pillar(saju_year)
        day_pillar = get_day_pillar(birth_dt.date())

        return {
            "input": {"birth": birth, "calendar": calendar, "birth_time": birth_time, "gender": gender},
            "pillars": {"year": year_pillar, "month": None, "day": day_pillar, "hour": None},
            "jieqi": {"year": str(birth_dt.year), "count": len(jieqi_this), "items": jieqi_this},
            "meta": {
                "version": "v1",
                "source": source,
                "fallback": fallback,
                "debug": {
                    "birth_dt_kst": birth_dt.isoformat(),
                    "ipchun_dt_kst": ipchun_dt.isoformat(),
                    "saju_year": saju_year,
                    "time_applied": time_applied
                }
            }

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
