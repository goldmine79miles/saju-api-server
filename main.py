from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from datetime import datetime, date, timedelta
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
    version="1.7.8"  # ✅ day pillar offset fix (+48) to match common almanac
)

# ==================================================
# PATHS
# ==================================================
THIS_DIR = Path(__file__).resolve().parent

PROJECT_ROOT = THIS_DIR
if not (PROJECT_ROOT / "data").exists() and (PROJECT_ROOT.parent / "data").exists():
    PROJECT_ROOT = PROJECT_ROOT.parent

DATA_DIR = PROJECT_ROOT / "data"
TOOLS_DIR = PROJECT_ROOT / "tools"

JIEQI_TABLE_PATH = DATA_DIR / "jieqi_1900_2052.json"
KST = ZoneInfo("Asia/Seoul")

# =========================
# Jieqi helpers
# =========================

def load_jieqi_table():
    if not JIEQI_TABLE_PATH.exists():
        raise FileNotFoundError(f"[JIEQI] missing file: {JIEQI_TABLE_PATH}")
    with JIEQI_TABLE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)

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

# ============================
# Pillars (day/year)
# ============================

STEMS = ["甲","乙","丙","丁","戊","己","庚","辛","壬","癸"]
BRANCHES = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"]

# 🔒 LOCKED: Jeomshin verified day pillar offset
DAY_PILLAR_JDN_OFFSET = 49  # 절대 수정 금지

def gregorian_to_jdn(y, m, d):
    a = (14 - m) // 12
    y2 = y + 4800 - a
    m2 = m + 12 * a - 3
    return d + (153*m2+2)//5 + 365*y2 + y2//4 - y2//100 + y2//400 - 32045

def get_day_pillar(dt: date):
    idx = (gregorian_to_jdn(dt.year, dt.month, dt.day) + DAY_PILLAR_JDN_OFFSET) % 60
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

# =========================
# Month pillar (점신식 날짜 경계)
# =========================

MONTH_TERM_TO_BRANCH = [
    ("입춘", "寅"), ("경칩", "卯"), ("청명", "辰"), ("입하", "巳"),
    ("망종", "午"), ("소서", "未"), ("입추", "申"), ("백로", "酉"),
    ("한로", "戌"), ("입동", "亥"), ("대설", "子"), ("소한", "丑"),
]

YEAR_STEM_TO_YIN_MONTH_STEM = {
    "甲": "丙", "己": "丙",
    "乙": "戊", "庚": "戊",
    "丙": "庚", "辛": "庚",
    "丁": "壬", "壬": "壬",
    "戊": "甲", "癸": "甲",
}

MONTH_BRANCH_SEQ = ["寅","卯","辰","巳","午","未","申","酉","戌","亥","子","丑"]

def _jieqi_term_dt_map(jieqi_list):
    m = {}
    for item in jieqi_list:
        name = item.get("name")
        dt = _pick_item_dt(item)
        if name and dt:
            m[name] = dt
    return m

def _get_month_branch_from_terms(birth_dt, this_year_terms, prev_year_terms):
    def boundary_next_midnight(dt):
        d = dt.astimezone(KST).date() + timedelta(days=1)
        return datetime(d.year, d.month, d.day, 0, 0, tzinfo=KST)

    candidates = []
    for term, branch in MONTH_TERM_TO_BRANCH:
        dt = this_year_terms.get(term)
        if dt:
            candidates.append((boundary_next_midnight(dt), branch))

    prev_daeseol = prev_year_terms.get("대설")
    if prev_daeseol:
        candidates.append((boundary_next_midnight(prev_daeseol), "子"))

    valid = [c for c in candidates if c[0] <= birth_dt]
    if not valid:
        return "丑"

    valid.sort(key=lambda x: x[0])
    return valid[-1][1]

def get_month_pillar(birth_dt, saju_year_pillar, jieqi_this_year, jieqi_prev_year):
    this_map = _jieqi_term_dt_map(jieqi_this_year)
    prev_map = _jieqi_term_dt_map(jieqi_prev_year)

    month_branch = _get_month_branch_from_terms(birth_dt, this_map, prev_map)

    year_stem = saju_year_pillar["stem"]
    yin_month_stem = YEAR_STEM_TO_YIN_MONTH_STEM[year_stem]

    month_index = MONTH_BRANCH_SEQ.index(month_branch)
    stem_index = (STEMS.index(yin_month_stem) + month_index) % 10
    month_stem = STEMS[stem_index]

    return {"stem": month_stem, "branch": month_branch, "ganji": month_stem + month_branch}

# =========================
# Hour pillar
# =========================

HOUR_BRANCH_SEQ = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"]

DAY_STEM_TO_ZI_HOUR_STEM = {
    "甲": "甲", "己": "甲",
    "乙": "丙", "庚": "丙",
    "丙": "戊", "辛": "戊",
    "丁": "庚", "壬": "庚",
    "戊": "壬", "癸": "壬",
}

def _get_hour_branch(hh, mm):
    total = hh * 60 + mm
    shifted = (total - 23 * 60) % (24 * 60)
    return HOUR_BRANCH_SEQ[int(shifted // 120)]

def get_hour_pillar(day_pillar, hh, mm):
    hour_branch = _get_hour_branch(hh, mm)
    zi_hour_stem = DAY_STEM_TO_ZI_HOUR_STEM[day_pillar["stem"]]
    stem_index = (STEMS.index(zi_hour_stem) + HOUR_BRANCH_SEQ.index(hour_branch)) % 10
    hour_stem = STEMS[stem_index]
    return {"stem": hour_stem, "branch": hour_branch, "ganji": hour_stem + hour_branch}

# =========================
# API
# =========================

@app.get("/api/saju/calc")
def calc_saju(
    birth: str = Query(...),
    calendar: str = Query("solar"),
    birth_time: str = Query("unknown"),
    gender: str = Query("unknown"),
):
    birth_date = datetime.strptime(birth, "%Y-%m-%d")

    bt = (birth_time or "").strip().lower()
    if bt and bt not in ("unknown", "null", "none"):
        hh, mm = map(int, bt.split(":"))
        has_time = True
    else:
        hh, mm = 0, 0
        has_time = False

    birth_dt = datetime(
        birth_date.year, birth_date.month, birth_date.day,
        hh, mm, tzinfo=KST
    )

    _, _, jieqi_this = get_jieqi_with_fallback(str(birth_dt.year))
    ipchun_dt = find_ipchun_dt(jieqi_this)
    saju_year = birth_dt.year if birth_dt >= ipchun_dt else birth_dt.year - 1

    year_pillar = get_year_pillar(saju_year)
    day_pillar = get_day_pillar(birth_dt.date())

    _, _, jieqi_prev = get_jieqi_with_fallback(str(birth_dt.year - 1))
    month_pillar = get_month_pillar(birth_dt, year_pillar, jieqi_this, jieqi_prev)

    hour_pillar = get_hour_pillar(day_pillar, hh, mm) if has_time else None

    return {
        "input": {
            "birth": birth,
            "calendar": calendar,
            "birth_time": birth_time,
            "gender": gender,
        },
        "pillars": {
            "year": year_pillar,
            "month": month_pillar,
            "day": day_pillar,
            "hour": hour_pillar
        },
        "jieqi": jieqi_this,
        "debug": {
            "timezone": "KST",
            "saju_year": saju_year
        }
    }
