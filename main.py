from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from datetime import datetime, date
from zoneinfo import ZoneInfo
import requests
import json
import os

app = FastAPI(
    title="Saju API Server",
    version="1.6.0"  # hour pillar added
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

def _pick_item_dt(item):
    if isinstance(item, dict):
        if "kst" in item:
            dt = _parse_dt_any(item.get("kst"))
            if dt:
                return dt
        if "utc" in item:
            dt = _parse_dt_any(item.get("utc"))
            if dt:
                return dt
        for k in ["dt", "datetime", "time", "at", "iso", "when", "timestamp", "ts"]:
            if k in item:
                dt = _parse_dt_any(item.get(k))
                if dt:
                    return dt
    return None

def find_ipchun_dt(jieqi_list):
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
        return ("입춘" in joined) or ("立春" in joined) or ("IPCHUN" in joined.upper())

    for item in jieqi_list:
        if not isinstance(item, dict):
            continue
        if not _is_ipchun(item):
            continue
        dt = _pick_item_dt(item)
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
    ipchun_dt = find_ipchun_dt(birth_year_jieqi_list)
    y = birth_dt_kst.year
    return y if birth_dt_kst >= ipchun_dt else y - 1

# =========================
# Core (Pillars)
# =========================

STEMS = ["甲","乙","丙","丁","戊","己","庚","辛","壬","癸"]
BRANCHES = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"]

def gregorian_to_jdn(y: int, m: int, d: int) -> int:
    a = (14 - m) // 12
    y2 = y + 4800 - a
    m2 = m + 12 * a - 3
    return d + (153 * m2 + 2) // 5 + 365 * y2 + y2 // 4 - y2 // 100 + y2 // 400 - 32045

def get_day_pillar(local_date: date):
    jdn = gregorian_to_jdn(local_date.year, local_date.month, local_date.day)
    day_index = (jdn + 47) % 60
    stem = STEMS[day_index % 10]
    branch = BRANCHES[day_index % 12]
    return {"stem": stem, "branch": branch, "ganji": stem + branch, "index60": day_index}

def get_year_pillar(saju_year: int):
    index60 = (saju_year - 1984) % 60  # 1984 = 甲子
    stem = STEMS[index60 % 10]
    branch = BRANCHES[index60 % 12]
    return {"stem": stem, "branch": branch, "ganji": stem + branch, "index60": index60}

# =========================
# Month Pillar (deg-based major terms)
# =========================

MAJOR_TERMS = [
    ("입춘", "寅", 2),
    ("경칩", "卯", 3),
    ("청명", "辰", 4),
    ("입하", "巳", 5),
    ("망종", "午", 6),
    ("소서", "未", 7),
    ("입추", "申", 8),
    ("백로", "酉", 9),
    ("한로", "戌", 10),
    ("입동", "亥", 11),
    ("대설", "子", 12),
    ("소한", "丑", 1),
]
MAJOR_NAME_TO_BRANCH = {n: b for (n, b, _) in MAJOR_TERMS}
MAJOR_NAME_TO_EXPECTED_MONTH = {n: m for (n, _, m) in MAJOR_TERMS}
BRANCH_ORDER_FROM_YIN = ["寅","卯","辰","巳","午","未","申","酉","戌","亥","子","丑"]

YEAR_STEM_TO_YIN_MONTH_STEM = {
    "甲": "丙", "己": "丙",
    "乙": "戊", "庚": "戊",
    "丙": "庚", "辛": "庚",
    "丁": "壬", "壬": "壬",
    "戊": "甲", "癸": "甲",
}

def _month_close(expected: int, actual: int) -> bool:
    if expected == actual:
        return True
    if expected == 1 and actual == 12:
        return True
    if expected == 12 and actual == 1:
        return True
    return abs(expected - actual) == 1

def extract_major_terms(jieqi_list: list):
    majors = []
    if not isinstance(jieqi_list, list):
        return majors

    for item in jieqi_list:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if name not in MAJOR_NAME_TO_BRANCH:
            continue

        dt = _pick_item_dt(item)
        if not dt:
            continue

        exp_m = MAJOR_NAME_TO_EXPECTED_MONTH.get(name)
        if exp_m is not None and not _month_close(exp_m, dt.month):
            continue

        majors.append({"name": name, "branch": MAJOR_NAME_TO_BRANCH[name], "dt": dt})

    if len(majors) == 0:
        for item in jieqi_list:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            if name not in MAJOR_NAME_TO_BRANCH:
                continue
            dt = _pick_item_dt(item)
            if dt:
                majors.append({"name": name, "branch": MAJOR_NAME_TO_BRANCH[name], "dt": dt})

    majors.sort(key=lambda x: x["dt"])
    return majors

def compute_month_branch(birth_dt: datetime, majors_prev: list, majors_this: list) -> str:
    timeline = []
    timeline.extend(majors_prev)
    timeline.extend(majors_this)
    timeline = [x for x in timeline if isinstance(x.get("dt"), datetime)]
    timeline.sort(key=lambda x: x["dt"])

    chosen = None
    for x in timeline:
        if x["dt"] <= birth_dt:
            chosen = x
        else:
            break

    if chosen is None:
        return "丑"
    return chosen["branch"]

def compute_month_stem(year_stem: str, month_branch: str) -> str:
    start_stem = YEAR_STEM_TO_YIN_MONTH_STEM.get(year_stem, "丙")
    start_idx = STEMS.index(start_stem)
    offset = BRANCH_ORDER_FROM_YIN.index(month_branch)
    return STEMS[(start_idx + offset) % 10]

def get_month_pillar(birth_dt: datetime, year_stem: str, jieqi_prev: list, jieqi_this: list):
    majors_prev = extract_major_terms(jieqi_prev)
    majors_this = extract_major_terms(jieqi_this)
    month_branch = compute_month_branch(birth_dt, majors_prev, majors_this)
    month_stem = compute_month_stem(year_stem, month_branch)
    return {
        "stem": month_stem,
        "branch": month_branch,
        "ganji": month_stem + month_branch,
        "rule": "major_terms_only + expected_month_filter + yearstem_mapping"
    }

# =========================
# ✅ Hour Pillar (NEW)
# =========================

# 시지(時支): 2시간 단위, 子시는 23:00~00:59
HOUR_BRANCHES = [
    ("子", 23, 1),
    ("丑", 1, 3),
    ("寅", 3, 5),
    ("卯", 5, 7),
    ("辰", 7, 9),
    ("巳", 9, 11),
    ("午", 11, 13),
    ("未", 13, 15),
    ("申", 15, 17),
    ("酉", 17, 19),
    ("戌", 19, 21),
    ("亥", 21, 23),
]

# 일간 기준 子시 시작 천간
# 甲己日 甲子 / 乙庚日 丙子 / 丙辛日 戊子 / 丁壬日 庚子 / 戊癸日 壬子
DAY_STEM_TO_ZI_HOUR_STEM = {
    "甲": "甲", "己": "甲",
    "乙": "丙", "庚": "丙",
    "丙": "戊", "辛": "戊",
    "丁": "庚", "壬": "庚",
    "戊": "壬", "癸": "壬",
}

BRANCH_ORDER_FROM_ZI = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"]

def get_hour_branch(hour: int, minute: int) -> str:
    # 子시: 23:00~00:59
    if hour == 23 or hour == 0:
        return "子"
    # 그 외 2시간 구간
    for br, start_h, end_h in HOUR_BRANCHES[1:]:
        if start_h <= hour < end_h:
            return br
    # 안전 fallback
    return "子"

def get_hour_stem(day_stem: str, hour_branch: str) -> str:
    start_stem = DAY_STEM_TO_ZI_HOUR_STEM.get(day_stem, "甲")  # 子시 시작 천간
    start_idx = STEMS.index(start_stem)
    offset = BRANCH_ORDER_FROM_ZI.index(hour_branch)  # 子=0..亥=11
    return STEMS[(start_idx + offset) % 10]

def get_hour_pillar(birth_dt: datetime, time_applied: bool, day_stem: str):
    if not time_applied:
        return None
    hb = get_hour_branch(birth_dt.hour, birth_dt.minute)
    hs = get_hour_stem(day_stem, hb)
    return {
        "stem": hs,
        "branch": hb,
        "ganji": hs + hb,
        "rule": "2h_blocks + zi_at_23_00 + daystem_mapping"
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
            return JSONResponse(status_code=400, content={"error": "gender must be male or female"})

        birth_dt, time_applied = parse_birth_dt_kst(birth, birth_time)

        birth_year = str(birth_dt.year)
        source, fallback, jieqi_this = get_jieqi_with_fallback(birth_year)

        saju_year = resolve_saju_year(birth_dt, jieqi_this)
        ipchun_dt = find_ipchun_dt(jieqi_this)

        prev_year = str(birth_dt.year - 1)
        try:
            _, _, jieqi_prev = get_jieqi_with_fallback(prev_year)
        except Exception:
            jieqi_prev = []

        day_pillar = get_day_pillar(birth_dt.date())
        year_pillar = get_year_pillar(saju_year)
        month_pillar = get_month_pillar(
            birth_dt=birth_dt,
            year_stem=year_pillar["stem"],
            jieqi_prev=jieqi_prev,
            jieqi_this=jieqi_this,
        )

        # ✅ 시주 추가
        hour_pillar = get_hour_pillar(
            birth_dt=birth_dt,
            time_applied=time_applied,
            day_stem=day_pillar["stem"]
        )

        return {
            "input": {"birth": birth, "calendar": calendar, "birth_time": birth_time, "gender": gender},
            "pillars": {
                "year": year_pillar,
                "month": month_pillar,
                "day": day_pillar,
                "hour": hour_pillar
            },
            "jieqi": {"year": birth_year, "count": len(jieqi_this), "items": jieqi_this},
            "meta": {
                "source": source,
                "fallback": fallback,
                "birth_dt_kst": birth_dt.isoformat(),
                "ipchun_dt_kst": ipchun_dt.isoformat(),
                "saju_year": saju_year,
                "year_rule": "ipchun_boundary",
                "time_policy": "optional",
                "time_applied": time_applied,
                "day_rule": "gregorian_jdn_offset47",
                "year_rule2": "base1984_gapja",
                "month_rule": "major_terms_only + expected_month_filter + yearstem_mapping",
                "hour_rule": "2h_blocks + zi_at_23_00 + daystem_mapping"
            }
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
