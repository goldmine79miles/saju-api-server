from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from datetime import datetime, date
from zoneinfo import ZoneInfo
import requests
import json
import os

app = FastAPI(
    title="Saju API Server",
    version="1.5.0"  # month pillar (deg-based) added
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
    # kst 우선, 없으면 utc
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
# ✅ Month Pillar (deg-based major terms)
# =========================

# 월지(寅~丑)는 "절(節)" 12개 기준: 입춘부터 시작
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

# 연간 -> 寅월 시작 월천간 규칙
# 甲己年 丙寅 / 乙庚年 戊寅 / 丙辛年 庚寅 / 丁壬年 壬寅 / 戊癸年 甲寅
YEAR_STEM_TO_YIN_MONTH_STEM = {
    "甲": "丙", "己": "丙",
    "乙": "戊", "庚": "戊",
    "丙": "庚", "辛": "庚",
    "丁": "壬", "壬": "壬",
    "戊": "甲", "癸": "甲",
}

def _month_close(expected: int, actual: int) -> bool:
    # ±1 month 허용 (1~12 wrap)
    if expected == actual:
        return True
    if expected == 1 and actual == 12:
        return True
    if expected == 12 and actual == 1:
        return True
    return abs(expected - actual) == 1

def extract_major_terms(year: str, jieqi_list: list):
    """
    절기 테이블 dt가 중복(반쯤 복붙)된 상태를 감안해서:
    - '절(節)' 12개만 뽑고
    - 각 절기에 대해 '예상월'에 맞는 dt만 채택
    """
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
            # 반년 뒤로 복붙된 잘못된 dt로 판단 -> 버림
            continue

        majors.append({
            "name": name,
            "branch": MAJOR_NAME_TO_BRANCH[name],
            "dt": dt,
        })

    # 혹시 하나도 못뽑으면(극단 케이스) dt 필터 없이라도 뽑기
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
    """
    출생 시각 기준으로 가장 최근의 '절(節)'을 찾고 그에 해당하는 월지를 리턴.
    """
    timeline = []
    timeline.extend(majors_prev)
    timeline.extend(majors_this)
    timeline = [x for x in timeline if isinstance(x.get("dt"), datetime)]
    timeline.sort(key=lambda x: x["dt"])

    # 가장 최근 절(節)
    chosen = None
    for x in timeline:
        if x["dt"] <= birth_dt:
            chosen = x
        else:
            break

    # 출생이 아주 이른 경우(1월 초) chosen이 없을 수 있음 -> 소한(丑)로 fallback
    if chosen is None:
        return "丑"
    return chosen["branch"]

def compute_month_stem(year_stem: str, month_branch: str) -> str:
    """
    월천간: 寅월 시작 천간 + (월지 offset)로 계산
    """
    start_stem = YEAR_STEM_TO_YIN_MONTH_STEM.get(year_stem)
    if not start_stem:
        # 비정상 입력이면 그냥 안전 fallback
        start_stem = "丙"

    start_idx = STEMS.index(start_stem)
    offset = BRANCH_ORDER_FROM_YIN.index(month_branch)  # 寅=0 ... 丑=11
    stem = STEMS[(start_idx + offset) % 10]
    return stem

def get_month_pillar(birth_dt: datetime, year_stem: str, jieqi_prev: list, jieqi_this: list):
    majors_prev = extract_major_terms("prev", jieqi_prev)
    majors_this = extract_major_terms("this", jieqi_this)
    month_branch = compute_month_branch(birth_dt, majors_prev, majors_this)
    month_stem = compute_month_stem(year_stem, month_branch)
    return {
        "stem": month_stem,
        "branch": month_branch,
        "ganji": month_stem + month_branch,
        "rule": "major_terms_deg_name_expected_month_filter"
    }

# =========================
# Jieqi Check (existing)
# =========================

JIEQI_NAMES_24 = {
    "입춘","우수","경칩","춘분","청명","곡우",
    "입하","소만","망종","하지","소서","대서",
    "입추","처서","백로","추분","한로","상강",
    "입동","소설","대설","동지","소한","대한"
}

def check_jieqi_year(year: str, jieqi_list: list):
    issues = []
    stats = {}

    if not isinstance(jieqi_list, list):
        return {"ok": False, "year": year, "issues": ["jieqi_list is not a list"], "stats": {}}

    stats["count"] = len(jieqi_list)
    if len(jieqi_list) != 24:
        issues.append(f"count is {len(jieqi_list)} (expected 24)")

    # dt 파싱 + 중복 체크
    dt_raws = []
    for it in jieqi_list:
        dt = _pick_item_dt(it)
        if dt:
            dt_raws.append(dt.isoformat())
        else:
            issues.append("some items missing parseable datetime (kst/utc)")
            break

    if dt_raws:
        uniq_dt = len(set(dt_raws))
        stats["unique_datetimes"] = uniq_dt
        stats["duplicate_datetimes"] = len(dt_raws) - uniq_dt
        if uniq_dt < 20:
            issues.append(f"too many duplicate datetimes: unique={uniq_dt}/24")
        stats["datetime_sorted"] = (dt_raws == sorted(dt_raws))

    # deg 분포
    degs = []
    for it in jieqi_list:
        if isinstance(it, dict) and "deg" in it:
            try:
                degs.append(int(it.get("deg")))
            except Exception:
                pass
    if degs:
        stats["unique_degs"] = len(set(degs))
        stats["duplicate_degs"] = 24 - len(set(degs))
        if len(set(degs)) < 20:
            issues.append(f"too many duplicate deg values: unique={len(set(degs))}/24")
    else:
        issues.append("deg field missing (recommended for month pillar)")

    # name 포함률
    names = []
    for it in jieqi_list:
        if isinstance(it, dict) and isinstance(it.get("name"), str):
            names.append(it["name"])
    if names:
        in_set = sum(1 for n in names if n in JIEQI_NAMES_24)
        stats["name_in_24set"] = in_set
        stats["name_unknown"] = len(names) - in_set

    ok = (len(issues) == 0)
    return {"ok": ok, "year": year, "issues": issues, "stats": stats}

# =========================
# API
# =========================

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/api/jieqi/check")
def jieqi_check(
    year: str = Query(..., description="YYYY (e.g. 1979)")
):
    try:
        source, fallback, jieqi_list = get_jieqi_with_fallback(year)
        result = check_jieqi_year(year, jieqi_list)
        result["meta"] = {"source": source, "fallback": fallback}
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

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

        # prev year jieqi (for Jan/early Feb month boundary safety)
        prev_year = str(birth_dt.year - 1)
        try:
            _, _, jieqi_prev = get_jieqi_with_fallback(prev_year)
        except Exception:
            jieqi_prev = []

        day_pillar = get_day_pillar(birth_dt.date())
        year_pillar = get_year_pillar(saju_year)

        # ✅ 월주 추가 (deg 기반 / 절 12개만)
        month_pillar = get_month_pillar(
            birth_dt=birth_dt,
            year_stem=year_pillar["stem"],
            jieqi_prev=jieqi_prev,
            jieqi_this=jieqi_this,
        )

        return {
            "input": {"birth": birth, "calendar": calendar, "birth_time": birth_time, "gender": gender},
            "pillars": {
                "year": year_pillar,
                "month": month_pillar,
                "day": day_pillar
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
                "month_rule": "major_terms_only + expected_month_filter + yearstem_mapping"
            }
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
