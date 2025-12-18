from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from datetime import datetime, date
from zoneinfo import ZoneInfo
import requests
import json
import os

app = FastAPI(
    title="Saju API Server",
    version="1.4.0"  # jieqi check added
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
        if "입춘" in joined or "立春" in joined or "IPCHUN" in joined.upper():
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
    index60 = (saju_year - 1984) % 60
    stem = STEMS[index60 % 10]
    branch = BRANCHES[index60 % 12]
    return {"stem": stem, "branch": branch, "ganji": stem + branch, "index60": index60}

# =========================
# ✅ Jieqi Check (NEW)
# =========================

# 24절기 이름(한글) - 검증용(완전일치 강요 X, 참고용)
JIEQI_NAMES_24 = {
    "입춘","우수","경칩","춘분","청명","곡우",
    "입하","소만","망종","하지","소서","대서",
    "입추","처서","백로","추분","한로","상강",
    "입동","소설","대설","동지","소한","대한"
}

def _pick_item_dt(item):
    # kst 우선, 없으면 utc
    if isinstance(item, dict):
        if "kst" in item:
            return _parse_dt_any(item.get("kst"))
        if "utc" in item:
            return _parse_dt_any(item.get("utc"))
        # 그 외 후보
        for k in ["dt","datetime","time","at","iso","when","timestamp","ts"]:
            if k in item:
                dt = _parse_dt_any(item.get(k))
                if dt:
                    return dt
    return None

def check_jieqi_year(year: str, jieqi_list: list):
    """
    월주 계산에 ‘안전하게’ 쓸 수 있는지 진단:
    - count=24인지
    - dt 파싱 가능한지
    - 시간 중복(같은 kst)이 과도한지
    - deg 값 분포/중복
    - name이 24절기 셋에 얼마나 포함되는지(참고)
    """
    issues = []
    stats = {}

    if not isinstance(jieqi_list, list):
        return {
            "ok": False,
            "year": year,
            "issues": ["jieqi_list is not a list"],
            "stats": {}
        }

    stats["count"] = len(jieqi_list)
    if len(jieqi_list) != 24:
        issues.append(f"count is {len(jieqi_list)} (expected 24)")

    # dt 파싱 + 중복 체크
    dts = []
    dt_raws = []
    for it in jieqi_list:
        dt = _pick_item_dt(it)
        if dt:
            dts.append(dt)
            dt_raws.append(dt.isoformat())
        else:
            issues.append("some items missing parseable datetime (kst/utc)")
            break

    # dt 중복
    if dts:
        uniq_dt = len(set(dt_raws))
        stats["unique_datetimes"] = uniq_dt
        stats["duplicate_datetimes"] = len(dts) - uniq_dt
        if uniq_dt < 20:  # 너무 심한 중복이면 위험
            issues.append(f"too many duplicate datetimes: unique={uniq_dt}/24")

        # 정렬 검사(시간순)
        sorted_ok = (dt_raws == [x.isoformat() for x in sorted(dts)])
        stats["datetime_sorted"] = sorted_ok
        if not sorted_ok:
            issues.append("datetimes are not sorted ascending (month pillar needs reliable ordering)")

    # deg 분포 체크
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
        # deg 범위 체크
        bad = [d for d in degs if d < 0 or d >= 360]
        if bad:
            issues.append("deg has out-of-range values")
    else:
        issues.append("deg field missing (recommended for month pillar)")

    # name 포함률(참고 지표)
    names = []
    for it in jieqi_list:
        if isinstance(it, dict) and isinstance(it.get("name"), str):
            names.append(it["name"])
    if names:
        in_set = sum(1 for n in names if n in JIEQI_NAMES_24)
        stats["name_in_24set"] = in_set
        stats["name_unknown"] = len(names) - in_set
        # 이름이 절반 이하로 엉망이면 경고
        if in_set < 18:
            issues.append(f"many jieqi names not in 24-set: in_set={in_set}/24 (month pillar should rely on deg)")
    else:
        issues.append("name field missing (not critical if deg+dt are reliable)")

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
        source, fallback, jieqi_list = get_jieqi_with_fallback(birth_year)

        saju_year = resolve_saju_year(birth_dt, jieqi_list)
        ipchun_dt = find_ipchun_dt(jieqi_list)

        day_pillar = get_day_pillar(birth_dt.date())
        year_pillar = get_year_pillar(saju_year)

        return {
            "input": {"birth": birth, "calendar": calendar, "birth_time": birth_time, "gender": gender},
            "pillars": {"year": year_pillar, "day": day_pillar},
            "jieqi": {"year": birth_year, "count": len(jieqi_list), "items": jieqi_list},
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
                "year_rule2": "base1984_gapja"
            }
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
