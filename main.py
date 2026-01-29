from fastapi import FastAPI, Query
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
import json
import os
from pathlib import Path
import requests
import xml.etree.ElementTree as ET

print("[BOOT] main.py LOADED ✅", os.path.abspath(__file__), flush=True)

app = FastAPI(
    title="Saju API Server",
    version="1.9.0"
)

# ==================================================
# PATHS
# ==================================================
THIS_DIR = Path(__file__).resolve().parent

PROJECT_ROOT = THIS_DIR
if not (PROJECT_ROOT / "data").exists() and (PROJECT_ROOT.parent / "data").exists():
    PROJECT_ROOT = PROJECT_ROOT.parent

DATA_DIR = PROJECT_ROOT / "data"
JIEQI_TABLE_PATH = DATA_DIR / "jieqi_1900_2052.json"

KST = ZoneInfo("Asia/Seoul")
UTC = timezone.utc

SEOUL_FIXED_OFFSET_MINUTES = 32


# ==================================================
# Birth Confirmed SSOT Cache (KASI 장애 대비)
# - Goal: once a conversion is computed, reuse it deterministically.
# - Storage here is a local JSON cache file (API-level). Front/DB can persist the same blob as SSOT.
# ==================================================
BIRTH_CONFIRMED_CACHE_PATH = DATA_DIR / "birth_confirmed_cache.json"

def _load_birth_confirmed_cache() -> dict:
    try:
        if not BIRTH_CONFIRMED_CACHE_PATH.exists():
            return {}
        with BIRTH_CONFIRMED_CACHE_PATH.open("r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}

def _save_birth_confirmed_cache(cache: dict) -> None:
    try:
        BIRTH_CONFIRMED_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = BIRTH_CONFIRMED_CACHE_PATH.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        tmp.replace(BIRTH_CONFIRMED_CACHE_PATH)
    except Exception:
        # cache is best-effort only
        return

def _bc_key(birth: str, calendar: str, is_leap_month: bool) -> str:
    cal = (calendar or "solar").lower()
    return f"{cal}|{birth}|leap={1 if is_leap_month else 0}"

def _try_fallback_lunar_to_solar(lun_year: int, lun_month: int, lun_day: int, is_leap: bool):
    """Fallback lunar->solar conversion when KASI is unavailable.
    Tries korean_lunar_calendar first if installed. Returns dict or None.
    """
    try:
        from korean_lunar_calendar import KoreanLunarCalendar  # type: ignore
        cal = KoreanLunarCalendar()
        cal.setLunarDate(lun_year, lun_month, lun_day, is_leap)
        y, m, d = map(int, cal.SolarIsoFormat().split("-"))
        return {"year": y, "month": m, "day": d}
    except Exception:
        return None

def build_birth_confirmed_json(
    birth: str,
    calendar: str,
    is_leap_month: bool,
    calc_dt_iso: str,
    fixed_offset_minutes: int,
) -> dict:
    """Build SSOT birth_confirmed_json.
    Priority:
      1) Local cache hit -> source='cache'
      2) KASI -> source='KASI' (and write cache)
      3) Fallback lib -> source='fallback' (and write cache)
    """
    key = _bc_key(birth, calendar, bool(is_leap_month))
    cache = _load_birth_confirmed_cache()
    if key in cache and isinstance(cache.get(key), dict):
        out = cache[key]
        out["calc_dt"] = calc_dt_iso
        out["fixed_offset_minutes"] = fixed_offset_minutes
        out["source"] = out.get("source") or "cache"
        return out

    # Parse input date
    bd = datetime.strptime(birth, "%Y-%m-%d").date()
    cal = (calendar or "solar").lower()

    try:
        if cal == "lunar":
            sol = kasi_lun_to_sol(bd.year, bd.month, bd.day, bool(is_leap_month))
            solar = {"year": sol["year"], "month": sol["month"], "day": sol["day"],
                     "label_kr": f"양력 {sol['year']}년 {sol['month']}월 {sol['day']}일"}
        else:
            solar = {"year": bd.year, "month": bd.month, "day": bd.day,
                     "label_kr": f"양력 {bd.year}년 {bd.month}월 {bd.day}일"}

        lunar = kasi_sol_to_lun(solar["year"], solar["month"], solar["day"])
        out = {
            "solar": solar,
            "lunar": lunar,
            "calc_dt": calc_dt_iso,
            "fixed_offset_minutes": fixed_offset_minutes,
            "source": "KASI",
        }
        cache[key] = out
        _save_birth_confirmed_cache(cache)
        return out
    except Exception:
        # KASI failed -> fallback only for lunar input. For solar input, we can still proceed with solar.
        if cal == "solar":
            try:
                lunar = kasi_sol_to_lun(bd.year, bd.month, bd.day)  # may still fail; ok
                out = {
                    "solar": {"year": bd.year, "month": bd.month, "day": bd.day,
                              "label_kr": f"양력 {bd.year}년 {bd.month}월 {bd.day}일"},
                    "lunar": lunar,
                    "calc_dt": calc_dt_iso,
                    "fixed_offset_minutes": fixed_offset_minutes,
                    "source": "fallback",
                }
                cache[key] = out
                _save_birth_confirmed_cache(cache)
                return out
            except Exception:
                # absolute fallback: keep only solar
                out = {
                    "solar": {"year": bd.year, "month": bd.month, "day": bd.day,
                              "label_kr": f"양력 {bd.year}년 {bd.month}월 {bd.day}일"},
                    "lunar": {"year": None, "month": None, "day": None, "is_leap_month": False, "label_kr": "", "_raw": {}},
                    "calc_dt": calc_dt_iso,
                    "fixed_offset_minutes": fixed_offset_minutes,
                    "source": "fallback",
                }
                cache[key] = out
                _save_birth_confirmed_cache(cache)
                return out

        # lunar input: need lunar->solar to compute pillars
        fb = _try_fallback_lunar_to_solar(bd.year, bd.month, bd.day, bool(is_leap_month))
        if not fb:
            raise
        solar = {"year": fb["year"], "month": fb["month"], "day": fb["day"],
                 "label_kr": f"양력 {fb['year']}년 {fb['month']}월 {fb['day']}일"}
        # Try to derive lunar label from input
        lunar_label = f"음력 {bd.year}년 " + (f"윤{bd.month}월 " if bool(is_leap_month) else f"{bd.month}월 ") + f"{bd.day}일"
        out = {
            "solar": solar,
            "lunar": {"year": bd.year, "month": bd.month, "day": bd.day, "is_leap_month": bool(is_leap_month), "label_kr": lunar_label, "_raw": {}},
            "calc_dt": calc_dt_iso,
            "fixed_offset_minutes": fixed_offset_minutes,
            "source": "fallback",
        }
        cache[key] = out
        _save_birth_confirmed_cache(cache)
        return out


# ==================================================
# KASI (Korea Astronomy and Space Science Institute) Calendar API
# - Solar <-> Lunar conversion (including leap month validation)
# - Authority: KASI via data.go.kr OpenAPI
# ==================================================
KASI_SERVICE_KEY = os.getenv("KASI_SERVICE_KEY", "").strip()
KASI_BASE = "https://apis.data.go.kr/B090041/openapi/service/LrsrCldInfoService"

def _kasi_parse_item(resp: requests.Response) -> dict:
    """Parse KASI response item safely (JSON preferred, XML fallback)."""
    # JSON (when _type=json works)
    try:
        data = resp.json()
        item = (
            data.get("response", {})
                .get("body", {})
                .get("items", {})
                .get("item")
        )
        if isinstance(item, list):
            item = item[0] if item else None
        if isinstance(item, dict):
            return item
    except Exception:
        pass

    # XML fallback
    try:
        root = ET.fromstring(resp.text or "")
        item_el = root.find(".//item")
        if item_el is None:
            return {}
        out = {}
        for child in list(item_el):
            out[child.tag] = (child.text or "").strip()
        return out
    except Exception:
        return {}

def _kasi_call(endpoint: str, params: dict) -> dict:
    if not KASI_SERVICE_KEY:
        raise RuntimeError("KASI_SERVICE_KEY is missing on server")

    q = {"serviceKey": KASI_SERVICE_KEY, "_type": "json"}
    q.update(params)
    url = f"{KASI_BASE}/{endpoint}"

    resp = requests.get(url, params=q, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"KASI HTTP {resp.status_code}: {resp.text[:200]}")

    item = _kasi_parse_item(resp)
    if not item:
        raise RuntimeError(f"KASI returned empty item: {resp.text[:200]}")
    return item

def kasi_sol_to_lun(sol_year: int, sol_month: int, sol_day: int) -> dict:
    """Solar -> Lunar. Returns normalized lunar fields + leap flag."""
    item = _kasi_call("getLunCalInfo", {
        "solYear": str(sol_year),
        "solMonth": f"{sol_month:02d}",
        "solDay": f"{sol_day:02d}",
    })
    lun_year = int(item.get("lunYear"))
    lun_month = int(item.get("lunMonth"))
    lun_day = int(item.get("lunDay"))
    leap = (item.get("lunLeapmonth") == "윤")
    label = f"음력 {lun_year}년 " + (f"윤{lun_month}월 " if leap else f"{lun_month}월 ") + f"{lun_day}일"
    return {
        "year": lun_year,
        "month": lun_month,
        "day": lun_day,
        "is_leap_month": leap,
        "label_kr": label,
        "_raw": {k: item.get(k) for k in ("lunLeapmonth","lunSecha","lunIljin") if k in item}
    }

def kasi_lun_to_sol(lun_year: int, lun_month: int, lun_day: int, is_leap_month: bool) -> dict:
    """Lunar(+leap) -> Solar. Returns confirmed solar date."""
    item = _kasi_call("getSolCalInfo", {
        "lunYear": str(lun_year),
        "lunMonth": f"{lun_month:02d}",
        "lunDay": f"{lun_day:02d}",
        "lunLeapmonth": "윤" if is_leap_month else "평",
    })
    sol_year = int(item.get("solYear"))
    sol_month = int(item.get("solMonth"))
    sol_day = int(item.get("solDay"))
    return {"year": sol_year, "month": sol_month, "day": sol_day}

def load_jieqi_table():
    if not JIEQI_TABLE_PATH.exists():
        raise FileNotFoundError(f"[JIEQI] missing file: {JIEQI_TABLE_PATH}")
    with JIEQI_TABLE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)

def _parse_dt_any(value, assume_tz):
    if value is None:
        return None
    if isinstance(value, str):
        s = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=assume_tz)
        return dt.astimezone(KST)
    return None

def _pick_item_dt(item):
    if "kst" in item:
        dt = _parse_dt_any(item.get("kst"), KST)
        if dt:
            return dt
    if "utc" in item:
        dt = _parse_dt_any(item.get("utc"), UTC)
        if dt:
            return dt
    return None

def get_jieqi_with_fallback(year: str):
    table = load_jieqi_table()
    year_data = table.get(year)
    if not year_data:
        raise ValueError(f"No jieqi for {year}")
    return year_data

def find_ipchun_dt(jieqi_list):
    for item in jieqi_list:
        if item.get("name") in ("입춘", "立春"):
            dt = _pick_item_dt(item)
            if dt:
                return dt
    raise ValueError("입춘 not found")

def _jieqi_term_dt_map(jieqi_list):
    m = {}
    for item in jieqi_list:
        name = item.get("name")
        dt = _pick_item_dt(item)
        if name and dt:
            m[name] = dt
    return m

STEMS = ["甲","乙","丙","丁","戊","己","庚","辛","壬","癸"]
BRANCHES = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"]


# ==================================================
# ==================================================
# TWELVE SINSAL (12신살) — SSOT
# 목표: '점신' 표기와 최대한 정합
# 주의: 12신살은 유파/앱마다 규칙이 다를 수 있음.
#       -> 기본 규칙(삼합국 중심지 기준) + 샘플 기반 OVERRIDE 병행
#       -> 화면(프론트)에서 절대 계산하지 말고 API에서만 산출
# ==================================================

# 12신살 표준 라벨(점신/당근에서 쓰는 12개)
TWELVE_SINSAL_ORDER = [
    "지살", "재살", "월살", "역마살", "화개살", "겁살",
    "망신살", "년살", "반안살", "육해살", "천살", "도화살",
]

# 삼합국 중심지(왕지) 기준 매핑
# - 申子辰(수국) 중심: 子
# - 寅午戌(화국) 중심: 午
# - 亥卯未(목국) 중심: 卯
# - 巳酉丑(금국) 중심: 酉
TRINE_CENTER = {
    "申": "子", "子": "子", "辰": "子",
    "寅": "午", "午": "午", "戌": "午",
    "亥": "卯", "卯": "卯", "未": "卯",
    "巳": "酉", "酉": "酉", "丑": "酉",
}

# 점신 표기 정합을 위한 샘플 기반 보정(SSOT 우선)
# key: (일지, 대상지지) -> 라벨
# ※ 여기 데이터는 '점신 화면 캡처'에서 역산한 것이며,
#    불일치 케이스가 발견되면 OVERRIDE에 추가/수정하고
#    그 사실만 백엔드 로그로 남긴다(프론트에 노출 금지).
TWELVE_SINSAL_OVERRIDE = {
    # 1979-08-12 (일지 亥)
    ("亥", "巳"): "역마살",
    ("亥", "申"): "겁살",
    ("亥", "亥"): "지살",
    ("亥", "未"): "화개살",

    # 1988-04-07 (일지 辰)
    ("辰", "辰"): "화개살",
    ("辰", "亥"): "망신살",

    # 1984-01-03 (일지 申)
    ("申", "巳"): "역마살",
    ("申", "申"): "겁살",
    ("申", "子"): "년살",
    ("申", "亥"): "망신살",

    # 1985-02-03 (일지 酉)
    ("酉", "酉"): "년살",
    ("酉", "丑"): "반안살",
    ("酉", "子"): "육해살",

    # (텍스트 샘플) 하정빈님 표
    ("午", "丑"): "월살",
    ("午", "午"): "육해살",
    ("午", "申"): "겁살",
    ("午", "亥"): "지살",
}

def twelve_sinsal(day_branch: str, target_branch: str, month_branch: str | None = None) -> str:
    """점신(삼합 기반) 12신살 매핑.

    기준: '일지(日支)'가 속한 삼합(해묘미/인오술/사유축/신자진)을 결정한 뒤,
         해당 삼합 행에서 target_branch(연/월/일/시 지지)에 대응하는 신살명을 반환한다.

    month_branch는 기존 호출 호환을 위해 남겨두지만, 점신 방식에서는 사용하지 않는다.
    """

    # 입력 정리
    if not day_branch or not target_branch:
        return ""

    # 12신살 컬럼 순서(표 머리)
    TWELVE_SINSAL_NAMES = [
        "겁살", "재살", "천살", "지살", "연살", "월살",
        "망신살", "장성살", "반안살", "역마살", "육해살", "화개살",
    ]

    # 삼합(4행) 기준 표: 각 행은 위 컬럼 순서대로 '해당 신살이 걸리는 지지'를 담는다.
    ROWS = {
        # 해·묘·미
        "해묘미": ["申", "酉", "戌", "亥", "子", "丑", "寅", "卯", "辰", "巳", "午", "未"],
        # 인·오·술
        "인오술": ["亥", "子", "丑", "寅", "卯", "辰", "巳", "午", "未", "申", "酉", "戌"],
        # 사·유·축
        "사유축": ["寅", "卯", "辰", "巳", "午", "未", "申", "酉", "戌", "亥", "子", "丑"],
        # 신·자·진
        "신자진": ["巳", "午", "未", "申", "酉", "戌", "亥", "子", "丑", "寅", "卯", "辰"],
    }

    # 일지가 속한 삼합 결정
    if day_branch in ("亥", "卯", "未"):
        group = "해묘미"
    elif day_branch in ("寅", "午", "戌"):
        group = "인오술"
    elif day_branch in ("巳", "酉", "丑"):
        group = "사유축"
    elif day_branch in ("申", "子", "辰"):
        group = "신자진"
    else:
        # 지지 12자 외 입력 방어
        return ""

    row = ROWS[group]

    # (선택) 개별 예외 오버라이드: 필요하면 여기만 추가해서 점신과 1:1 맞춘다.
    # key: (day_branch, target_branch)
    OVERRIDE: dict[tuple[str, str], str] = {
        # 예) ("午","子"): "연살",
    }
    ov = OVERRIDE.get((day_branch, target_branch))
    if ov:
        return ov

    # 행에서 target_branch가 등장하는 컬럼을 찾아 신살명 반환
    try:
        idx = row.index(target_branch)
        return TWELVE_SINSAL_NAMES[idx]
    except ValueError:
        return ""

# ==================================================
# TWELVE STAGES (12운성) — SSOT
# 기준: 일간(day_stem) + 대상 지지(branch)
# ==================================================
TWELVE_STAGES = ["장생","목욕","관대","건록","제왕","쇠","병","사","묘","절","태","양"]

# 장생 시작 지지 (일간 기준)
JANGSAENG_START_BY_STEM = {
    "甲": "亥", "乙": "午",
    "丙": "寅", "丁": "酉",
    "戊": "寅", "己": "酉",
    "庚": "巳", "辛": "子",
    "壬": "申", "癸": "卯",
}

BRANCH_INDEX = {b: i for i, b in enumerate(BRANCHES)}

def twelve_stage(day_stem: str, target_branch: str) -> str:
    """12운성(점신/만세력 표기 호환):
    - 기준: '일간(일주 천간)'을 기준으로 각 지지의 12운성을 산출
    - 방향: 양간(甲丙戊庚壬)=순행, 음간(乙丁己辛癸)=역행
    """
    if not day_stem or not target_branch:
        return ""
    start = JANGSAENG_START_BY_STEM.get(day_stem)
    if not start:
        return ""

    # 양간/음간 판정
    is_yang = day_stem in ("甲", "丙", "戊", "庚", "壬")

    if is_yang:
        idx = (BRANCH_INDEX[target_branch] - BRANCH_INDEX[start]) % 12
    else:
        idx = (BRANCH_INDEX[start] - BRANCH_INDEX[target_branch]) % 12

    return TWELVE_STAGES[idx]



# ==================================================
# KR READING + TEN GODS + HIDDEN STEMS (for infographic)
# SSOT in API: frontend should render only.
# ==================================================
STEM_KR = {
    "甲": "갑", "乙": "을", "丙": "병", "丁": "정", "戊": "무",
    "己": "기", "庚": "경", "辛": "신", "壬": "임", "癸": "계",
}
BRANCH_KR = {
    "子": "자", "丑": "축", "寅": "인", "卯": "묘", "辰": "진", "巳": "사",
    "午": "오", "未": "미", "申": "신", "酉": "유", "戌": "술", "亥": "해",
}

YANG_STEMS = set(["甲","丙","戊","庚","壬"])
STEM_POLARITY = {s: ("yang" if s in YANG_STEMS else "yin") for s in STEMS}

STEM_ELEMENT_HANJA = {
    "甲": "목", "乙": "목",
    "丙": "화", "丁": "화",
    "戊": "토", "己": "토",
    "庚": "금", "辛": "금",
    "壬": "수", "癸": "수",
}

# Generating cycle: wood -> fire -> earth -> metal -> water -> wood
GEN = {"목": "화", "화": "토", "토": "금", "금": "수", "수": "목"}
# Controlling cycle: wood controls earth, earth controls water, water controls fire, fire controls metal, metal controls wood
CTL = {"목": "토", "토": "수", "수": "화", "화": "금", "금": "목"}

def ten_god_of_stem(day_stem: str, target_stem: str) -> str:
    """Return 십성 of target_stem relative to day_stem (일간 기준).
    Uses standard element+yin/yang rules:
      - same element: 비견/겁재
      - day generates target: 식신/상관
      - day controls target: 편재/정재
      - target controls day: 편관/정관
      - target generates day: 편인/정인
    """
    if not day_stem or not target_stem:
        return ""
    de = STEM_ELEMENT_HANJA.get(day_stem)
    te = STEM_ELEMENT_HANJA.get(target_stem)
    if not de or not te:
        return ""

    same_polar = (STEM_POLARITY.get(day_stem) == STEM_POLARITY.get(target_stem))

    if te == de:
        return "비견" if same_polar else "겁재"

    # day generates target => output gods
    if GEN.get(de) == te:
        return "식신" if same_polar else "상관"

    # day controls target => wealth gods
    if CTL.get(de) == te:
        return "편재" if same_polar else "정재"

    # target controls day => officer gods
    if CTL.get(te) == de:
        return "편관" if same_polar else "정관"

    # target generates day => resource gods
    if GEN.get(te) == de:
        return "편인" if same_polar else "정인"

    return ""

# Hidden stems by branch (지장간)
HIDDEN_STEMS_BY_BRANCH = {
    "子": ["癸"],
    "丑": ["己","癸","辛"],
    "寅": ["甲","丙","戊"],
    "卯": ["乙"],
    "辰": ["戊","乙","癸"],
    "巳": ["丙","戊","庚"],
    "午": ["丁","己"],
    "未": ["己","丁","乙"],
    "申": ["庚","壬","戊"],
    "酉": ["辛"],
    "戌": ["戊","辛","丁"],
    "亥": ["壬","甲"],
}



# Display-only padding for hidden stems (UI only; no calculation impact)
HIDDEN_STEMS_DISPLAY_PAD = {
    "亥": "戊",
    "子": "戊",
    "卯": "戊",
    "酉": "戊",
}

# Display-only override for hidden stems (UI only; '점신' style)
# - NOTE: These are NOT traditional hidden stems additions; they are UI normalization rules to match the reference app.
# - Traditional calculation remains in HIDDEN_STEMS_BY_BRANCH / hidden_stems.
HIDDEN_STEMS_DISPLAY_OVERRIDE = {
    # 午: traditional hidden stems are 丁·己(2). Reference UI shows 丙·己·丁 (3).
    "午": ["丙", "己", "丁"],
}


# Main hidden stem (정기) for branch ten-god
MAIN_HIDDEN_STEM_BY_BRANCH = {
    "子": "癸", "丑": "己", "寅": "甲", "卯": "乙",
    "辰": "戊", "巳": "丙", "午": "丁", "未": "己",
    "申": "庚", "酉": "辛", "戌": "戊", "亥": "壬",
}

def enrich_pillar(p: dict, day_stem: str):
    """Mutate pillar dict in-place to include KR reading + ten gods + hidden stems."""
    if not p:
        return
    stem = p.get("stem")
    branch = p.get("branch")

    if stem:
        p["stem_kr"] = STEM_KR.get(stem, "")
        p["ten_god_stem"] = ten_god_of_stem(day_stem, stem)

    if branch:
        p["branch_kr"] = BRANCH_KR.get(branch, "")
        hidden = HIDDEN_STEMS_BY_BRANCH.get(branch, [])
        p["hidden_stems"] = hidden
        # display helpers (traditional stems preserved; Korean reading for UI)
        p["hidden_stems_kr"] = [STEM_KR.get(hs, "") for hs in hidden]
        p["hidden_stems_dot"] = "·".join([STEM_KR.get(hs, "") for hs in hidden if STEM_KR.get(hs, "")])
        # display-only (UI normalization; reference app compatible)
        # Priority:
        # 1) Explicit override (e.g., 午 -> 丙·己·丁)
        # 2) If exactly 2 stems and pad exists, append pad
        # 3) Otherwise keep traditional stems as-is
        if branch in HIDDEN_STEMS_DISPLAY_OVERRIDE:
            display = list(HIDDEN_STEMS_DISPLAY_OVERRIDE[branch])
        else:
            display = list(hidden)
            if len(display) == 2:
                pad = HIDDEN_STEMS_DISPLAY_PAD.get(branch)
                if pad:
                    display = display + [pad]

        p["hidden_stems_display"] = display
        p["hidden_stems_display_kr"] = [STEM_KR.get(hs, "") for hs in display]
        p["hidden_stems_display_dot"] = "·".join([STEM_KR.get(hs, "") for hs in display if STEM_KR.get(hs, "")])
        p["ten_god_hidden"] = [ten_god_of_stem(day_stem, hs) for hs in hidden]
        main_hidden = MAIN_HIDDEN_STEM_BY_BRANCH.get(branch, "")
        p["ten_god_branch"] = ten_god_of_stem(day_stem, main_hidden) if main_hidden else ""



# ==================================================
# ILJU ANIMAL (Color + Zodiac Animal)
# - Color from Day Stem element
# - Animal from Day Branch (12 zodiac)
# ==================================================
STEM_ELEMENT = {
    "甲": "목", "乙": "목",
    "丙": "화", "丁": "화",
    "戊": "토", "己": "토",
    "庚": "금", "辛": "금",
    "壬": "수", "癸": "수",
}
ELEMENT_COLOR_KR = {
    "목": "푸른",
    "화": "붉은",
    "토": "누런",
    "금": "하얀",
    "수": "검은",
}
BRANCH_ANIMAL_KR = {
    "子": "쥐", "丑": "소", "寅": "호랑이", "卯": "토끼",
    "辰": "용", "巳": "뱀", "午": "말", "未": "양",
    "申": "원숭이", "酉": "닭", "戌": "개", "亥": "돼지",
}
ANIMAL_EMOJI = {
    "쥐": "🐭", "소": "🐮", "호랑이": "🐯", "토끼": "🐰",
    "용": "🐲", "뱀": "🐍", "말": "🐴", "양": "🐑",
    "원숭이": "🐵", "닭": "🐔", "개": "🐶", "돼지": "🐷",
}

def get_ilju_animal(day_gan: str, day_ji: str) -> str:
    """Return e.g. '하얀 돼지'. Deterministic mapping only."""
    elem = STEM_ELEMENT.get(day_gan, "")
    color = ELEMENT_COLOR_KR.get(elem, "")
    animal = BRANCH_ANIMAL_KR.get(day_ji, "")
    if not color or not animal:
        return ""
    return f"{color} {animal}"

def get_ilju_emoji(day_ji: str) -> str:
    animal = BRANCH_ANIMAL_KR.get(day_ji, "")
    return ANIMAL_EMOJI.get(animal, "🐾")

DAY_PILLAR_JDN_OFFSET = 49

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

MONTH_TERM_TO_BRANCH = [
    ("입춘", "寅"), ("경칩", "卯"), ("청명", "辰"),
    ("입하", "巳"), ("망종", "午"), ("소서", "未"),
    ("입추", "申"), ("백로", "酉"), ("한로", "戌"),
    ("입동", "亥"), ("대설", "子"), ("소한", "丑"),
]

YEAR_STEM_TO_YIN_MONTH_STEM = {
    "甲": "丙", "己": "丙", "乙": "戊", "庚": "戊",
    "丙": "庚", "辛": "庚", "丁": "壬", "壬": "壬",
    "戊": "甲", "癸": "甲",
}

MONTH_BRANCH_SEQ = ["寅","卯","辰","巳","午","未","申","酉","戌","亥","子","丑"]

def _get_month_branch_by_jul(input_dt, this_year_terms, prev_year_terms):
    candidates = []
    for term, branch in MONTH_TERM_TO_BRANCH:
        dt = this_year_terms.get(term)
        if dt:
            candidates.append((dt, branch))
    prev_daeseol = prev_year_terms.get("대설")
    if prev_daeseol:
        candidates.append((prev_daeseol, "子"))
    valid = [c for c in candidates if c[0] <= input_dt]
    if not valid:
        return "丑"
    valid.sort(key=lambda x: x[0])
    return valid[-1][1]

def get_month_pillar(input_dt, saju_year_pillar, jieqi_this_year, jieqi_prev_year):
    this_map = _jieqi_term_dt_map(jieqi_this_year)
    prev_map = _jieqi_term_dt_map(jieqi_prev_year)
    month_branch = _get_month_branch_by_jul(input_dt, this_map, prev_map)
    year_stem = saju_year_pillar["stem"]
    yin_month_stem = YEAR_STEM_TO_YIN_MONTH_STEM[year_stem]
    month_index = MONTH_BRANCH_SEQ.index(month_branch)
    stem_index = (STEMS.index(yin_month_stem) + month_index) % 10
    month_stem = STEMS[stem_index]
    return {"stem": month_stem, "branch": month_branch, "ganji": month_stem + month_branch}

HOUR_BRANCH_SEQ = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"]

DAY_STEM_TO_ZI_HOUR_STEM = {
    "甲": "甲", "己": "甲", "乙": "丙", "庚": "丙",
    "丙": "戊", "辛": "戊", "丁": "庚", "壬": "庚",
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

@app.get("/api/saju/calc")
def calc_saju(
    birth: str = Query(...),
    calendar: str = Query("solar"),
    birth_time: str = Query("unknown"),
    gender: str = Query("unknown"),
    is_leap_month: bool = Query(False),
):
    from fastapi import HTTPException

    # --------------------------------------------------
    # 0) Parse inputs
    # --------------------------------------------------
    try:
        bd = datetime.strptime(birth, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid birth format (expected YYYY-MM-DD)")

    bt = (birth_time or "").strip().lower()
    if bt and bt not in ("unknown", "null", "none"):
        try:
            hh, mm = map(int, bt.split(":"))
        except Exception:
            raise HTTPException(status_code=400, detail="invalid birth_time format (expected HH:MM)")
        has_time = True
    else:
        hh, mm = 0, 0
        has_time = False

    fixed_offset_minutes = SEOUL_FIXED_OFFSET_MINUTES if has_time else 0
    cal = (calendar or "solar").strip().lower()

    # --------------------------------------------------
    # 1) Resolve confirmed solar date (SSOT)
    #   - Needed before we can build input_dt/calc_dt
    # --------------------------------------------------
    try:
        if cal == "lunar":
            try:
                sol = kasi_lun_to_sol(bd.year, bd.month, bd.day, bool(is_leap_month))
                solar_confirmed = date(int(sol["year"]), int(sol["month"]), int(sol["day"]))
            except Exception:
                fb = _try_fallback_lunar_to_solar(bd.year, bd.month, bd.day, bool(is_leap_month))
                if not fb:
                    raise
                solar_confirmed = date(int(fb["year"]), int(fb["month"]), int(fb["day"]))
        else:
            solar_confirmed = bd
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"calendar resolve failed: {e}")

    # --------------------------------------------------
    # 2) Time handling (kept as-is)
    # --------------------------------------------------
    input_dt = datetime(solar_confirmed.year, solar_confirmed.month, solar_confirmed.day, hh, mm, tzinfo=KST)
    calc_dt = input_dt - timedelta(minutes=SEOUL_FIXED_OFFSET_MINUTES) if has_time else input_dt

    # --------------------------------------------------
    # 3) Birth Confirmed SSOT (KASI + cache + fallback)
    # --------------------------------------------------
    try:
        birth_confirmed_json = build_birth_confirmed_json(
            birth=birth,
            calendar=calendar,
            is_leap_month=bool(is_leap_month),
            calc_dt_iso=calc_dt.isoformat(),
            fixed_offset_minutes=fixed_offset_minutes,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"calendar SSOT failed: {e}")

    # Trust SSOT solar if present
    try:
        if birth_confirmed_json.get("solar", {}).get("year"):
            solar_confirmed = date(
                int(birth_confirmed_json["solar"]["year"]),
                int(birth_confirmed_json["solar"]["month"]),
                int(birth_confirmed_json["solar"]["day"]),
            )
            # keep input_dt/calc_dt aligned for downstream logic
            input_dt = datetime(solar_confirmed.year, solar_confirmed.month, solar_confirmed.day, hh, mm, tzinfo=KST)
            calc_dt = input_dt - timedelta(minutes=SEOUL_FIXED_OFFSET_MINUTES) if has_time else input_dt
    except Exception:
        pass

    # --------------------------------------------------
    # 4) Pillar calculation (solar-based)
    # --------------------------------------------------
    jieqi_this = get_jieqi_with_fallback(str(input_dt.year))
    ipchun_dt = find_ipchun_dt(jieqi_this)
    saju_year = input_dt.year if input_dt >= ipchun_dt else input_dt.year - 1

    year_pillar = get_year_pillar(saju_year)
    day_pillar = get_day_pillar(input_dt.date())

    jieqi_prev = get_jieqi_with_fallback(str(input_dt.year - 1))
    month_pillar = get_month_pillar(input_dt, year_pillar, jieqi_this, jieqi_prev)
    hour_pillar = get_hour_pillar(day_pillar, calc_dt.hour, calc_dt.minute) if has_time else None

    # --------------------------------------------------
    # 5) Enrich pillars for infographic (ten gods + hidden stems)
    # --------------------------------------------------
    pillars = {"year": year_pillar, "month": month_pillar, "day": day_pillar, "hour": hour_pillar}
    day_stem = (day_pillar or {}).get("stem", "")
    day_branch = (day_pillar or {}).get("branch", "")

    for _k in ("year", "month", "day", "hour"):
        _p = pillars.get(_k)
        if not _p:
            continue

        enrich_pillar(_p, day_stem)

        _branch = _p.get("branch")
        if _branch:
            # 12운성/12신살
            _p["twelve_stage"] = twelve_stage(day_stem, _branch)
            _p["twelve_sinsal"] = twelve_sinsal(day_branch, _branch, month_pillar.get("branch") if month_pillar else None)

    return {
        "input": {
            "birth": birth,
            "calendar": calendar,
            "birth_time": birth_time,
            "gender": gender,
            "is_leap_month": is_leap_month,
        },
        "meta": {
            # Back-compat: 기존 프론트/route.ts가 meta.solar_confirmed / meta.lunar를 참조
            "solar_confirmed": {
                "year": int(birth_confirmed_json["solar"]["year"]),
                "month": int(birth_confirmed_json["solar"]["month"]),
                "day": int(birth_confirmed_json["solar"]["day"]),
                "label_kr": birth_confirmed_json["solar"].get("label_kr")
                    or f"양력 {birth_confirmed_json['solar']['year']}년 {birth_confirmed_json['solar']['month']}월 {birth_confirmed_json['solar']['day']}일",
            },
            "lunar": birth_confirmed_json.get("lunar") or {},
            # New SSOT blob (DB 저장용)
            "birth_confirmed_json": birth_confirmed_json,
        },
        "pillars": pillars,
        "ilju_animal": get_ilju_animal(day_pillar.get("stem", ""), day_pillar.get("branch", "")),
        "ilju_emoji": get_ilju_emoji(day_pillar.get("branch", "")),
        "debug": {
            "timezone": "KST",
            "fixed_offset_minutes": fixed_offset_minutes,
            "input_dt": input_dt.isoformat(),
            "calc_dt": calc_dt.isoformat(),
            "saju_year": saju_year,
        },
    }




# =========================
# API - PDF Generation
# =========================
from fastapi import HTTPException
from fastapi.responses import Response
from playwright.async_api import async_playwright
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from io import BytesIO
import requests
import cairosvg

@app.get("/api/pdf/generate")
async def generate_pdf(rid: str = Query(...), token: str = Query(...)):
    try:
        url = f"https://saju-baksa.com/report/{rid}?t={token}&print=1"
        bg_url = "https://saju-baksa.com/report-bg.png"
        logo_url = "https://saju-baksa.com/logo-text.svg"
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(args=['--no-sandbox', '--disable-setuid-sandbox'])
            page = await browser.new_page()
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await page.wait_for_timeout(3000)
            
            # PDF 생성 시에만 여백 강제 제거 (웹은 그대로)
            await page.evaluate("""
                // 표지 여백 제거
                const cover = document.querySelector('.report-cover');
                if (cover) {
                    cover.style.padding = '0';
                    cover.style.margin = '0';
                    cover.style.pageBreakAfter = 'always';
                    cover.style.breakAfter = 'page';
                }
                
                // 본문 컨테이너 여백 최소화
                const container = document.querySelectorAll('[style*="max-width"]')[0];
                if (container) {
                    container.style.padding = '15mm 15mm';
                    container.style.margin = '0';
                    container.style.pageBreakBefore = 'always';
                }
                
                // 프린트 색상 유지
                document.documentElement.style.webkitPrintColorAdjust = 'exact';
                document.documentElement.style.printColorAdjust = 'exact';
            """)
            
            await page.wait_for_timeout(1000)
            
            # margin 0으로 (표지 꽉 차게)
            pdf_bytes = await page.pdf(
                format="A4",
                print_background=True,
                margin={
                    "top": "0mm",
                    "bottom": "0mm",
                    "left": "0mm",
                    "right": "0mm"
                },
                prefer_css_page_size=False
            )
            
            await browser.close()
        
        # pypdf에서 배경+로고 추가
        pdf_bytes = add_background_and_logo(pdf_bytes, bg_url, logo_url)
        
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename=report-{rid}.pdf"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def add_background_and_logo(original_pdf_bytes, bg_url, logo_url):
    from PIL import Image
    
    original_pdf = PdfReader(BytesIO(original_pdf_bytes))
    output = PdfWriter()
    
    try:
        bg_response = requests.get(bg_url, timeout=10)
        bg_image = Image.open(BytesIO(bg_response.content))
    except:
        bg_image = None
    
    # SVG 로고를 PNG로 변환 (크기 지정)
    logo_image = None
    try:
        logo_response = requests.get(logo_url, timeout=10)
        logo_png = cairosvg.svg2png(
            bytestring=logo_response.content,
            output_width=240,   # 80pt * 3 (고해상도)
            output_height=72    # 24pt * 3
        )
        logo_image = Image.open(BytesIO(logo_png))
        print(f"[LOGO] Successfully loaded: {logo_image.size}")
    except Exception as e:
        print(f"[LOGO ERROR] {e}")
    
    page_width, page_height = A4
    
    for page_num in range(len(original_pdf.pages)):
        page = original_pdf.pages[page_num]
        
        # 표지(0페이지)는 그대로
        if page_num == 0:
            output.add_page(page)
            continue
        
        # 나머지 페이지: 1. 배경 먼저, 2. 페이지, 3. 로고 맨 위
        
        # 1. 배경 레이어
        bg_packet = BytesIO()
        bg_canvas = canvas.Canvas(bg_packet, pagesize=A4)
        if bg_image:
            img_reader = ImageReader(bg_image)
            bg_canvas.drawImage(
                img_reader,
                0, 0,
                width=page_width,
                height=page_height,
                preserveAspectRatio=False,
                mask='auto'
            )
        bg_canvas.save()
        bg_packet.seek(0)
        bg_pdf = PdfReader(bg_packet)
        bg_page = bg_pdf.pages[0]
        
        # 2. 배경 밑에 페이지 올리기 (본문이 위로)
        bg_page.merge_page(page)
        
        # 3. 로고 레이어 (맨 위)
        if logo_image:
            try:
                logo_packet = BytesIO()
                logo_canvas = canvas.Canvas(logo_packet, pagesize=A4)
                
                logo_reader = ImageReader(logo_image)
                logo_width = 80
                logo_height = 24
                logo_x = (page_width - logo_width) / 2
                logo_y = 40
                
                print(f"[LOGO] Drawing at x={logo_x}, y={logo_y}, size={logo_width}x{logo_height}")
                logo_canvas.drawImage(
                    logo_reader,
                    logo_x,
                    logo_y,
                    width=logo_width,
                    height=logo_height,
                    preserveAspectRatio=True,
                    mask='auto'
                )
                logo_canvas.save()
                
                logo_packet.seek(0)
                logo_pdf = PdfReader(logo_packet)
                logo_page = logo_pdf.pages[0]
                
                # 로고를 페이지 맨 위에 합치기
                bg_page.merge_page(logo_page)
                print(f"[LOGO] Page {page_num}: Successfully drawn on top")
            except Exception as e:
                print(f"[LOGO DRAW ERROR] Page {page_num}: {e}")
        
        output.add_page(bg_page)
    
    final_pdf = BytesIO()
    output.write(final_pdf)
    final_pdf.seek(0)
    return final_pdf.read()
