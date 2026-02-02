from fastapi import FastAPI, Query
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
import json
import os

# ==================================================
# SSOT: Calendar Cache (Solar/Lunar)
# - Reads/writes `public.calendar_ssot` via DATABASE_URL (Postgres)
# - Requires `psycopg2-binary` in requirements.txt
# - If DB driver/env missing, it silently falls back to KASI (non-breaking)
# ==================================================
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    _SSOT_DB_OK = True
except Exception:
    _SSOT_DB_OK = False

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
print("[SSOT_BOOT]", "_SSOT_DB_OK=", _SSOT_DB_OK, "DATABASE_URL_SET=", bool(DATABASE_URL))

def _ssot_get_conn():
    if not (_SSOT_DB_OK and DATABASE_URL):
        return None
    try:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except Exception:
        print("[SSOT] MISS", flush=True)
        return None

def ssot_lookup(birth_dt: date, calendar: str, is_leap_month: bool):
    """Return cached row dict or None."""
    print("[SSOT] LOOKUP", flush=True)
    conn = _ssot_get_conn()
    if not conn:
        print("[SSOT] MISS", flush=True)
        return None
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                select solar_confirmed, lunar_confirmed, meta_json
                from public.calendar_ssot
                where birth = %s and calendar = %s and is_leap_month = %s
                limit 1
                """,
                (birth_dt, (calendar or "").lower(), bool(is_leap_month)),
            )
            row = cur.fetchone()
            if row:
                print("[SSOT] HIT", flush=True)
            else:
                print("[SSOT] MISS", flush=True)
            return row
    except Exception:
        print("[SSOT] MISS", flush=True)
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass

def ssot_upsert(birth_dt: date, calendar: str, is_leap_month: bool, solar_confirmed_dt: date, lunar_meta: dict):
    """Upsert cache row. Non-fatal on any error."""
    conn = _ssot_get_conn()
    if not conn:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into public.calendar_ssot
                  (birth, calendar, is_leap_month, solar_confirmed, lunar_confirmed, meta_json)
                values
                  (%s, %s, %s, %s, %s, %s)
                on conflict (birth, calendar, is_leap_month)
                do update set
                  solar_confirmed = excluded.solar_confirmed,
                  lunar_confirmed = excluded.lunar_confirmed,
                  meta_json = excluded.meta_json
                """,
                (
                    birth_dt,
                    (calendar or "").lower(),
                    bool(is_leap_month),
                    solar_confirmed_dt,
                    json.dumps(lunar_meta, ensure_ascii=False),
                    json.dumps(
                        {
                            "source": "kasi",
                            "cached_at": (datetime.now(tz=UTC).isoformat() if "UTC" in globals() else datetime.utcnow().isoformat()),
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
            print("[SSOT] UPSERT", flush=True)
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
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
# Jieqi (24절기) table path resolver
# - Prefer env:JIEQI_TABLE_PATH if set
# - Otherwise try common repo paths (data/ first, then project root)
def _resolve_jieqi_path() -> Path | None:
    env = (os.getenv("JIEQI_TABLE_PATH") or "").strip()
    if env:
        p = Path(env)
        if p.exists():
            return p

    candidates = [
        DATA_DIR / "jieqi_table.json",
        DATA_DIR / "jieqi_1900_2052.json",
        PROJECT_ROOT / "jieqi_1900_2052.json",
        PROJECT_ROOT / "data" / "jieqi_1900_2052.json",
    ]
    for p in candidates:
        try:
            if p.exists():
                return p
        except Exception:
            continue
    return None

JIEQI_TABLE_PATH = _resolve_jieqi_path() or (DATA_DIR / "jieqi_1900_2052.json")


KST = ZoneInfo("Asia/Seoul")
UTC = timezone.utc

SEOUL_FIXED_OFFSET_MINUTES = 32


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

    # --------------------------------------------------
    # 4) Fortune bundle (대운/연운/월운/일진) — added only
    # --------------------------------------------------
    try:
        jieqi_next = get_jieqi_with_fallback(str(input_dt.year + 1))
        fortune_bundle = build_fortune_bundle(
            input_dt=input_dt,
            solar_confirmed_dt=solar_confirmed,
            year_pillar=year_pillar,
            month_pillar=month_pillar,
            gender=gender,
            jieqi_this_year=jieqi_this,
            jieqi_prev_year=jieqi_prev,
            jieqi_next_year=jieqi_next,
        )
    except Exception:
        fortune_bundle = {"daewoon": [], "yearly": {}, "monthly": {}, "daily": {}}

    return {"year": sol_year, "month": sol_month, "day": sol_day}

_JIEQI_TABLE_CACHE = None
_JIEQI_TABLE_PATH_USED = None

def load_jieqi_table():
    """Load 24절기 table with caching and robust path resolution.

    We DO NOT silently swallow errors here because this table is used for
    ipchun(입춘) boundary. If the file is missing in the container, we want a
    clear error in logs (and client detail).
    """
    global _JIEQI_TABLE_CACHE, _JIEQI_TABLE_PATH_USED

    if _JIEQI_TABLE_CACHE is not None:
        return _JIEQI_TABLE_CACHE

    p = _resolve_jieqi_path()
    _JIEQI_TABLE_PATH_USED = str(p) if p else str(JIEQI_TABLE_PATH)

    if not p or not p.exists():
        msg = f"[JIEQI] missing file (tried env or candidates). last={_JIEQI_TABLE_PATH_USED}"
        print(msg, flush=True)
        raise FileNotFoundError(msg)

    with p.open("r", encoding="utf-8") as f:
        table = json.load(f)

    # quick sanity log (years range)
    try:
        years = sorted(int(k) for k in table.keys() if str(k).isdigit())
        if years:
            print(f"[JIEQI] loaded {p} years={years[0]}..{years[-1]} count={len(years)}", flush=True)
        else:
            print(f"[JIEQI] loaded {p} (no year keys?)", flush=True)
    except Exception:
        print(f"[JIEQI] loaded {p}", flush=True)

    _JIEQI_TABLE_CACHE = table
    return table

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
    y = str(year)
    year_data = table.get(y)
    if not year_data and y.isdigit():
        # defensive: sometimes keys could be int in a different build step
        year_data = table.get(int(y))
    if not year_data:
        raise ValueError(f"No jieqi for {y}")
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

# ==================================================
# TWELVE SINSAL (12신살) — SSOT (YEAR BRANCH BASED)
# 기준: 년지 (국내 표준 / 점신 방식)
# ==================================================

TWELVE_SINSAL_BY_YEAR_TRINE = {
    # 해묘미
    "亥": {"申":"겁살","酉":"재살","戌":"천살","亥":"지살","子":"연살","丑":"월살","寅":"망신살","卯":"장성살","辰":"반안살","巳":"역마살","午":"육해살","未":"화개살"},
    "卯": {"申":"겁살","酉":"재살","戌":"천살","亥":"지살","子":"연살","丑":"월살","寅":"망신살","卯":"장성살","辰":"반안살","巳":"역마살","午":"육해살","未":"화개살"},
    "未": {"申":"겁살","酉":"재살","戌":"천살","亥":"지살","子":"연살","丑":"월살","寅":"망신살","卯":"장성살","辰":"반안살","巳":"역마살","午":"육해살","未":"화개살"},
    # 인오술
    "寅": {"亥":"겁살","子":"재살","丑":"천살","寅":"지살","卯":"연살","辰":"월살","巳":"망신살","午":"장성살","未":"반안살","申":"역마살","酉":"육해살","戌":"화개살"},
    "午": {"亥":"겁살","子":"재살","丑":"천살","寅":"지살","卯":"연살","辰":"월살","巳":"망신살","午":"장성살","未":"반안살","申":"역마살","酉":"육해살","戌":"화개살"},
    "戌": {"亥":"겁살","子":"재살","丑":"천살","寅":"지살","卯":"연살","辰":"월살","巳":"망신살","午":"장성살","未":"반안살","申":"역마살","酉":"육해살","戌":"화개살"},
    # 사유축
    "巳": {"寅":"겁살","卯":"재살","辰":"천살","巳":"지살","午":"연살","未":"월살","申":"망신살","酉":"장성살","戌":"반안살","亥":"역마살","子":"육해살","丑":"화개살"},
    "酉": {"寅":"겁살","卯":"재살","辰":"천살","巳":"지살","午":"연살","未":"월살","申":"망신살","酉":"장성살","戌":"반안살","亥":"역마살","子":"육해살","丑":"화개살"},
    "丑": {"寅":"겁살","卯":"재살","辰":"천살","巳":"지살","午":"연살","未":"월살","申":"망신살","酉":"장성살","戌":"반안살","亥":"역마살","子":"육해살","丑":"화개살"},
    # 신자진
    "申": {"巳":"겁살","午":"재살","未":"천살","申":"지살","酉":"연살","戌":"월살","亥":"망신살","子":"장성살","丑":"반안살","寅":"역마살","卯":"육해살","辰":"화개살"},
    "子": {"巳":"겁살","午":"재살","未":"천살","申":"지살","酉":"연살","戌":"월살","亥":"망신살","子":"장성살","丑":"반안살","寅":"역마살","卯":"육해살","辰":"화개살"},
    "辰": {"巳":"겁살","午":"재살","未":"천살","申":"지살","酉":"연살","戌":"월살","亥":"망신살","子":"장성살","丑":"반안살","寅":"역마살","卯":"육해살","辰":"화개살"},
}

def twelve_sinsal(year_branch: str, target_branch: str) -> str:
    if not year_branch or not target_branch:
        return ""
    table = TWELVE_SINSAL_BY_YEAR_TRINE.get(year_branch)
    if not table:
        return ""
    return table.get(target_branch, "")

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
# Applies ONLY when the traditional hidden stems list has exactly 2 items.
# Keep this minimal to avoid accidental "3 stems" when the reference UI expects 2.
HIDDEN_STEMS_DISPLAY_PAD = {
    # Example: 亥(壬甲) -> display as 3 (reference style)
    "亥": "戊",
}

# Display-only normalization for branches that have 1 hidden stem traditionally,
# but the reference UI (점신) shows them as a 2-stem pair.
HIDDEN_STEMS_DISPLAY_PAIR = {
    # 子: 癸 -> 壬癸 (임계)
    "子": ["壬", "癸"],
    # 卯: 乙 -> 甲乙 (갑을)
    "卯": ["甲", "乙"],
    # 酉: 辛 -> 庚辛 (경신)
    "酉": ["庚", "辛"],
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
        # display-only (점신 스타일 우선)
        display = list(hidden)

        # 1 -> 2 (pair) normalization
        if len(display) == 1:
            pair = HIDDEN_STEMS_DISPLAY_PAIR.get(branch)
            if pair:
                display = list(pair)

        # 2 -> 3 padding (only where explicitly desired)
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
BRANCH_ELEMENT = {
    "寅": "목", "卯": "목",
    "巳": "화", "午": "화",
    "辰": "토", "戌": "토", "丑": "토", "未": "토",
    "申": "금", "酉": "금",
    "亥": "수", "子": "수",
}
ELEMENT_COLOR_KR = {
    "목": "푸른",
    "화": "붉은",
    "토": "황금",
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

def calculate_elements_ratio(pillars: dict) -> dict:
    """Calculate five elements ratio from 8 characters (stems + branches)."""
    elements = []
    
    # Extract stems and branches from year/month/day/hour
    for key in ("year", "month", "day", "hour"):
        pillar = pillars.get(key)
        if not pillar:
            continue
        stem = pillar.get("stem", "")
        branch = pillar.get("branch", "")
        if stem:
            elements.append(STEM_ELEMENT.get(stem, ""))
        if branch:
            elements.append(BRANCH_ELEMENT.get(branch, ""))
    
    # Filter empty and count
    elements = [e for e in elements if e]
    count = {"목": 0, "화": 0, "토": 0, "금": 0, "수": 0}
    for e in elements:
        if e in count:
            count[e] += 1
    
    # Calculate ratio (round to 1 decimal)
    total = len(elements)
    ratio = {
        "wood": {"count": count["목"], "ratio": round((count["목"] / total * 100), 1) if total > 0 else 0},
        "fire": {"count": count["화"], "ratio": round((count["화"] / total * 100), 1) if total > 0 else 0},
        "earth": {"count": count["토"], "ratio": round((count["토"] / total * 100), 1) if total > 0 else 0},
        "metal": {"count": count["금"], "ratio": round((count["금"] / total * 100), 1) if total > 0 else 0},
        "water": {"count": count["수"], "ratio": round((count["수"] / total * 100), 1) if total > 0 else 0},
    }
    
    return ratio

def calculate_ten_gods_ratio(pillars: dict) -> dict:
    """Calculate ten gods ratio from 8 characters (4 stems + 4 branches)."""
    ten_gods = []
    
    # Extract ten_god from stems and branches
    for key in ("year", "month", "day", "hour"):
        pillar = pillars.get(key)
        if not pillar:
            continue
        # Stem ten god
        ten_god_stem = pillar.get("ten_god_stem", "")
        if ten_god_stem:
            ten_gods.append(ten_god_stem)
        # Branch ten god (main hidden stem)
        ten_god_branch = pillar.get("ten_god_branch", "")
        if ten_god_branch:
            ten_gods.append(ten_god_branch)
    
    # Count each ten god
    count = {
        "비견": 0, "겁재": 0,
        "식신": 0, "상관": 0,
        "편재": 0, "정재": 0,
        "편관": 0, "정관": 0,
        "편인": 0, "정인": 0
    }
    for tg in ten_gods:
        if tg in count:
            count[tg] += 1
    
    # Calculate ratio (round to 1 decimal)
    total = len([tg for tg in ten_gods if tg])
    ratio = {
        "bijeon": {"count": count["비견"], "ratio": round((count["비견"] / total * 100), 1) if total > 0 else 0},
        "geopjae": {"count": count["겁재"], "ratio": round((count["겁재"] / total * 100), 1) if total > 0 else 0},
        "siksin": {"count": count["식신"], "ratio": round((count["식신"] / total * 100), 1) if total > 0 else 0},
        "sanggwan": {"count": count["상관"], "ratio": round((count["상관"] / total * 100), 1) if total > 0 else 0},
        "pyeonjae": {"count": count["편재"], "ratio": round((count["편재"] / total * 100), 1) if total > 0 else 0},
        "jeongjae": {"count": count["정재"], "ratio": round((count["정재"] / total * 100), 1) if total > 0 else 0},
        "pyeongwan": {"count": count["편관"], "ratio": round((count["편관"] / total * 100), 1) if total > 0 else 0},
        "jeonggwan": {"count": count["정관"], "ratio": round((count["정관"] / total * 100), 1) if total > 0 else 0},
        "pyeonin": {"count": count["편인"], "ratio": round((count["편인"] / total * 100), 1) if total > 0 else 0},
        "jeongin": {"count": count["정인"], "ratio": round((count["정인"] / total * 100), 1) if total > 0 else 0},
    }
    
    return ratio

DAY_PILLAR_JDN_OFFSET = 49

def gregorian_to_jdn(y, m, d):
    a = (14 - m) // 12
    y2 = y + 4800 - a
    m2 = m + 12 * a - 3
    return d + (153*m2+2)//5 + 365*y2 + y2//4 - y2//100 + y2//400 - 32045

def get_day_pillar(dt: date):
    idx = (gregorian_to_jdn(dt.year, dt.month, dt.day) + DAY_PILLAR_JDN_OFFSET) % 60

    # --------------------------------------------------
    # 4) Fortune bundle (대운/연운/월운/일진) — added only
    # --------------------------------------------------
    try:
        jieqi_next = get_jieqi_with_fallback(str(input_dt.year + 1))
        fortune_bundle = build_fortune_bundle(
            input_dt=input_dt,
            solar_confirmed_dt=solar_confirmed,
            year_pillar=year_pillar,
            month_pillar=month_pillar,
            gender=gender,
            jieqi_this_year=jieqi_this,
            jieqi_prev_year=jieqi_prev,
            jieqi_next_year=jieqi_next,
        )
    except Exception:
        fortune_bundle = {"daewoon": [], "yearly": {}, "monthly": {}, "daily": {}}

    return {
        "stem": STEMS[idx % 10],
        "branch": BRANCHES[idx % 12],
        "ganji": STEMS[idx % 10] + BRANCHES[idx % 12],
        "index60": idx
    }

def get_year_pillar(year: int):
    idx = (year - 1984) % 60

    # --------------------------------------------------
    # 4) Fortune bundle (대운/연운/월운/일진) — added only
    # --------------------------------------------------
    try:
        jieqi_next = get_jieqi_with_fallback(str(input_dt.year + 1))
        fortune_bundle = build_fortune_bundle(
            input_dt=input_dt,
            solar_confirmed_dt=solar_confirmed,
            year_pillar=year_pillar,
            month_pillar=month_pillar,
            gender=gender,
            jieqi_this_year=jieqi_this,
            jieqi_prev_year=jieqi_prev,
            jieqi_next_year=jieqi_next,
        )
    except Exception:
        fortune_bundle = {"daewoon": [], "yearly": {}, "monthly": {}, "daily": {}}

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

    # --------------------------------------------------
    # 4) Fortune bundle (대운/연운/월운/일진) — added only
    # --------------------------------------------------
    try:
        jieqi_next = get_jieqi_with_fallback(str(input_dt.year + 1))
        fortune_bundle = build_fortune_bundle(
            input_dt=input_dt,
            solar_confirmed_dt=solar_confirmed,
            year_pillar=year_pillar,
            month_pillar=month_pillar,
            gender=gender,
            jieqi_this_year=jieqi_this,
            jieqi_prev_year=jieqi_prev,
            jieqi_next_year=jieqi_next,
        )
    except Exception:
        fortune_bundle = {"daewoon": [], "yearly": {}, "monthly": {}, "daily": {}}

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

    # --------------------------------------------------
    # 4) Fortune bundle (대운/연운/월운/일진) — added only
    # --------------------------------------------------
    try:
        jieqi_next = get_jieqi_with_fallback(str(input_dt.year + 1))
        fortune_bundle = build_fortune_bundle(
            input_dt=input_dt,
            solar_confirmed_dt=solar_confirmed,
            year_pillar=year_pillar,
            month_pillar=month_pillar,
            gender=gender,
            jieqi_this_year=jieqi_this,
            jieqi_prev_year=jieqi_prev,
            jieqi_next_year=jieqi_next,
        )
    except Exception:
        fortune_bundle = {"daewoon": [], "yearly": {}, "monthly": {}, "daily": {}}

    return {"stem": hour_stem, "branch": hour_branch, "ganji": hour_stem + hour_branch}


# ==================================================
# FORTUNE (대운/연운/월운/일진) — Extension only
# - Adds "fortune" key to response WITHOUT changing existing keys/logic
# - Uses Jieqi table already loaded for time boundaries
# ==================================================
import math as _math

def _sexagenary_shift(ganji: str, step: int) -> str:
    """Shift ganji by step on stem+branch cycles (not 60-index, but aligned step)."""
    if not ganji or len(ganji) < 2:
        return ganji
    g, j = ganji[0], ganji[1]
    si = STEMS.index(g)
    bi = BRANCHES.index(j)
    return STEMS[(si + step) % 10] + BRANCHES[(bi + step) % 12]

def _daewoon_forward(gender: str, year_stem: str) -> bool:
    """
    순행/역행:
    - 남자+양년(甲丙戊庚壬) / 여자+음년(乙丁己辛癸) => 순행
    - 그 외 => 역행
    """
    g = (gender or "").lower()
    is_male = g in ("m", "male", "man", "남", "남자", "남성")
    is_female = g in ("f", "female", "woman", "여", "여자", "여성")
    yang = year_stem in YANG_STEMS
    if is_male:
        return bool(yang)
    if is_female:
        return not bool(yang)
    # unknown: default to yang-year forward, yin-year backward (stable)
    return bool(yang)

def _next_jieqi_dt(after_dt: datetime, jieqi_this_year: list, jieqi_next_year: list) -> datetime:
    """Return the next jieqi datetime strictly after after_dt (KST)."""
    cands = []
    for item in (jieqi_this_year or []):
        dt = _pick_item_dt(item)
        if dt and dt > after_dt:
            cands.append(dt)
    if cands:
        return min(cands)


def _prev_jieqi_dt(before_dt: datetime, jieqi_this_year: list, jieqi_prev_year: list) -> datetime:
    """Return the previous jieqi datetime strictly before before_dt (KST)."""
    cands = []
    for item in (jieqi_this_year or []):
        dt = _pick_item_dt(item)
        if dt and dt < before_dt:
            cands.append(dt)
    for item in (jieqi_prev_year or []):
        dt = _pick_item_dt(item)
        if dt and dt < before_dt:
            cands.append(dt)
    if not cands:
        # deterministic fallback
        return before_dt - timedelta(days=30)
    return max(cands)

    for item in (jieqi_next_year or []):
        dt = _pick_item_dt(item)
        if dt and dt > after_dt:
            cands.append(dt)
    if not cands:
        # should never happen if table is valid; keep deterministic fallback
        return after_dt + timedelta(days=30)
    return min(cands)

def _daewoon_start_age(
    input_dt: datetime,
    forward: bool,
    jieqi_this_year: list,
    jieqi_prev_year: list,
    jieqi_next_year: list,
) -> int:
    """
    점신 호환 대운수:
    - 순행: ceil((다음 절기 - 출생) / 3일)
    - 역행: floor((출생 - 이전 절기) / 3일)
    range clamp: 1..12
    """
    if forward:
        nxt = _next_jieqi_dt(input_dt, jieqi_this_year, jieqi_next_year)
        diff_days = (nxt - input_dt).total_seconds() / 86400.0
        age = int(_math.ceil(diff_days / 3.0))
    else:
        prv = _prev_jieqi_dt(input_dt, jieqi_this_year, jieqi_prev_year)
        diff_days = (input_dt - prv).total_seconds() / 86400.0
        age = int(_math.floor(diff_days / 3.0))

    if age < 1:
        age = 1
    if age > 12:
        age = 12
    return age

def build_fortune_bundle(
    input_dt: datetime,
    solar_confirmed_dt: date,
    year_pillar: dict,
    month_pillar: dict,
    gender: str,
    jieqi_this_year: list,
    jieqi_prev_year: list | None = None,
    jieqi_next_year: list | None = None,
    chart: dict | None = None,
) -> dict:
    """
    Returns dict to be attached as response["fortune"].
    Frontend can ignore fields it doesn't need.
    """
    birth_year = int(solar_confirmed_dt.year)

    forward = _daewoon_forward(gender, (year_pillar or {}).get("stem", ""))
    start_age = _daewoon_start_age(
        input_dt=input_dt,
        forward=forward,
        jieqi_this_year=jieqi_this_year,
        jieqi_prev_year=(jieqi_prev_year or []),
        jieqi_next_year=(jieqi_next_year or []),
    )

    base_ganji = (month_pillar or {}).get("ganji", "")
    daewoon = []
    for i in range(10):
        step = i if forward else -i
        ganji = _sexagenary_shift(base_ganji, step)
        from_age = start_age + i * 10
        to_age = from_age + 9
        from_year = birth_year + (from_age - 1)
        to_year = from_year + 9
        daewoon.append({
            "index": i,
            "start_age": start_age,
            "from_age": from_age,
            "to_age": to_age,
            "from_year": from_year,
            "to_year": to_year,
            "ganji": ganji,
            "direction": "forward" if forward else "backward",
        })

    # Yearly for the first daewoon block by default (frontend may pick other index)
    y_from = daewoon[0]["from_year"]
    y_to = daewoon[0]["to_year"]
    yearly_items = []
    for y in range(y_from, y_to + 1):
        yp = get_year_pillar(y)
        yearly_items.append({"year": y, "ganji": yp["ganji"], "stem": yp["stem"], "branch": yp["branch"]})

    # Monthly for the first year in that range by default
    monthly_year = y_from
    y_stem = get_year_pillar(monthly_year)["stem"]
    yin_month_stem = YEAR_STEM_TO_YIN_MONTH_STEM.get(y_stem, "丙")
    monthly_items = []
    for idx, b in enumerate(MONTH_BRANCH_SEQ):
        sidx = (STEMS.index(yin_month_stem) + idx) % 10
        ms = STEMS[sidx]
        monthly_items.append({
            "month_index": idx + 1,  # 1..12 (절기월: 寅월=1)
            "stem": ms,
            "branch": b,
            "ganji": ms + b,
        })

    # Daily calendar for 2026-2028 (3년 전체)
    daily_items = []
    
    # 현재 달만 일진 생성 (빠른 응답)
    daily_items = []
    
    if chart:
        now_kst = datetime.now(tz=KST)
        year = now_kst.year
        month = now_kst.month
        
        first = date(year, month, 1)
        if month == 12:
            next_first = date(year + 1, 1, 1)
        else:
            next_first = date(year, month + 1, 1)
        days = (next_first - first).days
        
        for d in range(1, days + 1):
            dd = date(year, month, d)
            dp = get_day_pillar(dd)
            level, reason = calc_daily_level(chart, dp)
            daily_items.append({
                "date": dd.isoformat(),
                "ganji": dp["ganji"],
                "stem": dp["stem"],
                "branch": dp["branch"],
                "level": level,
                "reason": reason,
                })



    # --------------------------------------------------
    # 4) Fortune bundle (대운/연운/월운/일진) — added only
    # --------------------------------------------------
    try:
        jieqi_next = get_jieqi_with_fallback(str(input_dt.year + 1))
        fortune_bundle = build_fortune_bundle(
            input_dt=input_dt,
            solar_confirmed_dt=solar_confirmed,
            year_pillar=year_pillar,
            month_pillar=month_pillar,
            gender=gender,
            jieqi_this_year=jieqi_this,
            jieqi_prev_year=jieqi_prev,
            jieqi_next_year=jieqi_next,
        )
    except Exception:
        fortune_bundle = {"daewoon": [], "yearly": {}, "monthly": {}, "daily": {}}

    return {
        "daewoon": daewoon,
        "yearly": {"range": {"from_year": y_from, "to_year": y_to}, "items": yearly_items},
        "monthly": {"year": monthly_year, "items": monthly_items},
        "daily": {"year": cal_y, "month": cal_m, "items": daily_items},
    }


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
    # --------------------------------------------------
    # 1) Interpret input date by calendar type
    # - calendar=solar: birth is solar YYYY-MM-DD
    # - calendar=lunar: birth is lunar YYYY-MM-DD (+ is_leap_month)
    # Always compute pillars based on confirmed solar date.
    # SSOT behavior:
    #   1) Try `calendar_ssot` cache first (birth+calendar+is_leap_month)
    #   2) Cache miss -> call KASI -> best-effort upsert
    # --------------------------------------------------
    try:
        birth_date_in = datetime.strptime(birth, "%Y-%m-%d").date()

        cached = ssot_lookup(birth_date_in, calendar, bool(is_leap_month))
        if cached and cached.get("solar_confirmed"):
            solar_confirmed = cached["solar_confirmed"]
            lunar_meta = cached.get("lunar_confirmed") or {}
        else:
            if (calendar or "").lower() == "lunar":
                sol = kasi_lun_to_sol(
                    birth_date_in.year, birth_date_in.month, birth_date_in.day, bool(is_leap_month)
                )
                solar_confirmed = date(sol["year"], sol["month"], sol["day"])
            else:
                solar_confirmed = birth_date_in

            lunar_meta = kasi_sol_to_lun(solar_confirmed.year, solar_confirmed.month, solar_confirmed.day)
            ssot_upsert(birth_date_in, calendar, bool(is_leap_month), solar_confirmed, lunar_meta)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"KASI/SSOT calendar conversion failed: {e}")
    # --------------------------------------------------
    # 2) Time handling (kept as-is)
    # --------------------------------------------------
    bt = (birth_time or "").strip().lower()
    if bt and bt not in ("unknown", "null", "none"):
        hh, mm = map(int, bt.split(":"))
        has_time = True
    else:
        hh, mm = 0, 0
        has_time = False

    input_dt = datetime(solar_confirmed.year, solar_confirmed.month, solar_confirmed.day, hh, mm, tzinfo=KST)
    calc_dt = input_dt - timedelta(minutes=SEOUL_FIXED_OFFSET_MINUTES) if has_time else input_dt

    # --------------------------------------------------
    # 3) Pillar calculation (solar-based)
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
    # 3.5) Enrich pillars for infographic (ten gods + hidden stems)
    # --------------------------------------------------
    pillars = {"year": year_pillar, "month": month_pillar, "day": day_pillar, "hour": hour_pillar}
    day_stem = (day_pillar or {}).get("stem", "")
    for _k in ("year", "month", "day", "hour"):
        _p = pillars.get(_k)
        if _p:
            enrich_pillar(_p, day_stem)
            # 12운성 (hour가 없으면 자동 스킵)
            _branch = _p.get("branch")
            if _branch:
                # 점신/당근 호환: 각 기둥의 '천간' 기준으로 12운성 산출
                    # (연주는 연간, 월주는 월간, 일주는 일간, 시주는 시간)
                    base_stem = day_stem
                    _p["twelve_stage"] = twelve_stage(base_stem, _branch)
                    _p["twelve_sinsal"] = twelve_sinsal(pillars.get("year",{}).get("branch",""), _branch)


    # --------------------------------------------------
    # 4) Fortune bundle (대운/연운/월운/일진) — added only
    # --------------------------------------------------
    try:
        jieqi_next = get_jieqi_with_fallback(str(input_dt.year + 1))
        
        # calc_daily_level에 필요한 정보를 chart에 추가
        elements_data = calculate_elements_ratio(pillars)
        branches = []
        for k in ["year", "month", "day", "hour"]:
            b = pillars.get(k, {}).get("branch")
            if b:
                branches.append(b)
        
        chart_for_daily = {
            **pillars,
            "day_stem": day_stem,
            "elements": elements_data,
            "branches": branches,
        }
        
        fortune_bundle = build_fortune_bundle(
            input_dt=input_dt,
            solar_confirmed_dt=solar_confirmed,
            year_pillar=year_pillar,
            month_pillar=month_pillar,
            gender=gender,
            jieqi_this_year=jieqi_this,
            jieqi_prev_year=jieqi_prev,
            jieqi_next_year=jieqi_next,
            chart=chart_for_daily,
        )
    except Exception as e:
        print(f"[ERROR] build_fortune_bundle failed: {e}")
        import traceback
        traceback.print_exc()
        fortune_bundle = {"daewoon": [], "yearly": {}, "monthly": {}, "daily": {}}

    return {
        "input": {
            "birth": birth,
            "calendar": calendar,
            "birth_time": birth_time,
            "gender": gender,
            "is_leap_month": is_leap_month,
        },
        "meta": {
            "solar_confirmed": {
                "year": input_dt.year,
                "month": input_dt.month,
                "day": input_dt.day,
                "label_kr": f"양력 {input_dt.year}년 {input_dt.month}월 {input_dt.day}일",
            },
            "lunar": lunar_meta,
            "elements": calculate_elements_ratio(pillars),
            "ten_gods": calculate_ten_gods_ratio(pillars),
            "daily_items": fortune_bundle.get("daily", {}).get("items", []),
        },
        "pillars": pillars,
        "fortune": fortune_bundle,
        "ilju_animal": get_ilju_animal(day_pillar.get("stem", ""), day_pillar.get("branch", "")),
        "ilju_emoji": get_ilju_emoji(day_pillar.get("branch", "")),
        "debug": {
            "timezone": "KST",
            "fixed_offset_minutes": SEOUL_FIXED_OFFSET_MINUTES if has_time else 0,
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

@app.get("/api/saju/daily")
def get_daily_level(
    birth: str = Query(...),
    calendar: str = Query("solar"),
    birth_time: str = Query("unknown"),
    gender: str = Query("unknown"),
    is_leap_month: bool = Query(False),
    year: int = Query(...),
    month: int = Query(...),
):
    """특정 년월의 일진 레벨 반환 (달 바뀔 때 호출)"""
    from fastapi import HTTPException
    
    # 원국 계산 (calc_saju와 동일한 로직)
    try:
        # 1) 날짜 파싱
        try:
            parts = birth.split("-")
            if len(parts) != 3:
                raise ValueError("Invalid date format")
            b_y, b_m, b_d = int(parts[0]), int(parts[1]), int(parts[2])
        except Exception:
            raise HTTPException(400, "Invalid birth date format")
        
        # 2) 음력/양력 처리
        if calendar == "lunar":
            from lunar_python import Lunar, Solar
            lunar = Lunar.fromYmd(b_y, b_m, b_d)
            if is_leap_month:
                lunar.setLeap(True)
            solar = lunar.getSolar()
            solar_confirmed = date(solar.getYear(), solar.getMonth(), solar.getDay())
        else:
            solar_confirmed = date(b_y, b_m, b_d)
        
        # 3) 시간 파싱
        has_time = birth_time and birth_time.strip().lower() not in ("unknown", "모름", "")
        if has_time:
            try:
                hm = birth_time.split(":")
                hour_int = int(hm[0])
                minute_int = int(hm[1]) if len(hm) > 1 else 0
            except Exception:
                hour_int, minute_int = 0, 0
                has_time = False
        else:
            hour_int, minute_int = 0, 0
        
        # 4) KST 시간 계산
        input_dt = datetime(solar_confirmed.year, solar_confirmed.month, solar_confirmed.day, hour_int, minute_int, tzinfo=KST)
        calc_dt = input_dt - timedelta(minutes=32)
        
        # 5) 사주 계산
        jieqi_this = get_jieqi_with_fallback(str(input_dt.year))
        jieqi_prev = get_jieqi_with_fallback(str(input_dt.year - 1))
        
        year_pillar = get_year_pillar(input_dt, jieqi_this, jieqi_prev)
        day_pillar = get_day_pillar(solar_confirmed)
        month_pillar = get_month_pillar(input_dt, year_pillar, jieqi_this, jieqi_prev)
        hour_pillar = get_hour_pillar(day_pillar, calc_dt.hour, calc_dt.minute) if has_time else None
        
        pillars = {"year": year_pillar, "month": month_pillar, "day": day_pillar, "hour": hour_pillar}
        day_stem = (day_pillar or {}).get("stem", "")
        
        for _k in ("year", "month", "day", "hour"):
            _p = pillars.get(_k)
            if _p:
                enrich_pillar(_p, day_stem)
        
        # 6) chart 생성
        elements_data = calculate_elements_ratio(pillars)
        branches = []
        for k in ["year", "month", "day", "hour"]:
            b = pillars.get(k, {}).get("branch")
            if b:
                branches.append(b)
        
        chart = {
            **pillars,
            "day_stem": day_stem,
            "elements": elements_data,
            "branches": branches,
        }
        
        # 7) 요청한 년월의 일진 생성
        daily_items = []
        first = date(year, month, 1)
        if month == 12:
            next_first = date(year + 1, 1, 1)
        else:
            next_first = date(year, month + 1, 1)
        days = (next_first - first).days
        
        for d in range(1, days + 1):
            dd = date(year, month, d)
            dp = get_day_pillar(dd)
            level, reason = calc_daily_level(chart, dp)
            daily_items.append({
                "date": dd.isoformat(),
                "ganji": dp["ganji"],
                "stem": dp["stem"],
                "branch": dp["branch"],
                "level": level,
                "reason": reason,
            })
        
        return {"daily_items": daily_items}
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] get_daily_level: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Internal error: {str(e)}")

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

@app.get("/api/generate-calendar")
def generate_calendar():
    """달력 데이터 생성"""
    import subprocess
    result = subprocess.run(
        ["python3", "generate_calendar_v3.py"],
        capture_output=True,
        text=True,
        cwd="."
    )
    
    if result.returncode == 0:
        # CalendarData.ts 파일 읽기
        try:
            with open("CalendarData.ts", "r", encoding="utf-8") as f:
                content = f.read()
            return {"success": True, "output": result.stdout, "file_size": len(content)}
        except:
            return {"success": True, "output": result.stdout, "note": "Check server logs"}
    else:
        return {"success": False, "error": result.stderr}

@app.get("/api/download-calendar")
def download_calendar():
    """달력 데이터 다운로드 (순수 TypeScript 파일)"""
    try:
        with open("CalendarData.ts", "r", encoding="utf-8") as f:
            content = f.read()
        
        # 순수 TypeScript 코드만 반환 (JSON 감싸지 않음)
        return Response(
            content=content,
            media_type="text/plain; charset=utf-8",
            headers={"Content-Disposition": "attachment; filename=CalendarData.ts"}
        )
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="CalendarData.ts not found. Run /api/generate-calendar first.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# ================================
# DAILY FORTUNE LEVEL (ADDED SAFE)
# ================================

TEN_SCORE = {
    "비견": -6, "겁재": -8,
    "식신": 10, "상관": 6,
    "정재": 10, "편재": 8,
    "정관": -6, "편관": -8,
    "정인": 6, "편인": 4,
}

STEM_ELEMENT_MAP = {
    "甲": "wood", "乙": "wood",
    "丙": "fire", "丁": "fire",
    "戊": "earth", "己": "earth",
    "庚": "metal", "辛": "metal",
    "壬": "water", "癸": "water",
}

BRANCH_RELATION_SCORE = {
    "충": -12,
    "합": 8,
}

BRANCH_CHUNG = {
    "子": "午", "午": "子",
    "丑": "未", "未": "丑",
    "寅": "申", "申": "寅",
    "卯": "酉", "酉": "卯",
    "辰": "戌", "戌": "辰",
    "巳": "亥", "亥": "巳",
}

def elem_balance_score(elem, elements):
    if not elements or elem not in elements:
        return 0
    ratio = elements.get(elem, {}).get("ratio", 0)
    if ratio < 10:
        return 12
    if ratio > 35:
        return -12
    return 0

def branch_relation_score(day_branch, origin_branches):
    score = 0
    for b in origin_branches:
        if BRANCH_CHUNG.get(day_branch) == b:
            score += BRANCH_RELATION_SCORE["충"]
    return score

def calc_daily_level(chart, day_pillar):
    """일진 레벨과 이유를 계산하여 반환"""
    score = 50
    reasons = []

    # 십성 점수 계산
    ten = None
    ten_score = 0
    try:
        ten = get_ten_god(chart["day_stem"], day_pillar["stem"])
        ten_score = TEN_SCORE.get(ten, 0)
        score += ten_score * 3  # 가중치 3배
    except Exception:
        pass

    # 오행 균형 점수 계산
    elem = None
    elem_score = 0
    try:
        elem = STEM_ELEMENT_MAP.get(day_pillar["stem"])
        elem_score = elem_balance_score(elem, chart.get("elements"))
        score += elem_score * 3  # 가중치 3배
    except Exception:
        pass

    # 지지 관계 점수 계산
    branch_score = 0
    chung_branches = []
    try:
        day_branch = day_pillar["branch"]
        for b in chart.get("branches", []):
            if BRANCH_CHUNG.get(day_branch) == b:
                chung_branches.append(b)
        branch_score = branch_relation_score(day_branch, chart.get("branches", []))
        score += branch_score * 3  # 가중치 3배
    except Exception:
        pass

    # 레벨 결정
    if score >= 80:
        level = "길일"
    elif score >= 60:
        level = "양호"
    elif score >= 40:
        level = "보통"
    elif score >= 30:
        level = "신중"
    else:
        level = "주의"



    # 모든 레벨에 이유 생성 (프론트에서 길일/주의만 표시)
    
    # 60갑자 한글 맵
    ganji_kr = {
        "甲子": "갑자", "乙丑": "을축", "丙寅": "병인", "丁卯": "정묘", "戊辰": "무진",
        "己巳": "기사", "庚午": "경오", "辛未": "신미", "壬申": "임신", "癸酉": "계유",
        "甲戌": "갑술", "乙亥": "을해", "丙子": "병자", "丁丑": "정축", "戊寅": "무인",
        "己卯": "기묘", "庚辰": "경진", "辛巳": "신사", "壬午": "임오", "癸未": "계미",
        "甲申": "갑신", "乙酉": "을유", "丙戌": "병술", "丁亥": "정해", "戊子": "무자",
        "己丑": "기축", "庚寅": "경인", "辛卯": "신묘", "壬辰": "임진", "癸巳": "계사",
        "甲午": "갑오", "乙未": "을미", "丙申": "병신", "丁酉": "정유", "戊戌": "무술",
        "己亥": "기해", "庚子": "경자", "辛丑": "신축", "壬寅": "임인", "癸卯": "계묘",
        "甲辰": "갑진", "乙巳": "을사", "丙午": "병오", "丁未": "정미", "戊申": "무신",
        "己酉": "기유", "庚戌": "경술", "辛亥": "신해", "壬子": "임자", "癸丑": "계축",
        "甲寅": "갑인", "乙卯": "을묘", "丙辰": "병진", "丁巳": "정사", "戊午": "무오",
        "己未": "기미", "庚申": "경신", "辛酉": "신유", "壬戌": "임술", "癸亥": "계해"
    }
    
    branch_animal = {
        "子": "쥐", "丑": "소", "寅": "호랑이", "卯": "토끼", "辰": "용", "巳": "뱀",
        "午": "말", "未": "양", "申": "원숭이", "酉": "닭", "戌": "개", "亥": "돼지"
    }
    
    elem_kr = {
        "wood": "나무", "fire": "불", "earth": "흙", "metal": "쇠", "water": "물"
    }
    
    ganji = day_pillar.get("ganji", "")
    stem = day_pillar.get("stem", "")
    branch = day_pillar.get("branch", "")
    
    ganji_name = ganji_kr.get(ganji, ganji)
    animal_name = branch_animal.get(branch, "")
    elem = STEM_ELEMENT_MAP.get(stem)
    elem_name = elem_kr.get(elem, "") if elem else ""
    
    # 문장 구성
    sentences = []
    
    # 1. 일진 소개
    if animal_name:
        sentences.append(f"오늘은 {ganji_name}({animal_name})의 날입니다.")
    else:
        sentences.append(f"오늘은 {ganji_name}의 날입니다.")
    
    # 2. 레벨 중심으로 이유 설명
    positive_reasons = []  # 긍정 요소
    negative_reasons = []  # 부정 요소
    
    # 오행 분석
    if elem_score > 0:
        positive_reasons.append(f"오늘의 {elem_name} 기운이 당신 원국에 부족한 {elem_name} 기운을 채워주어 균형이 좋아집니다")
    elif elem_score < 0:
        negative_reasons.append(f"당신 원국에 {elem_name} 기운이 이미 강한데 오늘도 {elem_name} 기운이 더해져 과해질 수 있습니다")
    
    # 지지 충돌
    if chung_branches:
        for b in chung_branches:
            b_animal = branch_animal.get(b, b)
            negative_reasons.append(f"오늘의 {animal_name} 지지가 원국의 {b_animal} 지지와 충돌합니다")
    
    # 십성 분석 (ten_score 활용)
    if ten_score >= 6:
        positive_reasons.append("일진의 십성이 당신에게 유리하게 작용합니다")
    elif ten_score <= -6:
        negative_reasons.append("일진의 십성이 긴장감을 주는 날입니다")
    
    # 지지 합 (branch_score가 양수인데 충돌이 아닌 경우)
    if branch_score > 0 and not chung_branches:
        positive_reasons.append("일진 지지가 원국과 조화롭게 어우러집니다")
    
    # 3. 길일/주의 결과에 따라 설명 조합
    if level == "길일":
        # 이유가 없으면 점수 다시 분석해서 추가
        if not positive_reasons:
            if ten_score > 0:
                positive_reasons.append(f"일진의 십성이 당신에게 유리하게 작용합니다")
            if elem_score > 0:
                positive_reasons.append(f"오늘의 {elem_name} 기운이 부족한 {elem_name} 기운을 채워줍니다")
            if branch_score > 0:
                positive_reasons.append("일진 지지가 원국과 조화롭게 어우러집니다")
            # 그래도 없으면 (기본 50점에서 높은 점수)
            if not positive_reasons:
                positive_reasons.append(f"일진 {ganji_name}이 전반적으로 원국과 좋은 흐름을 만듭니다")
        
        sentences.append(" ".join(positive_reasons) + ".")
        sentences.append("좋은 기운이 흐르는 길한 날입니다.")
    
    elif level == "주의":
        # 이유가 없으면 점수 다시 분석해서 추가
        if not negative_reasons:
            if ten_score < 0:
                negative_reasons.append("일진의 십성이 긴장감을 주는 날입니다")
            if elem_score < 0:
                negative_reasons.append(f"당신 원국에 {elem_name} 기운이 강한데 오늘도 더해져 과해집니다")
            if branch_score < 0:
                negative_reasons.append("일진 지지가 원국과 불편한 관계를 형성합니다")
            # 그래도 없으면 (기본 50점에서 낮은 점수)
            if not negative_reasons:
                negative_reasons.append(f"일진 {ganji_name}이 전반적으로 원국과 약한 흐름을 만듭니다")
        
        sentences.append(" ".join(negative_reasons) + ".")
        sentences.append("변동이나 긴장 상황에 신중하게 대응하세요.")
    
    else:  # 양호, 보통, 신중
        # 긍정/부정 요소가 있으면 둘 다 설명
        if positive_reasons and negative_reasons:
            sentences.append(" ".join(positive_reasons) + ".")
            sentences.append("다만, " + " ".join(negative_reasons) + ".")
        elif positive_reasons:
            sentences.append(" ".join(positive_reasons) + ".")
        elif negative_reasons:
            sentences.append(" ".join(negative_reasons) + ".")
    
    reason = " ".join(sentences)
    return level, reason