from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
import json
import os
import threading
from contextlib import contextmanager

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

# ⏱ 2026-09-06: 요청마다 psycopg2.connect 를 새로 열고 있었다. Railway→Supabase 핸드셰이크가
#   호출당 1~3초. 새 생년월일은 조회+저장으로 두 번 열어 그만큼 두 배로 샜다.
#   실측 결과 처음 보는 양력 12.5초 · 음력 7초 → 인스타 DM 자동응답이 매니챗 외부요청
#   제한(10초)에 걸려 손님에게 아무 답도 안 나가고 있었다. 풀을 쓴다.
_SSOT_POOL = None
_SSOT_POOL_LOCK = threading.Lock()
_SSOT_POOL_DEAD = False

def _ssot_pool():
    global _SSOT_POOL, _SSOT_POOL_DEAD
    if not (_SSOT_DB_OK and DATABASE_URL) or _SSOT_POOL_DEAD:
        return None
    if _SSOT_POOL is None:
        with _SSOT_POOL_LOCK:
            if _SSOT_POOL is None:
                try:
                    from psycopg2 import pool as _pgpool
                    _SSOT_POOL = _pgpool.ThreadedConnectionPool(
                        1, 8, DATABASE_URL,
                        cursor_factory=RealDictCursor,
                        connect_timeout=5,
                        keepalives=1, keepalives_idle=30,
                        keepalives_interval=10, keepalives_count=3,
                    )
                    print("[SSOT] POOL READY", flush=True)
                except Exception as e:
                    print("[SSOT] POOL FAIL", e, flush=True)
                    _SSOT_POOL_DEAD = True
                    _SSOT_POOL = None
    return _SSOT_POOL

def _ssot_get_conn():
    """풀을 못 쓸 때만 쓰는 예비 경로 — 예전 동작 그대로."""
    if not (_SSOT_DB_OK and DATABASE_URL):
        return None
    try:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except Exception:
        print("[SSOT] MISS", flush=True)
        return None

@contextmanager
def _ssot_conn():
    """풀에서 꺼내 쓰고 돌려준다. 끊긴 연결은 버린다(Supabase 가 유휴 연결을 닫는다)."""
    pool = _ssot_pool()
    if pool is None:
        conn = _ssot_get_conn()
        try:
            yield conn
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
        return
    conn = None
    bad = False
    try:
        conn = pool.getconn()
        yield conn
    except Exception:
        bad = True
        raise
    finally:
        if conn is not None:
            try:
                pool.putconn(conn, close=bad or conn.closed != 0)
            except Exception:
                pass

def ssot_lookup(birth_dt: date, calendar: str, is_leap_month: bool):
    """Return cached row dict or None."""
    try:
        with _ssot_conn() as conn:
            if not conn:
                print("[SSOT] MISS", flush=True)
                return None
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
            print("[SSOT] HIT" if row else "[SSOT] MISS", flush=True)
            return row
    except Exception:
        print("[SSOT] MISS", flush=True)
        return None

def ssot_upsert(birth_dt: date, calendar: str, is_leap_month: bool, solar_confirmed_dt: date, lunar_meta: dict):
    """Upsert cache row. Non-fatal on any error."""
    try:
        with _ssot_conn() as conn:
            if not conn:
                return
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
from pathlib import Path
import requests
import xml.etree.ElementTree as ET

print("[BOOT] main.py LOADED ✅", os.path.abspath(__file__), flush=True)

app = FastAPI(
    title="Saju API Server",
    version="1.9.0"
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://saju-baksa.com", "https://www.saju-baksa.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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

def _kasi_call_raw(endpoint: str, params: dict) -> dict:
    """KASI API 호출 - 전체 응답 JSON 리턴 (배열 처리용)"""
    if not KASI_SERVICE_KEY:
        raise RuntimeError("KASI_SERVICE_KEY is missing on server")

    q = {"serviceKey": KASI_SERVICE_KEY, "_type": "json"}
    q.update(params)
    url = f"{KASI_BASE}/{endpoint}"
    
    print(f"[DEBUG _kasi_call_raw] Endpoint: {endpoint}")
    print(f"[DEBUG _kasi_call_raw] Params: {params}")

    resp = requests.get(url, params=q, timeout=10)
    
    print(f"[DEBUG _kasi_call_raw] 실제 URL: {resp.url}")
    print(f"[DEBUG _kasi_call_raw] HTTP Status: {resp.status_code}")
    
    if resp.status_code != 200:
        raise RuntimeError(f"KASI HTTP {resp.status_code}: {resp.text[:200]}")
    
    try:
        data = resp.json()
        print(f"[DEBUG _kasi_call_raw] 응답 데이터 구조: {list(data.keys())}")
        return data
    except Exception as e:
        raise RuntimeError(f"KASI JSON parse failed: {e}")

def _kasi_call(endpoint: str, params: dict) -> dict:
    if not KASI_SERVICE_KEY:
        raise RuntimeError("KASI_SERVICE_KEY is missing on server")

    q = {"serviceKey": KASI_SERVICE_KEY, "_type": "json"}
    q.update(params)
    url = f"{KASI_BASE}/{endpoint}"
    
    # 🔍 DEBUG: KASI API 호출 정보
    print(f"[DEBUG _kasi_call] Endpoint: {endpoint}")
    print(f"[DEBUG _kasi_call] Input params: {params}")
    print(f"[DEBUG _kasi_call] Full params (q): {q}")
    print(f"[DEBUG _kasi_call] Base URL: {url}")

    resp = requests.get(url, params=q, timeout=10)
    
    # 🔍 DEBUG: 실제 요청 URL
    print(f"[DEBUG _kasi_call] 실제 요청 URL: {resp.url}")
    print(f"[DEBUG _kasi_call] HTTP Status: {resp.status_code}")
    
    if resp.status_code != 200:
        raise RuntimeError(f"KASI HTTP {resp.status_code}: {resp.text[:200]}")

    item = _kasi_parse_item(resp)
    if not item:
        raise RuntimeError(f"KASI returned empty item: {resp.text[:200]}")
    
    print(f"[DEBUG _kasi_call] 파싱된 응답: {item}")
    
    return item

# KASI 가 죽어 있을 때 매 요청 10초씩 버리지 않게 하는 차단기.
#   표기용 호출에만 쓴다 — 음력 변환(kasi_lun_to_sol)은 없으면 계산이 안 되므로 항상 시도한다.
_KASI_LABEL_FAIL_AT = 0.0
_KASI_LABEL_COOLDOWN = 300  # 5분

def _kasi_label_blocked() -> bool:
    import time as _t
    return (_t.time() - _KASI_LABEL_FAIL_AT) < _KASI_LABEL_COOLDOWN

def _kasi_label_fail():
    global _KASI_LABEL_FAIL_AT
    import time as _t
    _KASI_LABEL_FAIL_AT = _t.time()

# ── KASI 가 죽었을 때 쓰는 로컬 변환 (lunar-python) ─────────────────
#   2026-09-06 실장애: apis.data.go.kr 이 통째로 무응답이 되자 음력 주문이 전부 502.
#   lunar-python 은 requirements 에 이미 들어 있는데 안 쓰고 있었다.
#   ⚠️ 평상시엔 쓰지 않는다 — KASI(천문연) 가 한국 음력의 정본이다.
#      lunar-python 은 중국력 기준(UTC+8)이라 삭 시각이 자정에 걸리는 드문 날에
#      하루가 어긋날 수 있다. 그래도 멈춤보다는 낫다는 판단.
#   ⚠️ 로컬로 푼 값은 calendar_ssot 에 저장하지 않는다 — KASI 가 살아나면
#      정본으로 다시 받아야 한다. 어긋난 값이 영구히 박히면 안 된다.
def local_lun_to_sol(lun_year: int, lun_month: int, lun_day: int, is_leap_month: bool) -> dict:
    from lunar_python import Lunar
    mm = -lun_month if is_leap_month else lun_month
    sol = Lunar.fromYmd(lun_year, mm, lun_day).getSolar()
    return {"year": sol.getYear(), "month": sol.getMonth(), "day": sol.getDay()}

def local_sol_to_lun(sol_year: int, sol_month: int, sol_day: int) -> dict:
    from lunar_python import Solar
    lun = Solar.fromYmd(sol_year, sol_month, sol_day).getLunar()
    mm = lun.getMonth()
    leap = mm < 0
    mm = abs(mm)
    label = f"음력 {lun.getYear()}년 " + (f"윤{mm}월 " if leap else f"{mm}월 ") + f"{lun.getDay()}일"
    return {
        "year": lun.getYear(),
        "month": mm,
        "day": lun.getDay(),
        "is_leap_month": leap,
        "label_kr": label,
        "_raw": {"source": "local"},
    }

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
    
    # 🔍 DEBUG: 입력값 확인
    print(f"[DEBUG kasi_lun_to_sol] 입력 - year:{lun_year} month:{lun_month} day:{lun_day}")
    print(f"[DEBUG kasi_lun_to_sol] is_leap_month: {is_leap_month} (타입: {type(is_leap_month)})")
    
    # ✅ FIX: KASI API는 lunLeapmonth=1로 보내면 평달/윤달 둘 다 리턴
    print(f"[DEBUG kasi_lun_to_sol] KASI 전달값 lunLeapmonth: '1' (평달/윤달 모두 요청)")
    
    # KASI API 호출 - lunLeapmonth=1로 고정
    resp_data = _kasi_call_raw("getSolCalInfo", {
        "lunYear": str(lun_year),
        "lunMonth": f"{lun_month:02d}",
        "lunDay": f"{lun_day:02d}",
        "lunLeapmonth": "1",  # ← 한글 대신 1 사용
    })
    
    # 응답 파싱
    try:
        items = (
            resp_data.get("response", {})
                .get("body", {})
                .get("items", {})
                .get("item")
        )
        
        print(f"[DEBUG kasi_lun_to_sol] KASI 응답 아이템 개수: {len(items) if isinstance(items, list) else 1}")
        
        # 리스트면 is_leap_month에 맞는 것 선택
        if isinstance(items, list):
            target_leap = "윤" if is_leap_month else "평"
            item = None
            for it in items:
                if it.get("lunLeapmonth") == target_leap:
                    item = it
                    break
            
            if not item:
                print(f"[DEBUG kasi_lun_to_sol] ❌ {target_leap}달 데이터 없음! 첫 번째 사용")
                item = items[0]
            else:
                print(f"[DEBUG kasi_lun_to_sol] ✅ {target_leap}달 데이터 선택됨")
        else:
            item = items
        
        print(f"[DEBUG kasi_lun_to_sol] 선택된 item: {item}")
        
    except Exception as e:
        print(f"[DEBUG kasi_lun_to_sol] ❌ 파싱 에러: {e}")
        raise
    
    sol_year = int(item.get("solYear"))
    sol_month = int(item.get("solMonth"))
    sol_day = int(item.get("solDay"))
    
    print(f"[DEBUG kasi_lun_to_sol] 변환결과 - 양력 {sol_year}-{sol_month:02d}-{sol_day:02d}")
    
    return {"year": sol_year, "month": sol_month, "day": sol_day}

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

# 절(節) 이름 — 대운 계산은 절(節)만 사용, 중기(中氣) 제외
JIE_NAMES = {
    "입춘", "경칩", "청명", "입하", "망종", "소서",
    "입추", "백로", "한로", "립동", "입동", "대설", "소한",
    "立春", "驚蟄", "清明", "立夏", "芒種", "小暑",
    "立秋", "白露", "寒露", "立冬", "大雪", "小寒",
}

def _is_jie(item: dict) -> bool:
    """절기가 절(節)인지 확인 (중기 제외)"""
    name = item.get("name", "")
    return name in JIE_NAMES

def _next_jieqi_dt(after_dt: datetime, jieqi_this_year: list, jieqi_next_year: list) -> datetime:
    """Return the next 절(節) datetime strictly after after_dt (KST)."""
    cands = []
    for item in (jieqi_this_year or []):
        if not _is_jie(item):
            continue
        dt = _pick_item_dt(item)
        if dt and dt > after_dt:
            cands.append(dt)
    for item in (jieqi_next_year or []):
        if not _is_jie(item):
            continue
        dt = _pick_item_dt(item)
        if dt and dt > after_dt:
            cands.append(dt)
    if not cands:
        return after_dt + timedelta(days=30)
    return min(cands)


def _prev_jieqi_dt(before_dt: datetime, jieqi_this_year: list, jieqi_prev_year: list) -> datetime:
    """Return the previous 절(節) datetime strictly before before_dt (KST)."""
    cands = []
    for item in (jieqi_this_year or []):
        if not _is_jie(item):
            continue
        dt = _pick_item_dt(item)
        if dt and dt < before_dt:
            cands.append(dt)
    for item in (jieqi_prev_year or []):
        if not _is_jie(item):
            continue
        dt = _pick_item_dt(item)
        if dt and dt < before_dt:
            cands.append(dt)
    if not cands:
        return before_dt - timedelta(days=30)
    return max(cands)

def _daewoon_start_age(
    input_dt: datetime,
    forward: bool,
    jieqi_this_year: list,
    jieqi_prev_year: list,
    jieqi_next_year: list,
) -> int:
    """
    점신 호환 대운수:
    - 순행: (다음 절기 - 출생) / 3일, 나머지 2일 이상이면 +1
    - 역행: (출생 - 이전 절기) / 3일, 나머지 2일 이상이면 +1
    range clamp: 1..12
    """
    if forward:
        nxt = _next_jieqi_dt(input_dt, jieqi_this_year, jieqi_next_year)
        diff_days = (nxt - input_dt).total_seconds() / 86400.0
    else:
        prv = _prev_jieqi_dt(input_dt, jieqi_this_year, jieqi_prev_year)
        diff_days = (input_dt - prv).total_seconds() / 86400.0

    q = int(diff_days) // 3
    r = int(diff_days) % 3
    age = q + (1 if r >= 2 else 0)

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
        step = (i + 1) if forward else -(i + 1)
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

    # Yearly: 현재 연도가 포함된 대운 블록 기준 (fallback: 첫 번째 블록)
    current_year = date.today().year
    active_dw = daewoon[0]  # fallback
    for dw in daewoon:
        if dw["from_year"] <= current_year <= dw["to_year"]:
            active_dw = dw
            break
    y_from = active_dw["from_year"]
    y_to = active_dw["to_year"]
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
        "daily": {"items": daily_items},
    }


@app.get("/api/saju/calc")
def calc_saju(
    birth: str = Query(...),
    calendar: str = Query("solar"),
    birth_time: str = Query("unknown"),
    gender: str = Query("unknown"),
    is_leap_month: str = Query("false"),
    # 음력 표기(KASI 3초)가 필요 없는 호출용 — 인스타 DM 티저만 켠다.
    # 보고서·표지·메일은 [음력 …] 을 쓰므로 기본값은 반드시 false 다.
    skip_lunar_label: str = Query("false"),
):
    from fastapi import HTTPException

    # 🔍 DEBUG: is_leap_month 파라미터 확인
    print(f"[DEBUG calc_saju] is_leap_month 원본값: {is_leap_month}")
    print(f"[DEBUG calc_saju] is_leap_month 타입: {type(is_leap_month)}")
    
    # Convert is_leap_month string to bool
    is_leap_bool = str(is_leap_month).lower() in ["true", "1", "yes"]
    
    print(f"[DEBUG calc_saju] is_leap_bool 변환결과: {is_leap_bool}")
    print(f"[DEBUG calc_saju] calendar: {calendar}")

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

        cached = ssot_lookup(birth_date_in, calendar, is_leap_bool)
        if cached and cached.get("solar_confirmed"):
            solar_confirmed = cached["solar_confirmed"]
            lunar_meta = cached.get("lunar_confirmed") or {}
        else:
            _kasi_local = False   # 로컬 변환으로 푼 값이면 캐시에 넣지 않는다
            if (calendar or "").lower() == "lunar":
                try:
                    sol = kasi_lun_to_sol(
                        birth_date_in.year, birth_date_in.month, birth_date_in.day, is_leap_bool
                    )
                except Exception as _ce:
                    # KASI 가 죽어도 음력 주문을 멈추지 않는다. 로컬 표로 푼다.
                    print("[KASI] 음력→양력 실패 — 로컬 변환으로 대체:", _ce, flush=True)
                    sol = local_lun_to_sol(
                        birth_date_in.year, birth_date_in.month, birth_date_in.day, is_leap_bool
                    )
                    _kasi_local = True
                solar_confirmed = date(sol["year"], sol["month"], sol["day"])
            else:
                solar_confirmed = birth_date_in

            if str(skip_lunar_label).strip().lower() in ("1", "true", "yes", "y"):
                # 음력 표기만 건너뛴다. 캐시에는 쓰지 않는다 —
                # 빈 음력으로 덮으면 나중에 보고서가 그 빈 값을 읽는다.
                lunar_meta = {}
            else:
                try:
                    if _kasi_label_blocked():
                        raise RuntimeError("KASI 최근 실패 — 잠시 건너뜀")
                    lunar_meta = kasi_sol_to_lun(solar_confirmed.year, solar_confirmed.month, solar_confirmed.day)
                    if not _kasi_local:
                        ssot_upsert(birth_date_in, calendar, is_leap_bool, solar_confirmed, lunar_meta)
                except Exception as _e:
                    # ⚠️ 2026-09-06 실장애: KASI(apis.data.go.kr)가 통째로 무응답이 되자
                    #   양력 주문까지 502 로 죽었다. 양력은 KASI 없이도 사주가 나온다 —
                    #   못 붙는 건 [음력 …] 표기뿐이다. 표기 하나 때문에 주문을 멈추지 않는다.
                    #   음력 입력의 변환도 로컬(lunar-python)로 대체한다 — 위 분기 참고.
                    print("[KASI] 음력 표기 실패 — 표기 없이 진행:", _e, flush=True)
                    _kasi_label_fail()
                    try:
                        lunar_meta = local_sol_to_lun(solar_confirmed.year, solar_confirmed.month, solar_confirmed.day)
                    except Exception as _le:
                        print("[KASI] 로컬 음력 표기도 실패 — 표기 없이 진행:", _le, flush=True)
                        lunar_meta = {}
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
            p = pillars.get(k); b = p.get("branch") if p else None
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
        
        # 🔥 연애운 캘린더는 프론트에서 API 호출로 처리 (종합사주와 동일)
        # calculate_love_calendar 삭제됨
        
        # 🔥 재물운 캘린더 추가 (3개년) - TODO: 구현 필요
        # try:
        #     money_cal = calculate_money_calendar(
        #         chart=chart_for_daily,
        #         start_year=input_dt.year,
        #         num_years=3
        #     )
        #     fortune_bundle["money_calendar"] = money_cal.get("daily_items", [])
        # except Exception as e:
        #     print(f"[ERROR] calculate_money_calendar failed: {e}")
        #     fortune_bundle["money_calendar"] = []
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
            "is_leap_month": is_leap_bool,
        },
        "meta": {
            # 프론트 표시용 추가
            "birth": birth,                    # 원본 입력 (음력이면 음력, 양력이면 양력)
            "calendar": calendar,              # "solar" or "lunar"
            "birth_time": birth_time,
            "gender": gender,
            "is_leap_month": is_leap_bool,    # 윤달 여부
            
            # 양력 변환 결과 (문자열)
            "solar": f"{input_dt.year}-{input_dt.month:02d}-{input_dt.day:02d}",
            "solar_time": birth_time,
            
            # 기존 구조 유지
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
    is_leap_month: str = Query("false"),
    year: int = Query(...),
    month: int = Query(...),
):
    """특정 년월의 일진 레벨 반환 (달 바뀔 때 호출)"""
    from fastapi import HTTPException
    
    # Convert is_leap_month string to bool
    is_leap_bool = str(is_leap_month).lower() in ["true", "1", "yes"]
    
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
            # ✅ KASI API로 음력→양력 변환
            sol = kasi_lun_to_sol(b_y, b_m, b_d, is_leap_bool)
            solar_confirmed = date(sol["year"], sol["month"], sol["day"])
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
        
        year_pillar = get_year_pillar(input_dt.year)
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
            p = pillars.get(k); b = p.get("branch") if p else None
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
        raise HTTPException(500, f"Internal error: {str(e)}")


@app.get("/api/saju/love-daily")
def get_love_daily(
    birth: str = Query(...),
    calendar: str = Query("solar"),
    birth_time: str = Query("unknown"),
    gender: str = Query("unknown"),
    is_leap_month: str = Query("false"),
    year: int = Query(...),
    month: int = Query(...),
):
    """특정 년월의 연애운 레벨 반환 (달 바뀔 때 호출)"""
    from fastapi import HTTPException
    
    # Convert is_leap_month string to bool
    is_leap_bool = str(is_leap_month).lower() in ["true", "1", "yes"]
    
    # 원국 계산 (get_daily_level과 동일)
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
            # ✅ KASI API로 음력→양력 변환
            sol = kasi_lun_to_sol(b_y, b_m, b_d, is_leap_bool)
            solar_confirmed = date(sol["year"], sol["month"], sol["day"])
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
        
        # 5) 사주 계산
        jieqi_this = get_jieqi_with_fallback(str(input_dt.year))
        jieqi_prev = get_jieqi_with_fallback(str(input_dt.year - 1))
        
        year_pillar = get_year_pillar(input_dt.year)
        day_pillar_birth = get_day_pillar(solar_confirmed)
        month_pillar = get_month_pillar(input_dt, year_pillar, jieqi_this, jieqi_prev)
        
        pillars = {"year": year_pillar, "month": month_pillar, "day": day_pillar_birth}
        day_stem = (day_pillar_birth or {}).get("stem", "")
        day_branch = (day_pillar_birth or {}).get("branch", "")
        
        # 원국 지지 추출
        origin_branches = []
        for k in ["year", "month", "day", "hour"]:
            p = pillars.get(k); b = p.get("branch") if p else None
            if b:
                origin_branches.append(b)
        
        # 6) 요청한 년월의 연애운 일진 생성
        daily_items = []
        first = date(year, month, 1)
        if month == 12:
            next_first = date(year + 1, 1, 1)
        else:
            next_first = date(year, month + 1, 1)
        days = (next_first - first).days
        
        for d in range(1, days + 1):
            dd = date(year, month, d)
            daily_pillar = get_day_pillar(dd)
            daily_stem = daily_pillar.get("stem", "")
            daily_branch = daily_pillar.get("branch", "")
            
            # 연애운 레벨 계산
            level_num, message = calculate_love_day(
                day_stem, day_branch, daily_stem, daily_branch, gender, origin_branches
            )
            
            # 레벨을 문자열로 변환
            level_map = {
                2: {"level": "충만", "icon": "❤️"},
                1: {"level": "탐색", "icon": "💭"},
                0: {"level": "경계", "icon": "💔"}
            }
            level_data = level_map.get(level_num, {"level": "탐색", "icon": "💭"})
            
            daily_items.append({
                "date": dd.isoformat(),
                "level": level_data["level"],
                "icon": level_data["icon"],
                "message": message
            })
        
        return {"daily_items": daily_items}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Internal error: {str(e)}")


@app.get("/api/saju/money-daily")
def get_money_daily(
    birth: str = Query(...),
    calendar: str = Query("solar"),
    birth_time: str = Query("unknown"),
    gender: str = Query("unknown"),
    is_leap_month: str = Query("false"),
    year: int = Query(...),
    month: int = Query(...),
):
    """특정 년월의 재물운 레벨 반환 (달 바뀔 때 호출)"""
    from fastapi import HTTPException
    
    # Convert is_leap_month string to bool
    is_leap_bool = str(is_leap_month).lower() in ["true", "1", "yes"]
    
    # 원국 계산
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
            # ✅ KASI API로 음력→양력 변환
            sol = kasi_lun_to_sol(b_y, b_m, b_d, is_leap_bool)
            solar_confirmed = date(sol["year"], sol["month"], sol["day"])
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
        
        # 5) 사주 계산
        jieqi_this = get_jieqi_with_fallback(str(input_dt.year))
        jieqi_prev = get_jieqi_with_fallback(str(input_dt.year - 1))
        
        year_pillar = get_year_pillar(input_dt.year)
        day_pillar_birth = get_day_pillar(solar_confirmed)
        month_pillar = get_month_pillar(input_dt, year_pillar, jieqi_this, jieqi_prev)
        
        pillars = {"year": year_pillar, "month": month_pillar, "day": day_pillar_birth}
        day_stem = (day_pillar_birth or {}).get("stem", "")
        day_branch = (day_pillar_birth or {}).get("branch", "")
        
        # 원국 지지 추출
        origin_branches = []
        for k in ["year", "month", "day", "hour"]:
            p = pillars.get(k); b = p.get("branch") if p else None
            if b:
                origin_branches.append(b)
        
        # 6) 요청한 년월의 재물운 일진 생성
        daily_items = []
        first = date(year, month, 1)
        if month == 12:
            next_first = date(year + 1, 1, 1)
        else:
            next_first = date(year, month + 1, 1)
        days = (next_first - first).days
        
        for d in range(1, days + 1):
            dd = date(year, month, d)
            daily_pillar = get_day_pillar(dd)
            daily_stem = daily_pillar.get("stem", "")
            daily_branch = daily_pillar.get("branch", "")
            
            # 재물운 레벨 계산
            level_num, message = calculate_money_day(
                day_stem, day_branch, daily_stem, daily_branch, origin_branches
            )
            
            # 레벨을 문자열로 변환
            level_map = {
                2: {"level": "상승", "icon": "💵", "color": "#fef3c7"},
                1: {"level": "관망", "icon": "🔭", "color": "#ffffff"},
                0: {"level": "손해", "icon": "📉", "color": "#dbeafe"}
            }
            level_data = level_map.get(level_num, {"level": "관망", "icon": "🔭", "color": "#ffffff"})
            
            daily_items.append({
                "date": dd.isoformat(),
                "level": level_data["level"],
                "icon": level_data["icon"],
                "color": level_data["color"],
                "message": message
            })
        
        return {"daily_items": daily_items}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Internal error: {str(e)}")
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

    # 시간모름 판별 (오행 비율 왜곡 방지용)
    branches = chart.get("branches", [])
    no_hour = len(branches) <= 3

    # 십성 점수 계산
    ten = None
    ten_score = 0
    try:
        ten = ten_god_of_stem(chart["day_stem"], day_pillar["stem"])
        ten_score = TEN_SCORE.get(ten, 0)
        score += ten_score * 3
    except Exception:
        pass

    # 오행 균형 점수 계산 (시간모름이면 비율 왜곡되므로 중립 처리)
    elem = None
    elem_score = 0
    try:
        if not no_hour:
            elem = STEM_ELEMENT_MAP.get(day_pillar["stem"])
            elem_score = elem_balance_score(elem, chart.get("elements"))
            score += elem_score * 3
    except Exception:
        pass

    # 지지 관계 점수 계산
    branch_score = 0
    chung_branches = []
    try:
        day_branch = day_pillar["branch"]
        for b in chart.get("branches", []):
            if BRANCH_CHUNG.get(day_branch) == b and b not in chung_branches:
                chung_branches.append(b)
        branch_score = branch_relation_score(day_branch, chart.get("branches", []))
        score += branch_score * 3
    except Exception:
        pass

    # 3단계 레벨 결정
    if score >= 75:
        level = "길일"
    elif score < 30:
        level = "주의"
    else:
        level = "보통"



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
    
    # 십성 한글 매핑
    ten_god_meaning = {
        "비견": "나와 같은 기운이 힘을 실어주는",
        "겁재": "경쟁 에너지가 강해지는",
        "식신": "표현력과 창의력이 빛나는",
        "상관": "자유롭고 거침없는 에너지가 흐르는",
        "정재": "안정적인 재물 기운이 들어오는",
        "편재": "뜻밖의 재물 기회가 열리는",
        "정관": "책임감과 질서가 강해지는",
        "편관": "외부 압박과 긴장이 커지는",
        "정인": "지혜와 배움의 기운이 도는",
        "편인": "직관과 영감이 살아나는",
    }
    
    # 오행 분석
    if elem_score > 0:
        positive_reasons.append(f"오늘의 {elem_name} 기운이 사주에 부족한 부분을 채워주면서 전체 균형이 좋아지는 날입니다")
    elif elem_score < 0:
        negative_reasons.append(f"사주에 이미 강한 {elem_name} 기운이 오늘 더 쌓이면서 에너지가 한쪽으로 치우칠 수 있습니다")
    
    # 조사 헬퍼 (받침 유무 판단)
    def _has_batchim(word: str) -> bool:
        if not word:
            return False
        last = ord(word[-1])
        if 0xAC00 <= last <= 0xD7A3:
            return (last - 0xAC00) % 28 != 0
        return False
    
    def _josa_nun(word: str) -> str:
        return "은" if _has_batchim(word) else "는"
    
    def _josa_wa(word: str) -> str:
        return "과" if _has_batchim(word) else "와"
    
    # 지지 충돌
    if chung_branches:
        for b in chung_branches:
            b_animal = branch_animal.get(b, b)
            negative_reasons.append(f"오늘 {animal_name}의 기운이 사주 속 {b_animal}의 기운과 정면으로 부딪히면서 예상치 못한 변수가 생기기 쉽습니다")
    
    # 십성 분석 (구체적 텍스트)
    if ten and ten in ten_god_meaning:
        meaning = ten_god_meaning[ten]
        if ten_score >= 6:
            positive_reasons.append(f"오늘은 {meaning} 날로, {ganji_name}의 흐름이 당신에게 유리하게 작용합니다")
        elif ten_score <= -6:
            negative_reasons.append(f"오늘은 {meaning} 날로, 평소보다 신중하게 움직이는 것이 좋습니다")
    
    # 지지 합 (branch_score가 양수인데 충돌이 아닌 경우)
    if branch_score > 0 and not chung_branches:
        positive_reasons.append(f"오늘 {animal_name}의 기운이 원국 지지와 조화롭게 어우러져 안정감을 줍니다")
    
    # 마지막 문장 배리에이션 (날짜 기반 선택)
    day_num = day_pillar.get("branch", "子")
    branch_idx = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"].index(day_num) if day_num in ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"] else 0
    
    good_endings = [
        "새로운 시도나 중요한 약속을 잡기에 길한 날입니다.",
        "평소 미뤄왔던 일을 추진하기 좋은 길한 날입니다.",
        "사람을 만나거나 계약을 진행하기에 길한 날입니다.",
        "자신감을 갖고 적극적으로 움직여도 좋은 길한 날입니다.",
        "결단력이 빛을 발하는 길한 날입니다.",
        "일과 관계 모두 순조롭게 풀리기 쉬운 길한 날입니다.",
    ]
    
    bad_endings_chung = [
        "중요한 결정은 하루 미루고 차분하게 보내세요.",
        "대인관계에서 오해가 생기기 쉬우니 말을 아끼세요.",
        "급한 일이 아니라면 내일로 넘기는 것이 현명합니다.",
        "컨디션 관리에 신경 쓰고 무리하지 마세요.",
        "예민해지기 쉬운 날이니 한 발짝 물러서 보세요.",
        "서두르지 말고 한 템포 쉬어가는 것이 좋습니다.",
    ]
    
    bad_endings_normal = [
        "무리한 일정이나 큰 결정은 피하고 신중하게 대응하세요.",
        "평소보다 보수적으로 움직이는 것이 유리합니다.",
        "에너지를 아끼고 내일을 준비하는 하루로 보내세요.",
        "감정적인 판단보다 논리적으로 접근하는 것이 좋습니다.",
        "작은 일에 집중하고 큰 그림은 내일 다시 보세요.",
        "조용히 자신을 돌아보는 시간으로 활용하세요.",
    ]
    
    # 3. 길일/주의 결과에 따라 설명 조합
    if level == "길일":
        if positive_reasons:
            sentences.append(" ".join(positive_reasons) + ".")
        else:
            sentences.append(f"{ganji_name}의 기운이 원국과 좋은 흐름을 만들어 여러 방면에서 순조로운 날입니다.")
        sentences.append(good_endings[branch_idx % len(good_endings)])
    
    elif level == "주의":
        if negative_reasons:
            sentences.append(" ".join(negative_reasons) + ".")
        else:
            sentences.append(f"{ganji_name}의 기운이 원국과 어긋나면서 흐름이 불안정한 날입니다.")
        if chung_branches:
            sentences.append(bad_endings_chung[branch_idx % len(bad_endings_chung)])
        else:
            sentences.append(bad_endings_normal[branch_idx % len(bad_endings_normal)])
    
    else:  # 보통
        if positive_reasons and negative_reasons:
            sentences.append(" ".join(positive_reasons) + ".")
            sentences.append("다만, " + " ".join(negative_reasons) + ".")
        elif positive_reasons:
            sentences.append(" ".join(positive_reasons) + ".")
        elif negative_reasons:
            sentences.append(" ".join(negative_reasons) + ".")
    
    reason = " ".join(sentences)
    return level, reason


# ==================================================
# LOVE FORTUNE CALENDAR (연애운 캘린더)
# ==================================================

# 천간합
HEAVENLY_STEM_HARMONY_LOVE = {
    "甲": "己", "己": "甲", "乙": "庚", "庚": "乙", "丙": "辛", 
    "辛": "丙", "丁": "壬", "壬": "丁", "戊": "癸", "癸": "戊",
}

# 지지충
EARTHLY_BRANCH_CLASH_LOVE = {
    "子": "午", "午": "子", "丑": "未", "未": "丑", "寅": "申", 
    "申": "寅", "卯": "酉", "酉": "卯", "辰": "戌", "戌": "辰", "巳": "亥", "亥": "巳",
}

# 도화살
PEACH_BLOSSOM_BRANCHES_LOVE = {"子", "午", "卯", "酉"}


def get_wealth_star_love(day_stem: str) -> str:
    """재성 - 일간이 극하는 오행 (남자 이성운)"""
    elem = STEM_ELEMENT.get(day_stem)
    control = {"목": "토", "화": "금", "토": "수", "금": "목", "수": "화"}
    return control.get(elem, "")


def get_officer_star_love(day_stem: str) -> str:
    """관성 - 일간을 극하는 오행 (여자 이성운)"""
    elem = STEM_ELEMENT.get(day_stem)
    controlled = {"목": "금", "화": "수", "토": "목", "금": "화", "수": "토"}
    return controlled.get(elem, "")


def calculate_love_day(day_stem: str, day_branch: str, daily_stem: str, daily_branch: str, gender: str, origin_branches: list) -> tuple:
    """하루 연애운 계산"""
    positive_score = 0
    negative_score = 0
    positive_reasons = []
    negative_reasons = []
    
    # 오행 한글 매핑
    elem_kr_map = {"목": "나무", "화": "불", "토": "흙", "금": "쇠", "수": "물"}
    
    # 1. 천간합
    if HEAVENLY_STEM_HARMONY_LOVE.get(day_stem) == daily_stem:
        positive_score += 1
        day_elem = STEM_ELEMENT.get(day_stem, "")
        daily_elem = STEM_ELEMENT.get(daily_stem, "")
        day_elem_kr = elem_kr_map.get(day_elem, day_elem)
        daily_elem_kr = elem_kr_map.get(daily_elem, daily_elem)
        positive_reasons.append(f"타고난 성향의 {day_elem_kr} 기운과 오늘의 {daily_elem_kr} 기운이 서로 합을 이루어 사람 사이의 흐름이 부드럽게 풀리는 날입니다")
    
    # 2. 재성/관성
    if gender == "male":
        target_elem = get_wealth_star_love(day_stem)
        daily_elem = STEM_ELEMENT.get(daily_stem, "")
        if target_elem == daily_elem:
            positive_score += 1
            positive_reasons.append("연애와 이성을 끌어당기는 기운이 강하게 작용해 자연스럽게 호감이 오가기 쉬워요")
    else:
        target_elem = get_officer_star_love(day_stem)
        daily_elem = STEM_ELEMENT.get(daily_stem, "")
        if target_elem == daily_elem:
            positive_score += 1
            positive_reasons.append("관계에서 매력이 드러나는 기운이 강하게 작용하는 날입니다")
    
    # 3. 도화살
    if daily_branch in PEACH_BLOSSOM_BRANCHES_LOVE:
        positive_score += 1
        daily_animal = BRANCH_ANIMAL_KR.get(daily_branch, "")
        positive_reasons.append(f"오늘의 {daily_animal} 기운이 인연을 활발하게 만들어 주면서 이성과의 접점이 자연스럽게 늘어날 수 있어요")
    
    # 4. 지지충
    for origin_br in origin_branches:
        if EARTHLY_BRANCH_CLASH_LOVE.get(daily_branch) == origin_br:
            negative_score += 1
            daily_animal = BRANCH_ANIMAL_KR.get(daily_branch, "")
            origin_animal = BRANCH_ANIMAL_KR.get(origin_br, "")
            negative_reasons.append(f"오늘의 {daily_animal} 기운이 원국 속 {origin_animal} 기운과 정면으로 부딪히는 날입니다. 대화에서 오해가 생기기 쉽고")
            break
    
    # 5. 비겁 과다
    if STEM_ELEMENT.get(day_stem) == STEM_ELEMENT.get(daily_stem):
        negative_score += 1
        elem = STEM_ELEMENT.get(day_stem, "")
        elem_kr = elem_kr_map.get(elem, elem)
        if negative_reasons:
            negative_reasons.append(f"타고난 성향과 오늘의 {elem_kr} 기운이 겹치면서 경쟁심도 커질 수 있어요")
        else:
            negative_reasons.append(f"타고난 성향과 오늘의 {elem_kr} 기운이 겹치면서 경쟁 상황이 생기기 쉬운 날입니다")
    
    # 6. 레벨 판정
    if positive_score >= 2 and negative_score == 0:
        level = 2  # 충만
    elif negative_score >= 1 and positive_score == 0:
        level = 0  # 경계
    elif negative_score >= 2:
        level = 0  # 경계
    else:
        level = 1  # 탐색
    
    # 7. 메시지 조합
    message_parts = []
    if level == 2:
        if positive_reasons:
            message_parts.extend(positive_reasons)
        else:
            message_parts.append("전반적으로 좋은 연애 기운이 흐르는 날입니다")
        message_parts.append("오늘은 괜히 망설이기보다 먼저 다가가도 좋은 하루입니다")
    elif level == 0:
        if negative_reasons:
            message_parts.extend(negative_reasons)
        else:
            message_parts.append("연애에 긴장감이 있는 날입니다")
        message_parts.append("오늘은 연애에서 한발 물러서 신중하게 행동하는 게 좋습니다")
    else:
        if positive_reasons and negative_reasons:
            message_parts.extend(positive_reasons[:1])  # 긍정 1개만
            message_parts.append("다만, " + negative_reasons[0])
            message_parts.append("관찰하며 신중하게 접근하는 게 좋습니다")
        elif positive_reasons:
            message_parts.extend(positive_reasons[:1])
            message_parts.append("천천히 흐름을 타는 게 좋습니다")
        elif negative_reasons:
            message_parts.extend(negative_reasons[:1])
            message_parts.append("무리하지 말고 기회를 엿보는 정도가 적당합니다")
        else:
            message_parts.append("오늘은 연애 기운이 특별히 강하게 작동하지 않는 평범한 흐름입니다. 연애보다는 자신을 돌아보고 준비하는 시간으로 활용하기에 좋은 날입니다")
    
    message = " ".join(message_parts) + "."
    return level, message


def calculate_love_calendar(chart: dict, gender: str, start_year: int, num_years: int = 3) -> dict:
    """연애운 캘린더 계산 (사용 안 함 - API 방식으로 변경됨)"""
    pass


def calculate_money_day(day_stem: str, day_branch: str, daily_stem: str, daily_branch: str, origin_branches: list) -> tuple:
    """하루 재물운 계산"""
    positive_score = 0
    negative_score = 0
    positive_reasons = []
    negative_reasons = []
    
    # 오행 한글 매핑
    elem_kr_map = {"목": "나무", "화": "불", "토": "흙", "금": "쇠", "수": "물"}
    
    # 일진 동물 이름
    daily_animal = BRANCH_ANIMAL_KR.get(daily_branch, "")
    
    # 조사 헬퍼
    def _josa_i(word: str) -> str:
        """받침 있으면 '이', 없으면 '가'"""
        if not word:
            return "이"
        last = ord(word[-1])
        if 0xAC00 <= last <= 0xD7A3:
            return "이" if (last - 0xAC00) % 28 != 0 else "가"
        return "이"
    
    def _josa_eun(word: str) -> str:
        """받침 있으면 '은', 없으면 '는'"""
        if not word:
            return "은"
        last = ord(word[-1])
        if 0xAC00 <= last <= 0xD7A3:
            return "은" if (last - 0xAC00) % 28 != 0 else "는"
        return "은"
    
    # 1. 식상생재 (일간→식상→재성 흐름)
    day_elem = STEM_ELEMENT.get(day_stem, "")
    daily_elem = STEM_ELEMENT.get(daily_stem, "")
    generates = {"목": "화", "화": "토", "토": "금", "금": "수", "수": "목"}
    
    if generates.get(day_elem) == daily_elem:
        positive_score += 1
        day_elem_kr = elem_kr_map.get(day_elem, day_elem)
        daily_elem_kr = elem_kr_map.get(daily_elem, daily_elem)
        positive_reasons.append(f"타고난 {day_elem_kr} 기운이 오늘 일진인 {daily_animal}에 {daily_elem_kr} 기운과 만나 성과가 재물로 이어지기 좋은 날입니다")
    
    # 2. 재성 강함 (일간이 극하는 오행)
    controls = {"목": "토", "화": "금", "토": "수", "금": "목", "수": "화"}
    if controls.get(day_elem) == daily_elem:
        positive_score += 1
        daily_elem_kr = elem_kr_map.get(daily_elem, daily_elem)
        positive_reasons.append(f"오늘 일진인 {daily_animal}에 {daily_elem_kr} 기운이 실리면서 재물로 작용해 수입 흐름이 늘어날 수 있어요")
    
    # 3. 천간생 (일간이 생하는 오행의 오행)
    if generates.get(generates.get(day_elem)) == daily_elem:
        positive_score += 1
        daily_elem_kr = elem_kr_map.get(daily_elem, daily_elem)
        positive_reasons.append(f"오늘 {daily_animal}의 {daily_elem_kr} 기운이 간접적으로 재물 흐름을 돕습니다")
    
    # 4. 비겁탈재 (같은 오행 = 경쟁)
    if day_elem == daily_elem:
        negative_score += 1
        elem_kr = elem_kr_map.get(day_elem, day_elem)
        negative_reasons.append(f"오늘 일진인 {daily_animal}에 {elem_kr} 기운이 타고난 기운과 겹치면서 재물이 분산될 수 있습니다")
    
    # 5. 지지충으로 재물 파괴
    EARTHLY_BRANCH_CLASH = {
        "子": "午", "午": "子", "丑": "未", "未": "丑", "寅": "申", 
        "申": "寅", "卯": "酉", "酉": "卯", "辰": "戌", "戌": "辰", "巳": "亥", "亥": "巳",
    }
    for origin_br in origin_branches:
        if EARTHLY_BRANCH_CLASH.get(daily_branch) == origin_br:
            negative_score += 1
            origin_animal = BRANCH_ANIMAL_KR.get(origin_br, "")
            negative_reasons.append(f"오늘 일진인 {daily_animal}의 기운에 원국 속 {origin_animal} 기운이 충돌하면서 예상치 못한 지출이 생기기 쉽습니다")
            break
    
    # 6. 관성 과다 (일간을 극하는 오행)
    controlled_by = {"목": "금", "화": "수", "토": "목", "금": "화", "수": "토"}
    if controlled_by.get(day_elem) == daily_elem:
        negative_score += 1
        daily_elem_kr = elem_kr_map.get(daily_elem, daily_elem)
        negative_reasons.append(f"오늘 일진인 {daily_animal}에 {daily_elem_kr} 기운이 강하게 눌러오면서 재물 압박을 느낄 수 있습니다")
    
    # 7. 레벨 판정 (연애와 동일 구조)
    if positive_score >= 2 and negative_score == 0:
        level = 2  # 상승
    elif negative_score >= 1 and positive_score == 0:
        level = 0  # 손해
    elif negative_score >= 2:
        level = 0  # 손해
    else:
        level = 1  # 관망
    
    # 마지막 문장 배리에이션
    branch_list = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"]
    b_idx = branch_list.index(daily_branch) if daily_branch in branch_list else 0
    
    money_good_endings = [
        "재테크나 투자 계획을 세우기 좋은 날입니다",
        "평소 눈여겨봤던 재물 기회를 잡아보세요",
        "수입과 관련된 새로운 시도를 해볼 만한 날입니다",
        "재물 흐름이 순조로우니 자신 있게 움직여 보세요",
        "저축이나 자산 관리에 힘을 실어도 좋은 날입니다",
        "기다려온 재물 기회가 열리기 좋은 타이밍입니다",
    ]
    
    money_bad_endings = [
        "큰 지출이나 투자는 미루는 것이 좋습니다",
        "충동적인 소비를 경계하고 지갑을 단단히 하세요",
        "오늘은 돈 관련 결정을 내일로 미루는 것이 현명합니다",
        "예상치 못한 지출에 대비해 여유 자금을 확보해 두세요",
        "재물보다 에너지 관리에 집중하는 것이 나은 날입니다",
        "보수적으로 지출을 관리하고 큰 계약은 피하세요",
    ]
    
    # 8. 메시지 조합
    message_parts = []
    if level == 2:
        if positive_reasons:
            message_parts.extend(positive_reasons)
        else:
            message_parts.append("전반적으로 재물운이 좋은 흐름을 타는 날입니다")
        message_parts.append(money_good_endings[b_idx % len(money_good_endings)])
    elif level == 0:
        if negative_reasons:
            message_parts.extend(negative_reasons)
        else:
            message_parts.append("재물에 긴장감이 있는 날입니다")
        message_parts.append(money_bad_endings[b_idx % len(money_bad_endings)])
    else:
        if positive_reasons and negative_reasons:
            message_parts.extend(positive_reasons[:1])
            message_parts.append("다만, " + negative_reasons[0])
            message_parts.append("신중하게 판단하며 기회를 엿보세요")
        elif positive_reasons:
            message_parts.extend(positive_reasons[:1])
            message_parts.append("작은 재테크부터 시작해보세요")
        elif negative_reasons:
            message_parts.extend(negative_reasons[:1])
            message_parts.append("보수적으로 접근하되 기회를 찾아보세요")
        else:
            message_parts.append("특별한 재물 기운은 없지만 꾸준한 관리가 중요한 시기입니다")
    
    message = " ".join(message_parts) + "."
    return level, message


def calculate_love_calendar(chart: dict, gender: str, start_year: int, num_years: int = 3) -> dict:
    """
    연애운 캘린더 계산 (3개년)
    
    Args:
        chart: /api/saju 응답의 chart 객체
        gender: "male" or "female"
        start_year: 시작 연도
        num_years: 계산할 년수 (기본 3년)
    
    Returns:
        {
            "daily_items": [{"date": "2025-01-01", "level": 2, "message": "..."}],
            "summary": {"충만": 25, "탐색": 322, "경계": 18, "total": 365}
        }
    """
    # 일간/일지 추출
    day_pillar = chart.get("pillars", {}).get("day", {})
    day_stem = day_pillar.get("stem", "")
    day_branch = day_pillar.get("branch", "")
    
    # 원국 지지 4개 추출
    pillars = chart.get("pillars", {})
    origin_branches = []
    for key in ("year", "month", "day", "hour"):
        p = pillars.get(key, {})
        br = p.get("branch", "")
        if br:
            origin_branches.append(br)
    
    # 날짜 범위 생성
    start_date = date(start_year, 1, 1)
    end_date = date(start_year + num_years - 1, 12, 31)
    
    daily_items = []
    summary = {"충만": 0, "탐색": 0, "경계": 0, "total": 0}
    
    current_date = start_date
    while current_date <= end_date:
        # 일진 계산
        daily_pillar = get_day_pillar(current_date)
        daily_stem = daily_pillar.get("stem", "")
        daily_branch = daily_pillar.get("branch", "")
        
        # 레벨 및 메시지 계산
        level, message = calculate_love_day(
            day_stem, day_branch, daily_stem, daily_branch, gender, origin_branches
        )
        
        daily_items.append({
            "date": current_date.strftime("%Y-%m-%d"),
            "level": level,
            "message": message
        })
        
        # 요약 집계
        if level == 2:
            summary["충만"] += 1
        elif level == 1:
            summary["탐색"] += 1
        else:
            summary["경계"] += 1
        summary["total"] += 1
        
        current_date += timedelta(days=1)
    
    return {
        "daily_items": daily_items,
        "summary": summary
    }