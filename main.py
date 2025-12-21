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
    version="1.7.7"  # month boundary -> next-day midnight (match common almanac), keep hour/month pillars
)

# ==================================================
# PATHS (Railway/uvicorn cwd 흔들림 방지)
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
# Jieqi table helpers
# =========================

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


def _run_generate_jieqi_script(start_year: int, end_year: int):
    """
    절기 테이블 생성 (길게 걸릴 수 있음)
    - 요청 타임아웃을 피하기 위해 "백그라운드"에서만 실행한다.
    - ✅ start/end 범위를 env로 넘겨서 "쪼개기" 실행 가능
    - ✅ generate_jieqi_table.py는 매년 저장(append)하므로 중간에 죽어도 누적됨
    """
    script_path = TOOLS_DIR / "generate_jieqi_table.py"
    if not script_path.exists():
        print(f"[JIEQI] generator script not found: {script_path}", flush=True)
        return False, f"generator script not found: {script_path}"

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["JIEQI_OUTPUT"] = str(JIEQI_TABLE_PATH)
    env["JIEQI_APPEND"] = "true"
    env["JIEQI_START_YEAR"] = str(start_year)
    env["JIEQI_END_YEAR"] = str(end_year)

    print(f"[JIEQI] generating jieqi table... (background) range={start_year}..{end_year}", flush=True)

    try:
        proc = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(PROJECT_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )

        if proc.stdout:
            print("[JIEQI] generator stdout (head):", flush=True)
            print(proc.stdout[:4000], flush=True)
        if proc.stderr:
            print("[JIEQI] generator stderr (head):", flush=True)
            print(proc.stderr[:4000], flush=True)

        if proc.returncode != 0:
            msg = f"generator failed: returncode={proc.returncode}"
            print(f"[JIEQI] {msg}", flush=True)
            return False, msg

        if _is_jieqi_table_usable(JIEQI_TABLE_PATH):
            print("[JIEQI] jieqi table generated and looks usable ✅", flush=True)
        else:
            print("[JIEQI] generation finished but file not yet 'usable' (maybe partial) ⏳", flush=True)

        return True, "ok"

    except Exception as e:
        msg = f"generator exception: {e}"
        print(f"[JIEQI] {msg}", flush=True)
        return False, msg


# =========================
# Background job state
# =========================

JIEQI_JOB = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "ok": None,
    "message": None,
    "last_log_at": None,
    "range": None,
}

_job_lock = threading.Lock()

def _jieqi_job_worker(start_year: int, end_year: int):
    with _job_lock:
        JIEQI_JOB["running"] = True
        JIEQI_JOB["started_at"] = datetime.now(tz=KST).isoformat()
        JIEQI_JOB["finished_at"] = None
        JIEQI_JOB["ok"] = None
        JIEQI_JOB["message"] = None
        JIEQI_JOB["last_log_at"] = datetime.now(tz=KST).isoformat()
        JIEQI_JOB["range"] = {"start": start_year, "end": end_year}

    ok, msg = _run_generate_jieqi_script(start_year, end_year)

    with _job_lock:
        JIEQI_JOB["running"] = False
        JIEQI_JOB["finished_at"] = datetime.now(tz=KST).isoformat()
        JIEQI_JOB["ok"] = bool(ok)
        JIEQI_JOB["message"] = msg
        JIEQI_JOB["last_log_at"] = datetime.now(tz=KST).isoformat()


@app.on_event("startup")
def _startup():
    print("[BOOT] startup event fired ✅", flush=True)
    print(f"[JIEQI] path={JIEQI_TABLE_PATH} exists={JIEQI_TABLE_PATH.exists()} cwd={Path.cwd()}", flush=True)

    if JIEQI_TABLE_PATH.exists():
        try:
            d = load_jieqi_table()
            print(f"[JIEQI] loaded OK ✅ years={len(d)} 1979_count={len(d.get('1979', []))}", flush=True)
        except Exception as e:
            print(f"[JIEQI] load failed ❌ {e}", flush=True)

    # ✅ 부팅 시 자동 생성 금지


# =========================
# Utils (jieqi)
# =========================

def load_jieqi_table():
    if not JIEQI_TABLE_PATH.exists():
        raise FileNotFoundError(f"[JIEQI] missing file: {JIEQI_TABLE_PATH} (cwd={Path.cwd()})")
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


# =========================
# Pillars (day/year)
# =========================

STEMS = ["甲","乙","丙","丁","戊","己","庚","辛","壬","癸"]
BRANCHES = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"]

def gregorian_to_jdn(y, m, d):
    a = (14 - m) // 12
    y2 = y + 4800 - a
    m2 = m + 12 * a - 3
    return d + (153*m2+2)//5 + 365*y2 + y2//4 - y2//100 + y2//400 - 32045

def get_day_pillar(dt: date):
    idx = (gregorian_to_jdn(dt.year, dt.month, dt.day) + 47) % 60
    return {"stem": STEMS[idx % 10], "branch": BRANCHES[idx % 12], "ganji": STEMS[idx % 10] + BRANCHES[idx % 12], "index60": idx}

def get_year_pillar(year: int):
    idx = (year - 1984) % 60
    return {"stem": STEMS[idx % 10], "branch": BRANCHES[idx % 12], "ganji": STEMS[idx % 10] + BRANCHES[idx % 12], "index60": idx}


# =========================
# Pillars (month) - 절기(12절) 기반 + "점신식" 날짜 경계 보정
# =========================

MONTH_TERM_TO_BRANCH = [
    ("입춘", "寅"),
    ("경칩", "卯"),
    ("청명", "辰"),
    ("입하", "巳"),
    ("망종", "午"),
    ("소서", "未"),
    ("입추", "申"),
    ("백로", "酉"),
    ("한로", "戌"),
    ("입동", "亥"),
    ("대설", "子"),
    ("소한", "丑"),
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
        if not name:
            continue
        dt = _pick_item_dt(item)
        if not dt:
            continue
        m[name] = dt
    return m

def _get_month_branch_from_terms(birth_dt: datetime, this_year_terms: dict, prev_year_terms: dict):
    """
    ✅ 점신/시중 만세력 기준에 맞춘 월주 경계:
    - 절기 '시각' 기준이 아니라 '날짜' 기준으로 월주 전환
    - 즉, 절기 발생 '당일'은 이전 월로 유지
    - 절기 다음날 00:00(KST)부터 다음 월로 전환
    """
    def boundary_next_midnight(dt: datetime) -> datetime:
        d = dt.astimezone(KST).date() + timedelta(days=1)
        return datetime(d.year, d.month, d.day, 0, 0, tzinfo=KST)

    candidates = []

    for term, branch in MONTH_TERM_TO_BRANCH:
        dt = this_year_terms.get(term)
        if dt:
            candidates.append((boundary_next_midnight(dt), branch, term))

    # 1월 초(소한 이전) 케이스를 위해 전년도 대설(子)도 포함
    prev_daeseol = prev_year_terms.get("대설")
    if prev_daeseol:
        candidates.append((boundary_next_midnight(prev_daeseol), "子", "대설(prev)"))

    valid = [c for c in candidates if c[0] <= birth_dt]
    if not valid:
        return "丑"

    valid.sort(key=lambda x: x[0])
    return valid[-1][1]

def get_month_pillar(birth_dt: datetime, saju_year_pillar: dict, jieqi_this_year: list, jieqi_prev_year: list):
    this_map = _jieqi_term_dt_map(jieqi_this_year)
    prev_map = _jieqi_term_dt_map(jieqi_prev_year)

    month_branch = _get_month_branch_from_terms(birth_dt, this_map, prev_map)

    year_stem = saju_year_pillar["stem"]
    yin_month_stem = YEAR_STEM_TO_YIN_MONTH_STEM.get(year_stem)
    if not yin_month_stem:
        raise ValueError(f"Invalid year stem for month pillar: {year_stem}")

    month_index = MONTH_BRANCH_SEQ.index(month_branch)
    stem_index = (STEMS.index(yin_month_stem) + month_index) % 10
    month_stem = STEMS[stem_index]

    return {"stem": month_stem, "branch": month_branch, "ganji": month_stem + month_branch}


# =========================
# Pillars (hour) - 표준 시주(12지) 기반
# =========================

HOUR_BRANCH_SEQ = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"]

DAY_STEM_TO_ZI_HOUR_STEM = {
    "甲": "甲", "己": "甲",
    "乙": "丙", "庚": "丙",
    "丙": "戊", "辛": "戊",
    "丁": "庚", "壬": "庚",
    "戊": "壬", "癸": "壬",
}

def _get_hour_branch(hh: int, mm: int) -> str:
    total = hh * 60 + mm
    shifted = (total - 23 * 60) % (24 * 60)  # 23:00을 기준으로 0분
    idx = shifted // 120                      # 2시간 단위
    return HOUR_BRANCH_SEQ[int(idx)]

def get_hour_pillar(day_pillar: dict, hh: int, mm: int):
    hour_branch = _get_hour_branch(hh, mm)

    day_stem = day_pillar["stem"]
    zi_hour_stem = DAY_STEM_TO_ZI_HOUR_STEM.get(day_stem)
    if not zi_hour_stem:
        raise ValueError(f"Invalid day stem for hour pillar: {day_stem}")

    hour_index = HOUR_BRANCH_SEQ.index(hour_branch)        # 子=0 ... 亥=11
    stem_index = (STEMS.index(zi_hour_stem) + hour_index) % 10
    hour_stem = STEMS[stem_index]

    return {"stem": hour_stem, "branch": hour_branch, "ganji": hour_stem + hour_branch}


# =========================
# API
# =========================

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/admin/generate-jieqi")
def admin_generate_jieqi(
    token: str = Query(..., description="관리자 토큰"),
    force: bool = Query(False, description="True면 기존 파일 있어도 재생성 시작"),
    start_year: int = Query(1900, description="생성 시작 연도"),
    end_year: int = Query(2052, description="생성 종료 연도"),
):
    try:
        admin_token = os.getenv("ADMIN_TOKEN")
        if not admin_token:
            return JSONResponse(status_code=500, content={"ok": False, "error": "ADMIN_TOKEN env not set"})
        if token != admin_token:
            return JSONResponse(status_code=403, content={"ok": False, "error": "invalid token"})

        if start_year > end_year:
            return JSONResponse(status_code=400, content={"ok": False, "error": "start_year must be <= end_year"})

        if (not force) and _is_jieqi_table_usable(JIEQI_TABLE_PATH):
            return {"ok": True, "message": "jieqi table already exists (skip)", "path": str(JIEQI_TABLE_PATH)}

        with _job_lock:
            if JIEQI_JOB["running"]:
                return {"ok": True, "message": "jieqi generation already running", "job": JIEQI_JOB}

            t = threading.Thread(target=_jieqi_job_worker, args=(start_year, end_year), daemon=True)
            t.start()

            return {
                "ok": True,
                "message": f"jieqi generation started (background) range={start_year}..{end_year}",
                "job": JIEQI_JOB
            }

    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.get("/admin/jieqi-status")
def admin_jieqi_status(token: str = Query(..., description="관리자 토큰")):
    admin_token = os.getenv("ADMIN_TOKEN")
    if not admin_token:
        return JSONResponse(status_code=500, content={"ok": False, "error": "ADMIN_TOKEN env not set"})
    if token != admin_token:
        return JSONResponse(status_code=403, content={"ok": False, "error": "invalid token"})

    with _job_lock:
        return {
            "ok": True,
            "job": JIEQI_JOB,
            "file_exists": JIEQI_TABLE_PATH.exists(),
            "file_usable": _is_jieqi_table_usable(JIEQI_TABLE_PATH),
            "path": str(JIEQI_TABLE_PATH)
        }


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

        # ✅ 월주 (점신식 날짜 경계 보정 적용)
        _, _, jieqi_prev = get_jieqi_with_fallback(str(birth_dt.year - 1))
        month_pillar = get_month_pillar(birth_dt, year_pillar, jieqi_this, jieqi_prev)

        # ✅ 시주 (시간 입력이 있을 때만)
        hour_pillar = get_hour_pillar(day_pillar, hh, mm) if time_applied else None

        result = {
            "input": {"birth": birth, "calendar": calendar, "birth_time": birth_time, "gender": gender},
            "pillars": {"year": year_pillar, "month": month_pillar, "day": day_pillar, "hour": hour_pillar},
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
        }
        return result

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
