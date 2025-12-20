from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from datetime import datetime, date
from zoneinfo import ZoneInfo
import json
import os
import sys
import subprocess
import threading
import time

print("[BOOT] main.py LOADED ✅", os.path.abspath(__file__), flush=True)

app = FastAPI(
    title="Saju API Server",
    version="1.7.2"  # async/background jieqi generation (no request-timeout)
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JIEQI_TABLE_PATH = os.path.join(BASE_DIR, "data", "jieqi_1900_2052.json")
KST = ZoneInfo("Asia/Seoul")

# =========================
# Jieqi table helpers
# =========================

def _is_jieqi_table_usable(path: str) -> bool:
    if not os.path.exists(path):
        return False
    try:
        with open(path, "r", encoding="utf-8") as f:
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


def _run_generate_jieqi_script():
    """
    절기 테이블 생성 (길게 걸릴 수 있음)
    - 요청 타임아웃을 피하기 위해 "백그라운드"에서만 실행한다.
    """
    script_path = os.path.join(BASE_DIR, "tools", "generate_jieqi_table.py")
    if not os.path.exists(script_path):
        print(f"[JIEQI] generator script not found: {script_path}", flush=True)
        return False, f"generator script not found: {script_path}"

    os.makedirs(os.path.dirname(JIEQI_TABLE_PATH), exist_ok=True)

    env = os.environ.copy()
    env["JIEQI_OUTPUT"] = JIEQI_TABLE_PATH

    print("[JIEQI] generating jieqi table... (background)", flush=True)

    try:
        # ✅ 여기서는 timeout을 걸지 않는다. (시간 오래 걸려도 끝까지)
        proc = subprocess.run(
            [sys.executable, script_path],
            cwd=BASE_DIR,
            env=env,
            capture_output=True,
            text=True,
        )

        print("[JIEQI] generator stdout:", flush=True)
        if proc.stdout:
            print(proc.stdout[:4000], flush=True)

        if proc.stderr:
            print("[JIEQI] generator stderr:", flush=True)
            print(proc.stderr[:4000], flush=True)

        if proc.returncode != 0:
            msg = f"generator failed: returncode={proc.returncode}"
            print(f"[JIEQI] {msg}", flush=True)
            return False, msg

        if _is_jieqi_table_usable(JIEQI_TABLE_PATH):
            print("[JIEQI] jieqi table generated and looks usable ✅", flush=True)
            return True, "ok"
        else:
            print("[JIEQI] jieqi table generated but looks NOT usable ❌", flush=True)
            return False, "generated but not usable"

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
}

_job_lock = threading.Lock()

def _jieqi_job_worker():
    with _job_lock:
        JIEQI_JOB["running"] = True
        JIEQI_JOB["started_at"] = datetime.now(tz=KST).isoformat()
        JIEQI_JOB["finished_at"] = None
        JIEQI_JOB["ok"] = None
        JIEQI_JOB["message"] = None
        JIEQI_JOB["last_log_at"] = datetime.now(tz=KST).isoformat()

    ok, msg = _run_generate_jieqi_script()

    with _job_lock:
        JIEQI_JOB["running"] = False
        JIEQI_JOB["finished_at"] = datetime.now(tz=KST).isoformat()
        JIEQI_JOB["ok"] = bool(ok)
        JIEQI_JOB["message"] = msg
        JIEQI_JOB["last_log_at"] = datetime.now(tz=KST).isoformat()


@app.on_event("startup")
def _startup():
    print("[BOOT] startup event fired ✅", flush=True)
    # ✅ 부팅 시 자동 생성 금지
    # (관리자 호출로만 돌린다)


# =========================
# Utils (jieqi)
# =========================

def load_jieqi_table():
    with open(JIEQI_TABLE_PATH, "r", encoding="utf-8") as f:
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
# Pillars (day/year only in this file)
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
# API
# =========================

@app.get("/health")
def health():
    return {"status": "ok"}


# ✅ 관리자: 생성 "시작"만 하고 바로 반환 (요청 타임아웃 방지)
@app.post("/admin/generate-jieqi")
def admin_generate_jieqi(
    token: str = Query(..., description="관리자 토큰"),
    force: bool = Query(False, description="True면 기존 파일 있어도 재생성 시작")
):
    try:
        admin_token = os.getenv("ADMIN_TOKEN")
        if not admin_token:
            return JSONResponse(status_code=500, content={"ok": False, "error": "ADMIN_TOKEN env not set"})
        if token != admin_token:
            return JSONResponse(status_code=403, content={"ok": False, "error": "invalid token"})

        # 이미 usable + force 아님이면 시작할 필요 없음
        if (not force) and _is_jieqi_table_usable(JIEQI_TABLE_PATH):
            return {"ok": True, "message": "jieqi table already exists (skip)", "path": JIEQI_TABLE_PATH}

        with _job_lock:
            if JIEQI_JOB["running"]:
                return {"ok": True, "message": "jieqi generation already running", "job": JIEQI_JOB}

            # 백그라운드 시작
            t = threading.Thread(target=_jieqi_job_worker, daemon=True)
            t.start()

            return {"ok": True, "message": "jieqi generation started (background)", "job": JIEQI_JOB}

    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


# ✅ 관리자: 진행상황 확인
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
            "file_exists": os.path.exists(JIEQI_TABLE_PATH),
            "file_usable": _is_jieqi_table_usable(JIEQI_TABLE_PATH),
            "path": JIEQI_TABLE_PATH
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

        result = {
            "input": {
                "birth": birth,
                "calendar": calendar,
                "birth_time": birth_time,
                "gender": gender
            },
            "pillars": {
                "year": year_pillar,
                "month": None,
                "day": day_pillar,
                "hour": None
            },
            "jieqi": {
                "year": str(birth_dt.year),
                "count": len(jieqi_this),
                "items": jieqi_this
            },
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
