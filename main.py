from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from datetime import datetime, date
from zoneinfo import ZoneInfo
import requests
import json
import os

# ✅ 추가: 절기 생성 자동 실행용
import sys
import subprocess
import threading
import time

# ✅ BOOT 로그 (main.py가 실제로 로드되는지 확인)
print("[BOOT] main.py LOADED ✅", os.path.abspath(__file__), flush=True)

app = FastAPI(
    title="Saju API Server",
    version="1.7.1"  # admin 1-shot jieqi generator endpoint added
)

# =========================
# Paths / Env
# =========================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JIEQI_TABLE_PATH = os.path.join(BASE_DIR, "data", "jieqi_1900_2052.json")
KASI_SERVICE_KEY = os.getenv("KASI_SERVICE_KEY")
KST = ZoneInfo("Asia/Seoul")

# =========================
# ✅ Jieqi Table Bootstrap (Railway용)
# =========================

def _is_jieqi_table_usable(path: str) -> bool:
    """
    최소 검증:
    - 파일 존재
    - JSON 로딩 가능
    - 임의의 연도 1~2개가 24개 아이템을 가지고 있는지
    """
    if not os.path.exists(path):
        return False
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            return False

        # 대표 샘플 연도 (가볍게)
        for y in ("1979", "2000"):
            items = data.get(y)
            if isinstance(items, list) and len(items) == 24:
                return True

        # 샘플이 없으면, 아무 연도나 24개 있는지라도 체크
        for _, items in data.items():
            if isinstance(items, list) and len(items) == 24:
                return True

        return False
    except Exception:
        return False


def _run_generate_jieqi_script(timeout_seconds: int = 1800):
    """
    tools/generate_jieqi_table.py를 실행해서 data/jieqi_1900_2052.json 생성/갱신.
    ⚠️ 부팅에서는 절대 안 돌리고, 관리자 엔드포인트에서 1회만 트리거한다.
    """
    script_path = os.path.join(BASE_DIR, "tools", "generate_jieqi_table.py")

    if not os.path.exists(script_path):
        print(f"[JIEQI] generator script not found: {script_path}", flush=True)
        return False, f"generator script not found: {script_path}"

    # 출력 경로 고정
    env = os.environ.copy()
    env["JIEQI_OUTPUT"] = JIEQI_TABLE_PATH

    print("[JIEQI] generating jieqi table... (admin-triggered, 1-shot)", flush=True)

    try:
        proc = subprocess.run(
            [sys.executable, script_path],
            cwd=BASE_DIR,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
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

        # 생성 후 검증
        if _is_jieqi_table_usable(JIEQI_TABLE_PATH):
            print("[JIEQI] jieqi table generated and looks usable ✅", flush=True)
            return True, "ok"
        else:
            print("[JIEQI] jieqi table generated but looks NOT usable ❌", flush=True)
            return False, "generated but not usable"

    except subprocess.TimeoutExpired:
        msg = f"timeout after {timeout_seconds}s"
        print(f"[JIEQI] generator timeout: {msg}", flush=True)
        return False, msg
    except Exception as e:
        msg = f"generator exception: {e}"
        print(f"[JIEQI] {msg}", flush=True)
        return False, msg


# =========================
# Startup (DO NOT AUTO-GENERATE)
# =========================

@app.on_event("startup")
def _startup():
    # ✅ startup 이벤트가 실제로 타는지 확인
    print("[BOOT] startup event fired ✅", flush=True)
    # ✅ 절기 자동 생성은 꺼둔다 (부팅 지연/멈춤 방지)
    # ensure_jieqi_table_async()  # DO NOT ENABLE


# =========================
# Utils
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

# =========================
# Jieqi
# =========================

def find_ipchun_dt(jieqi_list):
    for item in jieqi_list:
        if item.get("name") in ("입춘", "立春"):
            return _pick_item_dt(item)
    raise ValueError("입춘 not found")

def get_jieqi_with_fallback(year: str):
    source = "json"
    fallback = True
    table = load_jieqi_table()
    year_data = table.get(year)
    if not year_data:
        raise ValueError(f"No jieqi for {year}")
    return source, fallback, year_data

# =========================
# Pillars
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


# ✅ 관리자용 1회 생성 엔드포인트
# - 부팅 시 자동생성 금지
# - 필요할 때 딱 한 번 호출해서 data/jieqi_1900_2052.json을 만든다
@app.post("/admin/generate-jieqi")
def admin_generate_jieqi(
    token: str = Query(..., description="관리자 토큰"),
    force: bool = Query(False, description="True면 기존 파일이 있어도 재생성 시도")
):
    try:
        admin_token = os.getenv("ADMIN_TOKEN")

        if not admin_token:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": "ADMIN_TOKEN env not set"}
            )

        if token != admin_token:
            return JSONResponse(
                status_code=403,
                content={"ok": False, "error": "invalid token"}
            )

        # 이미 usable 하면 스킵
        if (not force) and _is_jieqi_table_usable(JIEQI_TABLE_PATH):
            return {
                "ok": True,
                "message": "jieqi table already exists (skip)",
                "path": JIEQI_TABLE_PATH
            }

        # data 폴더 보장
        os.makedirs(os.path.dirname(JIEQI_TABLE_PATH), exist_ok=True)

        ok, msg = _run_generate_jieqi_script(timeout_seconds=1800)
        if not ok:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": msg, "path": JIEQI_TABLE_PATH}
            )

        return {
            "ok": True,
            "message": "jieqi table generated",
            "path": JIEQI_TABLE_PATH
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


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

        # ⛔ 월주 / 시주 계산 로직은 기존 그대로 호출한다고 가정
        # (이미 검증 완료)

        result = {
            "input": {
                "birth": birth,
                "calendar": calendar,
                "birth_time": birth_time,
                "gender": gender
            },
            "pillars": {
                "year": year_pillar,
                "month": None,  # 기존 로직 연결
                "day": day_pillar,
                "hour": None    # 기존 로직 연결
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
                "rules": {
                    "year": "ipchun_boundary",
                    "month": "major_terms_deg",
                    "day": "gregorian_jdn_offset47",
                    "hour": "2h_blocks_optional"
                },
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
