import os
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import xml.etree.ElementTree as ET
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# =========================
# Config
# =========================
KST = dt.timezone(dt.timedelta(hours=9))

KASI_LUNAR_BASE = "https://apis.data.go.kr/B090041/openapi/service/LrsrCldInfoService"
KASI_SPCDE_BASE = "https://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService"

# 우리가 만든 절기 테이블(JSON)
JIEQI_JSON_PATH = Path("data/jieqi_1900_2052.json")

app = FastAPI(title="Saju API Server", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

JIEQI_TABLE: Dict[str, List[Dict[str, Any]]] = {}  # {"1979": [ ... ]}

# =========================
# Helpers
# =========================
def _get_env_key() -> str:
    key = os.getenv("KASI_SERVICE_KEY", "").strip()
    if not key:
        raise HTTPException(status_code=500, detail="Missing env var: KASI_SERVICE_KEY")
    return key

def _parse_date_yyyy_mm_dd(s: str) -> dt.date:
    try:
        y, m, d = s.split("-")
        return dt.date(int(y), int(m), int(d))
    except Exception:
        raise HTTPException(status_code=400, detail="birth must be YYYY-MM-DD")

def _prev_month(year: int, month: int) -> Tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1

def _xml_items(xml_text: str) -> List[ET.Element]:
    root = ET.fromstring(xml_text)
    body = root.find("body")
    if body is None:
        return []
    items = body.find("items")
    if items is None:
        return []
    return list(items.findall("item"))

def _xml_text(elem: Optional[ET.Element], tag: str) -> Optional[str]:
    if elem is None:
        return None
    t = elem.find(tag)
    return t.text.strip() if (t is not None and t.text) else None

def _xml_int(root: ET.Element, path: str, default: int = 0) -> int:
    node = root.find(path)
    if node is None or node.text is None:
        return default
    try:
        return int(node.text.strip())
    except Exception:
        return default

def _safe_request(url: str, params: Dict[str, Any], timeout: int = 15) -> str:
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.text

def _locdate_kst_to_dt(locdate: str, kst_hhmm: str) -> dt.datetime:
    # locdate: "YYYYMMDD", kst: "HHMM"
    y = int(locdate[0:4]); m = int(locdate[4:6]); d = int(locdate[6:8])
    hh = int(kst_hhmm[0:2]); mm = int(kst_hhmm[2:4])
    return dt.datetime(y, m, d, hh, mm, tzinfo=KST)

# =========================
# Startup: load table
# =========================
@app.on_event("startup")
def load_jieqi_table() -> None:
    global JIEQI_TABLE
    if JIEQI_JSON_PATH.exists():
        try:
            JIEQI_TABLE = json.loads(JIEQI_JSON_PATH.read_text(encoding="utf-8"))
        except Exception:
            JIEQI_TABLE = {}
    else:
        JIEQI_TABLE = {}

# =========================
# KASI: lunar conversion
# =========================
def fetch_lunar_info(sol_date: dt.date) -> Dict[str, Any]:
    service_key = _get_env_key()
    url = f"{KASI_LUNAR_BASE}/getLunCalInfo"

    params = {
        "serviceKey": service_key,
        "solYear": f"{sol_date.year:04d}",
        "solMonth": f"{sol_date.month:02d}",
        "solDay": f"{sol_date.day:02d}",
        "numOfRows": 10,
        "pageNo": 1,
    }

    xml_text = _safe_request(url, params)
    root = ET.fromstring(xml_text)

    result_code = _xml_text(root.find("header"), "resultCode") or ""
    result_msg = _xml_text(root.find("header"), "resultMsg") or ""
    total_count = _xml_int(root, "body/totalCount", 0)

    items = _xml_items(xml_text)
    if result_code != "00" or total_count <= 0 or not items:
        return {
            "result": {"resultCode": result_code, "resultMsg": result_msg, "totalCount": total_count},
            "rawXml": xml_text,
        }

    item = items[0]
    lun_year = _xml_text(item, "lunYear")
    lun_month = _xml_text(item, "lunMonth")
    lun_day = _xml_text(item, "lunDay")
    lun_leap = _xml_text(item, "lunLeapmonth")  # "윤" or "평"
    is_leap = (lun_leap == "윤")

    lunar_label = f"{'윤달 ' if is_leap else ''}{int(lun_month):02d}월 {int(lun_day):02d}일"

    # 간지 raw fields는 API에 따라 비는 경우가 있어 보존만
    ganji_year = _xml_text(item, "year")
    ganji_month = _xml_text(item, "month")
    ganji_day = _xml_text(item, "day")

    return {
        "result": {"resultCode": result_code, "resultMsg": result_msg, "totalCount": total_count, "pageNo": 1, "numOfRows": 10},
        "solar": {"year": f"{sol_date.year:04d}", "month": f"{sol_date.month:02d}", "day": f"{sol_date.day:02d}"},
        "lunar": {
            "lunYear": lun_year,
            "lunMonth": lun_month,
            "lunDay": lun_day,
            "isLeap": is_leap,
            "lunLeapmonth": lun_leap,
            "lunarLabel": lunar_label,
        },
        "ganji": {"rawGanji": {"year": ganji_year, "month": ganji_month, "day": ganji_day}},
    }

# =========================
# KASI: 24 jieqi (try)
# =========================
def fetch_jieqi_kasi(year: int, month: int) -> List[Dict[str, Any]]:
    """
    KASI get24DivisionsInfo는 연도/월 파라미터를 요구할 가능성이 높아서
    solYear + solMonth로 조회한다.
    """
    service_key = _get_env_key()
    url = f"{KASI_SPCDE_BASE}/get24DivisionsInfo"

    params = {
        "serviceKey": service_key,
        "solYear": f"{year:04d}",
        "solMonth": f"{month:02d}",
        "numOfRows": 50,
        "pageNo": 1,
    }

    xml_text = _safe_request(url, params)
    root = ET.fromstring(xml_text)
    result_code = _xml_text(root.find("header"), "resultCode") or ""
    total_count = _xml_int(root, "body/totalCount", 0)

    if result_code != "00" or total_count <= 0:
        return []

    items = _xml_items(xml_text)
    out: List[Dict[str, Any]] = []
    for it in items:
        date_name = _xml_text(it, "dateName")
        locdate = _xml_text(it, "locdate")
        kst = (_xml_text(it, "kst") or "").strip()
        sun_long = _xml_text(it, "sunLongitude")

        if not (date_name and locdate):
            continue

        # kst가 "0909 "처럼 공백 포함하는 케이스가 있어 정리
        kst = kst.replace(" ", "")
        if len(kst) != 4 or not kst.isdigit():
            # 시간 없으면 0000 처리
            kst = "0000"

        out.append({
            "dateName": date_name,
            "locdate": locdate,
            "kst": kst,
            "sunLongitude": int(sun_long) if (sun_long and sun_long.isdigit()) else None,
            "source": "kasi",
        })
    return out

# =========================
# Table fallback
# =========================
def fetch_jieqi_table(year: int) -> List[Dict[str, Any]]:
    return JIEQI_TABLE.get(str(year), [])

def build_jieqi_list(solar_date: dt.date) -> Tuple[List[Dict[str, Any]], str]:
    """
    1) KASI (전월+당월) 시도
    2) 비면 JSON 테이블(해당연도+전년도)로 fallback
    """
    y, m = solar_date.year, solar_date.month
    py, pm = _prev_month(y, m)

    # KASI 시도 (데이터가 나오는 연도만)
    try:
        a = fetch_jieqi_kasi(py, pm)
        b = fetch_jieqi_kasi(y, m)
        kasi = a + b
        if kasi:
            # locdate+kst 기준 정렬
            kasi.sort(key=lambda x: (x.get("locdate", ""), x.get("kst", "0000")))
            return kasi, "kasi"
    except Exception:
        pass

    # Fallback: 테이블 (해당연도+전년도 포함해서 prevJieQi 잡기)
    tbl = []
    prev_year = y - 1
    tbl.extend(fetch_jieqi_table(prev_year))
    tbl.extend(fetch_jieqi_table(y))

    # source 표시
    for it in tbl:
        it["source"] = "table"

    tbl.sort(key=lambda x: (x.get("locdate", ""), x.get("kst", "0000")))
    return tbl, "table"

def compute_prev_jieqi(jieqi_list: List[Dict[str, Any]], solar_date: dt.date) -> Optional[Dict[str, Any]]:
    """
    출생일 기준 '직전 절기'를 계산.
    지금 단계에서는 출생시간이 없으니 '출생일 00:00(KST)' 기준으로 잡는다.
    """
    if not jieqi_list:
        return None

    birth_dt = dt.datetime(solar_date.year, solar_date.month, solar_date.day, 0, 0, tzinfo=KST)

    prev = None
    for it in jieqi_list:
        locdate = it.get("locdate")
        kst = (it.get("kst") or "0000").replace(" ", "")
        if not locdate or len(locdate) != 8:
            continue
        if len(kst) != 4 or not kst.isdigit():
            kst = "0000"

        try:
            t = _locdate_kst_to_dt(locdate, kst)
        except Exception:
            continue

        if t <= birth_dt:
            prev = it
        else:
            break

    return prev

# =========================
# Endpoints
# =========================
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/api/saju/calc")
def calc_saju(
    birth: str = Query(..., description="YYYY-MM-DD"),
    calendar: str = Query("solar", description="solar or lunar (currently solar recommended)"),
):
    solar_date = _parse_date_yyyy_mm_dd(birth)

    # 1) Lunar conversion (KASI)
    lunar_info = fetch_lunar_info(solar_date)

    # 2) Jieqi list: KASI or table
    jieqi_list, jieqi_source = build_jieqi_list(solar_date)

    # 3) prevJieQi
    prev_jieqi = compute_prev_jieqi(jieqi_list, solar_date)

    resp = {
        "input": {"birth": birth, "calendar": calendar},
        "solarDate": solar_date.strftime("%Y-%m-%d"),
        "lunarInfo": lunar_info,
        "jieqiSource": jieqi_source,
        "jieqiList": jieqi_list,
        "prevJieQi": prev_jieqi,
    }

    # 경고: 테이블이 로드가 안 됐다면 알려주기
    if jieqi_source == "table" and not JIEQI_TABLE:
        resp["warning"] = "JIEQI table not loaded. Ensure data/jieqi_1900_2052.json exists in repo and deployed."

    # 경고: 해당 연도 테이블이 없을 때
    if jieqi_source == "table" and JIEQI_TABLE and (str(solar_date.year) not in JIEQI_TABLE):
        resp["warning"] = f"No table data for year={solar_date.year}. Table range is 1900~2052."

    return resp
