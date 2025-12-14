import os
import re
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from xml.etree import ElementTree as ET

# =========================
# Config
# =========================
KASI_SERVICE_KEY = os.getenv("KASI_SERVICE_KEY", "").strip()

KASI_LUNAR_BASE = "https://apis.data.go.kr/B090041/openapi/service/LrsrCldInfoService"
KASI_SPCDE_BASE = "https://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService"

DEFAULT_TIMEOUT = 12


# =========================
# Helpers
# =========================
def _yyyymmdd(date_obj: dt.date) -> str:
    return date_obj.strftime("%Y%m%d")


def _ym(date_obj: dt.date) -> Tuple[str, str]:
    return date_obj.strftime("%Y"), date_obj.strftime("%m")  # month: 2-digit


def _prev_month(date_obj: dt.date) -> dt.date:
    """
    Return a date in the previous month.
    If the same day doesn't exist (e.g., Mar 31 -> Feb 28/29),
    clamp to the last day of the previous month.
    """
    first_this_month = date_obj.replace(day=1)
    last_prev_month = first_this_month - dt.timedelta(days=1)
    day = min(date_obj.day, last_prev_month.day)
    return last_prev_month.replace(day=day)


def _parse_xml_items(xml_text: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Returns: (meta, items)
    meta includes resultCode/resultMsg/totalCount/pageNo/numOfRows
    items is list of <item> as dict(tag->text)
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        raise HTTPException(status_code=502, detail=f"KASI XML parse error: {e}")

    def find_text(path: str) -> str:
        el = root.find(path)
        return (el.text or "").strip() if el is not None else ""

    result_code = find_text("./header/resultCode")
    result_msg = find_text("./header/resultMsg")
    total_count = find_text("./body/totalCount")
    page_no = find_text("./body/pageNo")
    num_rows = find_text("./body/numOfRows")

    items: List[Dict[str, Any]] = []
    for item_el in root.findall("./body/items/item"):
        d: Dict[str, Any] = {}
        for child in list(item_el):
            d[child.tag] = (child.text or "").strip()
        items.append(d)

    meta = {
        "resultCode": result_code,
        "resultMsg": result_msg,
        "totalCount": int(total_count) if str(total_count).isdigit() else total_count,
        "pageNo": int(page_no) if str(page_no).isdigit() else page_no,
        "numOfRows": int(num_rows) if str(num_rows).isdigit() else num_rows,
    }
    return meta, items


def _kasi_get(url: str, params: Dict[str, Any]) -> Tuple[str, Dict[str, Any], List[Dict[str, Any]]]:
    """
    KASI 호출 헬퍼.
    - ServiceKey(대문자)로 고정
    - resultCode != 00 이면 502로 에러 처리
    """
    if not KASI_SERVICE_KEY:
        raise HTTPException(status_code=500, detail="KASI_SERVICE_KEY is not set in environment variables")

    params = dict(params)
    params["ServiceKey"] = KASI_SERVICE_KEY

    try:
        r = requests.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"KASI request error: {e}")

    xml_text = r.text
    meta, items = _parse_xml_items(xml_text)

    if meta.get("resultCode") and meta["resultCode"] != "00":
        raise HTTPException(
            status_code=502,
            detail={
                "message": "KASI returned error",
                "resultCode": meta.get("resultCode"),
                "resultMsg": meta.get("resultMsg"),
                "url": url,
                "params": {k: ("***" if k.lower() == "servicekey" else v) for k, v in params.items()},
                "raw": xml_text[:1200],
            },
        )

    return xml_text, meta, items


def _normalize_calendar(value: str) -> str:
    v = (value or "").strip().lower()
    if v in ("solar", "양력", "gregorian"):
        return "solar"
    if v in ("lunar", "음력"):
        return "lunar"
    return v


def _parse_birth(birth: str) -> dt.date:
    s = (birth or "").strip()
    if re.fullmatch(r"\d{8}", s):
        return dt.datetime.strptime(s, "%Y%m%d").date()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    raise HTTPException(status_code=400, detail="birth must be YYYY-MM-DD or YYYYMMDD")


def _dedupe_sort_jieqi(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplicate by (locdate, dateName) and sort by locdate ascending.
    Expected fields: locdate(YYYYMMDD), dateName, isHoliday, dateKind, seq
    """
    seen = set()
    out = []
    for it in items:
        loc = (it.get("locdate") or "").strip()
        name = (it.get("dateName") or "").strip()
        key = (loc, name)
        if loc and name and key not in seen:
            seen.add(key)
            out.append(it)

    out.sort(key=lambda x: x.get("locdate", "00000000"))
    return out


def _find_prev_jieqi(jieqi_list: List[Dict[str, Any]], birth_date: dt.date) -> Optional[Dict[str, Any]]:
    b = _yyyymmdd(birth_date)
    prev = None
    for it in jieqi_list:
        loc = it.get("locdate", "")
        if loc and loc <= b:
            prev = it
        elif loc and loc > b:
            break
    return prev


def _safe_month_query_dates(birth_date: dt.date) -> List[dt.date]:
    """
    Always query birth month.
    Also query previous month (to guarantee prevJieQi for month-start births).
    """
    return [_prev_month(birth_date), birth_date]


# =========================
# FastAPI App
# =========================
app = FastAPI(title="Saju API Server", version="1.0.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"ok": True, "service": "saju-api-server", "version": "1.0.1"}


@app.get("/health")
def health():
    return {"ok": True}


# =========================
# Core Endpoint
# =========================
@app.get("/api/saju/calc")
def calc_saju(
    birth: str = Query(..., description="YYYY-MM-DD or YYYYMMDD"),
    calendar: str = Query("solar", description="solar|lunar"),
    debug: int = Query(0, description="1이면 KASI raw 일부/메타 포함"),
):
    birth_date = _parse_birth(birth)
    cal = _normalize_calendar(calendar)

    # -------------------------
    # Step 1) 음양력 변환 / 간지(가능 범위) 확보
    # -------------------------
    lunar_info: Dict[str, Any] = {}
    solar_date: dt.date

    if cal == "solar":
        solar_date = birth_date
        y, m, d = solar_date.strftime("%Y"), solar_date.strftime("%m"), solar_date.strftime("%d")

        url = f"{KASI_LUNAR_BASE}/getLunCalInfo"
        raw_xml, meta, items = _kasi_get(
            url,
            {
                "solYear": y,
                "solMonth": m,
                "solDay": d,
                "numOfRows": 10,
                "pageNo": 1,
            },
        )
        item = items[0] if items else {}

        is_leap = (item.get("lunLeapmonth") == "윤") or (item.get("isLeap") in ("true", "Y", "1"))

        lunar_label = item.get("lunarLabel")
        if not lunar_label and item.get("lunMonth") and item.get("lunDay"):
            lunar_label = f"{'윤달 ' if item.get('lunLeapmonth') == '윤' else ''}{item.get('lunMonth')}월 {item.get('lunDay')}일"

        lunar_info = {
            "result": meta,
            "solar": {"year": y, "month": m, "day": d},
            "lunar": {
                "lunYear": item.get("lunYear"),
                "lunMonth": item.get("lunMonth"),
                "lunDay": item.get("lunDay"),
                "isLeap": bool(is_leap),
                "lunLeapmonth": item.get("lunLeapmonth"),
                "lunarLabel": lunar_label,
            },
            "ganji": {
                "rawGanji": {
                    "year": item.get("year"),
                    "month": item.get("month"),
                    "day": item.get("day"),
                }
            },
        }
        if debug:
            lunar_info["debugRawXml"] = raw_xml[:1500]

    elif cal == "lunar":
        # 음력 입력 → 양력으로 변환 필요
        y, m, d = birth_date.strftime("%Y"), birth_date.strftime("%m"), birth_date.strftime("%d")

        url = f"{KASI_LUNAR_BASE}/getSolCalInfo"
        raw_xml, meta, items = _kasi_get(
            url,
            {
                "lunYear": y,
                "lunMonth": m,
                "lunDay": d,
                "lunLeapmonth": "평",
                "numOfRows": 10,
                "pageNo": 1,
            },
        )
        item = items[0] if items else {}
        sol_y = item.get("solYear")
        sol_m = item.get("solMonth")
        sol_d = item.get("solDay")

        if not (sol_y and sol_m and sol_d):
            raise HTTPException(status_code=502, detail="KASI lunar->solar conversion returned empty solar date")

        solar_date = dt.datetime.strptime(f"{sol_y}{sol_m}{sol_d}", "%Y%m%d").date()

        lunar_info = {
            "result": meta,
            "lunar": {"year": y, "month": m, "day": d, "lunLeapmonth": "평"},
            "solar": {"year": sol_y, "month": sol_m, "day": sol_d},
            "ganji": {"rawGanji": {"year": item.get("year"), "month": item.get("month"), "day": item.get("day")}},
        }
        if debug:
            lunar_info["debugRawXml"] = raw_xml[:1500]
    else:
        raise HTTPException(status_code=400, detail="calendar must be solar or lunar")

    # -------------------------
    # Step 2) 24절기 조회 (출생월 + 전월)
    # -------------------------
    all_jieqi_items: List[Dict[str, Any]] = []
    jieqi_debug: List[Dict[str, Any]] = []

    for q_date in _safe_month_query_dates(solar_date):
        sol_year, sol_month = _ym(q_date)
        url = f"{KASI_SPCDE_BASE}/get24DivisionsInfo"

        raw_xml, meta, items = _kasi_get(
            url,
            {
                "solYear": sol_year,
                "solMonth": sol_month,  # ✅ 핵심: 월(2자리) 포함
                "numOfRows": 50,
                "pageNo": 1,
            },
        )

        all_jieqi_items.extend(items)

        if debug:
            jieqi_debug.append(
                {
                    "queryYear": sol_year,
                    "queryMonth": sol_month,
                    "meta": meta,
                    "rawXmlHead": raw_xml[:800],
                }
            )

    jieqi_list = _dedupe_sort_jieqi(all_jieqi_items)
    prev_jieqi = _find_prev_jieqi(jieqi_list, solar_date)

    resp: Dict[str, Any] = {
        "input": {"birth": birth, "calendar": cal},
        "solarDate": solar_date.strftime("%Y-%m-%d"),
        "lunarInfo": lunar_info,
        "jieqiList": jieqi_list,
        "prevJieQi": prev_jieqi,
    }

    if not jieqi_list:
        resp["warning"] = "jieqiList is empty. Check KASI key permissions or parameter mismatch."
    if debug:
        resp["debug"] = {"jieqiQueries": jieqi_debug}

    return resp
