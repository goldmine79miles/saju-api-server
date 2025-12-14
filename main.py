from fastapi import FastAPI, Query
import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

app = FastAPI()

KASI_BASE_LUNAR = "https://apis.data.go.kr/B090041/openapi/service/LrsrCldInfoService"
KASI_BASE_SPCDE = "https://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService"


def _get_text(root, path: str, default=None):
    el = root.find(path)
    return el.text if el is not None else default


def kasi_sol_to_lun(sol_year: str, sol_month: str, sol_day: str, service_key: str):
    url = f"{KASI_BASE_LUNAR}/getLunCalInfo"
    params = {
        "serviceKey": service_key,
        "solYear": sol_year,
        "solMonth": sol_month,
        "solDay": sol_day,
        "numOfRows": 10,
        "pageNo": 1,
    }

    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()

    root = ET.fromstring(r.text)

    result_code = _get_text(root, ".//resultCode")
    result_msg = _get_text(root, ".//resultMsg")

    item = root.find(".//item")
    if item is None:
        return {
            "resultCode": result_code,
            "resultMsg": result_msg,
            "raw": r.text[:2000],
        }

    lun_month = _get_text(item, "lunMonth")
    lun_day = _get_text(item, "lunDay")
    leap_flag = (_get_text(item, "lunLeapmonth") == "윤")

    return {
        "resultCode": result_code,
        "resultMsg": result_msg,
        "lunYear": _get_text(item, "lunYear"),
        "lunMonth": lun_month,
        "lunDay": lun_day,
        "lunLeapmonth": _get_text(item, "lunLeapmonth"),
        "isLeap": leap_flag,
        "lunarLabel": (f"윤달 {lun_month}월 {lun_day}일" if leap_flag else f"{lun_month}월 {lun_day}일"),
        "lunNday": _get_text(item, "lunNday"),
        "rawGanji": {
            "year": _get_text(item, "lunSecha"),
            "month": _get_text(item, "lunWolgeon"),
            "day": _get_text(item, "lunIljin"),
        },
    }


def _parse_jieqi_items(xml_text: str):
    root = ET.fromstring(xml_text)
    result_code = _get_text(root, ".//resultCode")
    result_msg = _get_text(root, ".//resultMsg")
    total_count = _get_text(root, ".//totalCount")
    items = root.findall(".//item")

    out = []
    for it in items:
        out.append({
            "name": _get_text(it, "dateName"),
            "date": _get_text(it, "locdate"),
        })

    debug = {
        "resultCode": result_code,
        "resultMsg": result_msg,
        "totalCount": total_count,
    }
    return out, debug, root


def kasi_get_jieqi(year: str, service_key: str):
    """
    24절기 리스트 반환
    - 1차: get24DivisionsInfo (solYear)
    - 2차(폴백): get24DivisionsInfo (solYear + solMonth='01')  ← 일부 환경에서 필요
    """
    url = f"{KASI_BASE_SPCDE}/get24DivisionsInfo"

    # 1차 시도
    params1 = {
        "serviceKey": service_key,
        "solYear": year,
        "numOfRows": 50,
        "pageNo": 1,
    }
    r1 = requests.get(url, params=params1, timeout=15)
    r1.raise_for_status()
    out1, dbg1, _ = _parse_jieqi_items(r1.text)
    if out1:
        return out1, {"mode": "A", **dbg1}

    # 2차 폴백 (일부 케이스에서 month 파라미터 요구)
    params2 = {
        "serviceKey": service_key,
        "solYear": year,
        "solMonth": "01",
        "numOfRows": 50,
        "pageNo": 1,
    }
    r2 = requests.get(url, params=params2, timeout=15)
    r2.raise_for_status()
    out2, dbg2, _ = _parse_jieqi_items(r2.text)
    if out2:
        return out2, {"mode": "B", **dbg2}

    # 둘 다 실패면 raw 함께 반환
    return [], {
        "error": "EMPTY_ITEMS",
        "tryA": {"params": params1, **dbg1, "raw": r1.text[:1200]},
        "tryB": {"params": params2, **dbg2, "raw": r2.text[:1200]},
    }


def pick_prev_jieqi(jieqi_list, birth_ymd: str):
    try:
        birth_dt = datetime.strptime(birth_ymd, "%Y-%m-%d")
    except Exception:
        return None

    parsed = []
    for j in jieqi_list:
        if j.get("date"):
            try:
                parsed.append({
                    "name": j.get("name"),
                    "date": j.get("date"),
                    "dt": datetime.strptime(j.get("date"), "%Y%m%d")
                })
            except Exception:
                continue

    if not parsed:
        return None

    parsed.sort(key=lambda x: x["dt"])

    prev = None
    for j in parsed:
        if j["dt"] <= birth_dt:
            prev = j
        else:
            break

    if prev is None:
        prev = parsed[-1]

    return {"name": prev["name"], "date": prev["date"]}


@app.get("/")
def health_check():
    return {"status": "ok", "message": "saju api server running"}


@app.get("/api/saju/calc")
def calc(
    birth: str = Query(..., description="YYYY-MM-DD"),
    calendar: str = Query("solar")
):
    service_key = os.getenv("KASI_SERVICE_KEY")
    if not service_key:
        return {"error": "KASI_SERVICE_KEY not set"}

    if calendar != "solar":
        return {"error": "only solar supported for now"}

    try:
        y, m, d = birth.split("-")
    except ValueError:
        return {"error": "birth format must be YYYY-MM-DD"}

    lunar_data = kasi_sol_to_lun(y, m, d, service_key)

    jieqi_list, jieqi_debug = kasi_get_jieqi(y, service_key)
    prev_jieqi = pick_prev_jieqi(jieqi_list, birth) if jieqi_list else None

    resp = {
        "input": {"birth": birth, "calendar": calendar},
        "kasi": lunar_data,
        "jieqiList": jieqi_list,
        "prevJieQi": prev_jieqi,
        "jieqiDebug": jieqi_debug,
    }
    return resp
