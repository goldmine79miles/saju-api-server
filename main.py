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


def kasi_get_jieqi(year: str, service_key: str):
    """
    24절기 리스트 반환
    성공: (list, None)
    실패/빈값: ([], debug_dict)
    """
    url = f"{KASI_BASE_SPCDE}/get24DivisionsInfo"

    # KASI 쪽에서 int를 기대하는 케이스가 있어서 안전하게 int 변환
    try:
        sol_year_val = int(year)
    except Exception:
        sol_year_val = year

    params = {
        "serviceKey": service_key,
        "solYear": sol_year_val,
        "numOfRows": 50,
        "pageNo": 1,
    }

    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
    except Exception as e:
        return [], {
            "error": "REQUEST_FAILED",
            "detail": str(e),
            "url": url,
            "params": {"solYear": str(sol_year_val), "numOfRows": 50, "pageNo": 1},
        }

    raw = r.text
    try:
        root = ET.fromstring(raw)
    except Exception as e:
        return [], {
            "error": "XML_PARSE_FAILED",
            "detail": str(e),
            "raw": raw[:2000],
        }

    # KASI는 HTTP 200이어도 resultCode/resultMsg로 실패를 알려줄 수 있음
    result_code = _get_text(root, ".//resultCode")
    result_msg = _get_text(root, ".//resultMsg")

    items = root.findall(".//item")
    out = []
    for it in items:
        out.append({
            "name": _get_text(it, "dateName"),
            "date": _get_text(it, "locdate"),  # YYYYMMDD
        })

    if not out:
        return [], {
            "error": "EMPTY_ITEMS",
            "resultCode": result_code,
            "resultMsg": result_msg,
            "raw": raw[:2000],
        }

    return out, None


def pick_prev_jieqi(jieqi_list, birth_ymd: str):
    """
    출생일 기준 직전 절기 1개 반환
    jieqi_list: [{"name":"입춘","date":"19790204"}, ...]
    """
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
        # 연초 이전이면(혹은 비교 실패) 마지막 절기로 fallback
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

    # 1) 음력/윤달/연·일 간지
    lunar_data = kasi_sol_to_lun(y, m, d, service_key)

    # 2) 24절기 (debug 포함)
    jieqi_list, jieqi_debug = kasi_get_jieqi(y, service_key)

    # 3) 출생일 기준 직전 절기
    prev_jieqi = pick_prev_jieqi(jieqi_list, birth) if jieqi_list else None

    resp = {
        "input": {"birth": birth, "calendar": calendar},
        "kasi": lunar_data,
        "jieqiList": jieqi_list,
        "prevJieQi": prev_jieqi,
    }

    # 절기 비었으면 디버그를 같이 내려서 원인 바로 확인
    if not jieqi_list and jieqi_debug:
        resp["jieqiDebug"] = jieqi_debug

    return resp
