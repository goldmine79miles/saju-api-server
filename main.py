from fastapi import FastAPI, Query
import os
import requests
import xml.etree.ElementTree as ET

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
    r = requests.get(url, params=params, timeout=10)
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
    - year: "1979"
    - return: [{"name": "입춘", "date": "19790204"}, ...]
    """
    url = f"{KASI_BASE_SPCDE}/get24DivisionsInfo"
    params = {
        "serviceKey": service_key,
        "solYear": int(year),
        "numOfRows": 50,
        "pageNo": 1,
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()

    root = ET.fromstring(r.text)
    items = root.findall(".//item")

    out = []
    for it in items:
        out.append({
            "name": _get_text(it, "dateName"),
            "date": _get_text(it, "locdate"),  # YYYYMMDD
        })
    return out


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
    jieqi_list = kasi_get_jieqi(y, service_key)

    return {
        "input": {"birth": birth, "calendar": calendar},
        "kasi": lunar_data,
        "jieqiList": jieqi_list
    }
