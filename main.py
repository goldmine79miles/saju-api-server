from fastapi import FastAPI, Query
import os
import requests
import xml.etree.ElementTree as ET
app = FastAPI()
KASI_BASE_LUNAR = "https://apis.data.go.kr/B090041/openapi/service/LrsrCldInfoService"

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

    return {
        "resultCode": result_code,
        "resultMsg": result_msg,
        "lunYear": _get_text(item, "lunYear"),
        "lunMonth": _get_text(item, "lunMonth"),
        "lunDay": _get_text(item, "lunDay"),
        "lunLeapmonth": _get_text(item, "lunLeapmonth"),
        "lunNday": _get_text(item, "lunNday"),
        "rawGanji": {
            "year": _get_text(item, "lunSecha"),
            "month": _get_text(item, "lunWolgeon"),
            "day": _get_text(item, "lunIljin"),
        },
    }

@app.get("/")
def health_check():
    return {
        "status": "ok",
        "message": "saju api server running"
    }
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

    data = kasi_sol_to_lun(y, m, d, service_key)

    return {
        "input": {
            "birth": birth,
            "calendar": calendar
        },
        "kasi": data
    }
