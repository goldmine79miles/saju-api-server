#!/usr/bin/env python3
"""
2026~2028 달력 데이터 생성 (main.py 함수 복사 사용)
"""
import json
from datetime import date, datetime
import random
import requests
import os
import xml.etree.ElementTree as ET

# 천간지지
STEMS = ["甲","乙","丙","丁","戊","己","庚","辛","壬","癸"]
BRANCHES = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"]
STEMS_KR = ["갑","을","병","정","무","기","경","신","임","계"]
BRANCHES_KR = ["자","축","인","묘","진","사","오","미","신","유","술","해"]

DAY_PILLAR_JDN_OFFSET = 49

def gregorian_to_jdn(y, m, d):
    a = (14 - m) // 12
    y2 = y + 4800 - a
    m2 = m + 12 * a - 3
    return d + (153*m2+2)//5 + 365*y2 + y2//4 - y2//100 + y2//400 - 32045

def get_day_ganji(dt: date):
    idx = (gregorian_to_jdn(dt.year, dt.month, dt.day) + DAY_PILLAR_JDN_OFFSET) % 60
    return {
        "stem_kr": STEMS_KR[idx % 10],
        "branch_kr": BRANCHES_KR[idx % 12],
        "ganji": STEMS[idx % 10] + BRANCHES[idx % 12]
    }

# 절기 데이터
with open('jieqi_1900_2052.json', 'r', encoding='utf-8') as f:
    JIEQI_DATA = json.load(f)

def get_jieqi_for_date(dt: date):
    year_data = JIEQI_DATA.get(str(dt.year), [])
    for jq in year_data:
        kst_str = jq['kst']
        jq_dt = datetime.fromisoformat(kst_str.replace('+09:00', ''))
        if jq_dt.year == dt.year and jq_dt.month == dt.month and jq_dt.day == dt.day:
            return jq['name']
    return ""

# KASI 함수 (main.py에서 복사)
KASI_SERVICE_KEY = os.getenv("KASI_SERVICE_KEY", "").strip()
KASI_BASE = "https://apis.data.go.kr/B090041/openapi/service/LrsrCldInfoService"

def _kasi_parse_item(resp):
    try:
        data = resp.json()
        item = data.get("response", {}).get("body", {}).get("items", {}).get("item")
        if isinstance(item, list):
            item = item[0] if item else None
        if isinstance(item, dict):
            return item
    except Exception:
        pass
    
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

def _kasi_call(endpoint: str, params: dict):
    if not KASI_SERVICE_KEY:
        raise RuntimeError("KASI_SERVICE_KEY is missing")
    
    q = {"serviceKey": KASI_SERVICE_KEY, "_type": "json"}
    q.update(params)
    url = f"{KASI_BASE}/{endpoint}"
    
    resp = requests.get(url, params=q, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"KASI HTTP {resp.status_code}")
    
    item = _kasi_parse_item(resp)
    if not item:
        raise RuntimeError(f"KASI returned empty item")
    return item

def kasi_sol_to_lun(sol_year: int, sol_month: int, sol_day: int):
    item = _kasi_call("getLunCalInfo", {
        "solYear": str(sol_year),
        "solMonth": f"{sol_month:02d}",
        "solDay": f"{sol_day:02d}",
    })
    lun_year = int(item.get("lunYear"))
    lun_month = int(item.get("lunMonth"))
    lun_day = int(item.get("lunDay"))
    leap = (item.get("lunLeapmonth") == "윤")
    
    return {
        "year": lun_year,
        "month": lun_month,
        "day": lun_day,
        "is_leap_month": leap
    }

# 명절
LUNAR_HOLIDAYS = {
    (1, 1): '설날🧧',
    (1, 15): '정월대보름',
    (4, 8): '석가탄신일',
    (5, 5): '단오',
    (7, 7): '칠석',
    (8, 15): '추석🌕'
}

SOLAR_HOLIDAYS = {
    (1, 1): '신정',
    (3, 1): '삼일절',
    (5, 5): '어린이날',
    (6, 6): '현충일',
    (8, 15): '광복절',
    (10, 3): '개천절',
    (10, 9): '한글날',
    (12, 25): '크리스마스🎄'
}

def get_holiday(dt: date, lunar_info: dict):
    # 양력 명절
    solar = SOLAR_HOLIDAYS.get((dt.month, dt.day), '')
    if solar:
        return solar
    
    # 음력 명절
    if lunar_info:
        lunar = LUNAR_HOLIDAYS.get((lunar_info['month'], lunar_info['day']), '')
        if lunar:
            return lunar
    
    return ''

def generate_month_levels(days_count):
    levels = ['주의']*4 + ['신중']*4 + ['보통']*8 + ['양호']*10 + ['길일']*5
    while len(levels) < days_count:
        levels.append('보통')
    random.shuffle(levels)
    return levels[:days_count]

def generate_calendar_data():
    result = {}
    
    for year in range(2026, 2029):
        print(f"\n📅 {year}년 생성 중...")
        result[year] = {}
        
        for month in range(1, 13):
            if month in [1, 3, 5, 7, 8, 10, 12]:
                days_in_month = 31
            elif month in [4, 6, 9, 11]:
                days_in_month = 30
            else:
                if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
                    days_in_month = 29
                else:
                    days_in_month = 28
            
            levels = generate_month_levels(days_in_month)
            month_data = []
            
            print(f"  {month}월...", end=' ', flush=True)
            
            for day in range(1, days_in_month + 1):
                dt = date(year, month, day)
                weekday = dt.weekday()
                
                ganji = get_day_ganji(dt)
                jieqi = get_jieqi_for_date(dt)
                
                # 음력 조회
                lunar_info = None
                try:
                    lunar_info = kasi_sol_to_lun(year, month, day)
                except Exception as e:
                    if day == 1:  # 첫날만 에러 표시
                        print(f"\n  ⚠️  KASI 에러: {e}", flush=True)
                        print(f"  {month}월...", end=' ', flush=True)
                
                # 음력 문자열
                lunar_str = ""
                if lunar_info:
                    leap = '(윤)' if lunar_info.get('is_leap_month') else ''
                    lunar_str = f"{lunar_info['month']}.{lunar_info['day']}{leap}"
                
                # 명절
                holiday = get_holiday(dt, lunar_info)
                
                # 표시 우선순위: 명절 > 음력
                lunar_display = holiday if holiday else lunar_str
                
                month_data.append({
                    "day": day,
                    "weekday": (weekday + 1) % 7,
                    "gan": ganji['stem_kr'],
                    "ji": ganji['branch_kr'],
                    "ganHanja": ganji['ganji'],
                    "lunar": lunar_display,
                    "jieqi": jieqi,
                    "level": levels[day - 1]
                })
            
            print("✅")
            result[year][month] = month_data
    
    return result

# 생성
print("="*50)
print("달력 데이터 생성 시작")
print("="*50)

calendar_data = generate_calendar_data()

# TypeScript 출력
output = """// 자동 생성된 달력 데이터 (2026~2028)
// 생성일: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """
// 음력: KASI API 연동

export interface DayData {
  day: number;
  weekday: number;  // 0=일, 1=월, ..., 6=토
  gan: string;      // 갑, 을, 병 등
  ji: string;       // 자, 축, 인 등
  ganHanja: string; // 甲子, 乙丑 등
  lunar: string;    // 12.2, 설날🧧 등
  jieqi: string;    // 입춘 등
  level: string;    // 주의, 신중, 보통, 양호, 길일 (임시)
}

export const CALENDAR_DATA: Record<number, Record<number, DayData[]>> = """

output += json.dumps(calendar_data, ensure_ascii=False, indent=2)
output += ";\n"

with open('/mnt/user-data/outputs/CalendarData.ts', 'w', encoding='utf-8') as f:
    f.write(output)

print("\n" + "="*50)
print("✅ CalendarData.ts 생성 완료!")
print(f"   - 2026~2028년 데이터")
print(f"   - 총 {sum(len(calendar_data[y][m]) for y in calendar_data for m in calendar_data[y])}일")
print("="*50)
