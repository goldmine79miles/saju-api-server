from fastapi import FastAPI, Query
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
import json
import os
from pathlib import Path
import requests
import xml.etree.ElementTree as ET

print("[BOOT] main.py LOADED ✅", os.path.abspath(__file__), flush=True)

app = FastAPI(
    title="Saju API Server",
    version="1.9.0"
)

# ==================================================
# PATHS
# ==================================================
THIS_DIR = Path(__file__).resolve().parent

PROJECT_ROOT = THIS_DIR
if not (PROJECT_ROOT / "data").exists() and (PROJECT_ROOT.parent / "data").exists():
    PROJECT_ROOT = PROJECT_ROOT.parent

DATA_DIR = PROJECT_ROOT / "data"
JIEQI_TABLE_PATH = DATA_DIR / "jieqi_1900_2052.json"

KST = ZoneInfo("Asia/Seoul")
UTC = timezone.utc

SEOUL_FIXED_OFFSET_MINUTES = 32


# ==================================================
# KASI (Korea Astronomy and Space Science Institute) Calendar API
# - Solar <-> Lunar conversion (including leap month validation)
# - Authority: KASI via data.go.kr OpenAPI
# ==================================================
KASI_SERVICE_KEY = os.getenv("KASI_SERVICE_KEY", "").strip()
KASI_BASE = "https://apis.data.go.kr/B090041/openapi/service/LrsrCldInfoService"

def _kasi_parse_item(resp: requests.Response) -> dict:
    """Parse KASI response item safely (JSON preferred, XML fallback)."""
    # JSON (when _type=json works)
    try:
        data = resp.json()
        item = (
            data.get("response", {})
                .get("body", {})
                .get("items", {})
                .get("item")
        )
        if isinstance(item, list):
            item = item[0] if item else None
        if isinstance(item, dict):
            return item
    except Exception:
        pass

    # XML fallback
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

def _kasi_call(endpoint: str, params: dict) -> dict:
    if not KASI_SERVICE_KEY:
        raise RuntimeError("KASI_SERVICE_KEY is missing on server")

    q = {"serviceKey": KASI_SERVICE_KEY, "_type": "json"}
    q.update(params)
    url = f"{KASI_BASE}/{endpoint}"

    resp = requests.get(url, params=q, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"KASI HTTP {resp.status_code}: {resp.text[:200]}")

    item = _kasi_parse_item(resp)
    if not item:
        raise RuntimeError(f"KASI returned empty item: {resp.text[:200]}")
    return item

def kasi_sol_to_lun(sol_year: int, sol_month: int, sol_day: int) -> dict:
    """Solar -> Lunar. Returns normalized lunar fields + leap flag."""
    item = _kasi_call("getLunCalInfo", {
        "solYear": str(sol_year),
        "solMonth": f"{sol_month:02d}",
        "solDay": f"{sol_day:02d}",
    })
    lun_year = int(item.get("lunYear"))
    lun_month = int(item.get("lunMonth"))
    lun_day = int(item.get("lunDay"))
    leap = (item.get("lunLeapmonth") == "윤")
    label = f"음력 {lun_year}년 " + (f"윤{lun_month}월 " if leap else f"{lun_month}월 ") + f"{lun_day}일"
    return {
        "year": lun_year,
        "month": lun_month,
        "day": lun_day,
        "is_leap_month": leap,
        "label_kr": label,
        "_raw": {k: item.get(k) for k in ("lunLeapmonth","lunSecha","lunIljin") if k in item}
    }

def kasi_lun_to_sol(lun_year: int, lun_month: int, lun_day: int, is_leap_month: bool) -> dict:
    """Lunar(+leap) -> Solar. Returns confirmed solar date."""
    item = _kasi_call("getSolCalInfo", {
        "lunYear": str(lun_year),
        "lunMonth": f"{lun_month:02d}",
        "lunDay": f"{lun_day:02d}",
        "lunLeapmonth": "윤" if is_leap_month else "평",
    })
    sol_year = int(item.get("solYear"))
    sol_month = int(item.get("solMonth"))
    sol_day = int(item.get("solDay"))
    return {"year": sol_year, "month": sol_month, "day": sol_day}

def load_jieqi_table():
    if not JIEQI_TABLE_PATH.exists():
        raise FileNotFoundError(f"[JIEQI] missing file: {JIEQI_TABLE_PATH}")
    with JIEQI_TABLE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)

def _parse_dt_any(value, assume_tz):
    if value is None:
        return None
    if isinstance(value, str):
        s = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=assume_tz)
        return dt.astimezone(KST)
    return None

def _pick_item_dt(item):
    if "kst" in item:
        dt = _parse_dt_any(item.get("kst"), KST)
        if dt:
            return dt
    if "utc" in item:
        dt = _parse_dt_any(item.get("utc"), UTC)
        if dt:
            return dt
    return None

def get_jieqi_with_fallback(year: str):
    table = load_jieqi_table()
    year_data = table.get(year)
    if not year_data:
        raise ValueError(f"No jieqi for {year}")
    return year_data

def find_ipchun_dt(jieqi_list):
    for item in jieqi_list:
        if item.get("name") in ("입춘", "立春"):
            dt = _pick_item_dt(item)
            if dt:
                return dt
    raise ValueError("입춘 not found")

def _jieqi_term_dt_map(jieqi_list):
    m = {}
    for item in jieqi_list:
        name = item.get("name")
        dt = _pick_item_dt(item)
        if name and dt:
            m[name] = dt
    return m

STEMS = ["甲","乙","丙","丁","戊","己","庚","辛","壬","癸"]
BRANCHES = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"]


# ==================================================
# KR READING + TEN GODS + HIDDEN STEMS (for infographic)
# SSOT in API: frontend should render only.
# ==================================================
STEM_KR = {
    "甲": "갑", "乙": "을", "丙": "병", "丁": "정", "戊": "무",
    "己": "기", "庚": "경", "辛": "신", "壬": "임", "癸": "계",
}
BRANCH_KR = {
    "子": "자", "丑": "축", "寅": "인", "卯": "묘", "辰": "진", "巳": "사",
    "午": "오", "未": "미", "申": "신", "酉": "유", "戌": "술", "亥": "해",
}

YANG_STEMS = set(["甲","丙","戊","庚","壬"])
STEM_POLARITY = {s: ("yang" if s in YANG_STEMS else "yin") for s in STEMS}

STEM_ELEMENT_HANJA = {
    "甲": "목", "乙": "목",
    "丙": "화", "丁": "화",
    "戊": "토", "己": "토",
    "庚": "금", "辛": "금",
    "壬": "수", "癸": "수",
}

# Generating cycle: wood -> fire -> earth -> metal -> water -> wood
GEN = {"목": "화", "화": "토", "토": "금", "금": "수", "수": "목"}
# Controlling cycle: wood controls earth, earth controls water, water controls fire, fire controls metal, metal controls wood
CTL = {"목": "토", "토": "수", "수": "화", "화": "금", "금": "목"}

def ten_god_of_stem(day_stem: str, target_stem: str) -> str:
    """Return 십성 of target_stem relative to day_stem (일간 기준).
    Uses standard element+yin/yang rules:
      - same element: 비견/겁재
      - day generates target: 식신/상관
      - day controls target: 편재/정재
      - target controls day: 편관/정관
      - target generates day: 편인/정인
    """
    if not day_stem or not target_stem:
        return ""
    de = STEM_ELEMENT_HANJA.get(day_stem)
    te = STEM_ELEMENT_HANJA.get(target_stem)
    if not de or not te:
        return ""

    same_polar = (STEM_POLARITY.get(day_stem) == STEM_POLARITY.get(target_stem))

    if te == de:
        return "비견" if same_polar else "겁재"

    # day generates target => output gods
    if GEN.get(de) == te:
        return "식신" if same_polar else "상관"

    # day controls target => wealth gods
    if CTL.get(de) == te:
        return "편재" if same_polar else "정재"

    # target controls day => officer gods
    if CTL.get(te) == de:
        return "편관" if same_polar else "정관"

    # target generates day => resource gods
    if GEN.get(te) == de:
        return "편인" if same_polar else "정인"

    return ""

# Hidden stems by branch (지장간)
HIDDEN_STEMS_BY_BRANCH = {
    "子": ["癸"],
    "丑": ["己","癸","辛"],
    "寅": ["甲","丙","戊"],
    "卯": ["乙"],
    "辰": ["戊","乙","癸"],
    "巳": ["丙","戊","庚"],
    "午": ["丁","己"],
    "未": ["己","丁","乙"],
    "申": ["庚","壬","戊"],
    "酉": ["辛"],
    "戌": ["戊","辛","丁"],
    "亥": ["壬","甲"],
}

# Main hidden stem (정기) for branch ten-god
MAIN_HIDDEN_STEM_BY_BRANCH = {
    "子": "癸", "丑": "己", "寅": "甲", "卯": "乙",
    "辰": "戊", "巳": "丙", "午": "丁", "未": "己",
    "申": "庚", "酉": "辛", "戌": "戊", "亥": "壬",
}

def enrich_pillar(p: dict, day_stem: str):
    """Mutate pillar dict in-place to include KR reading + ten gods + hidden stems."""
    if not p:
        return
    stem = p.get("stem")
    branch = p.get("branch")

    if stem:
        p["stem_kr"] = STEM_KR.get(stem, "")
        p["ten_god_stem"] = ten_god_of_stem(day_stem, stem)

    if branch:
        p["branch_kr"] = BRANCH_KR.get(branch, "")
        hidden = HIDDEN_STEMS_BY_BRANCH.get(branch, [])
        p["hidden_stems"] = hidden
        p["ten_god_hidden"] = [ten_god_of_stem(day_stem, hs) for hs in hidden]
        main_hidden = MAIN_HIDDEN_STEM_BY_BRANCH.get(branch, "")
        p["ten_god_branch"] = ten_god_of_stem(day_stem, main_hidden) if main_hidden else ""



# ==================================================
# ILJU ANIMAL (Color + Zodiac Animal)
# - Color from Day Stem element
# - Animal from Day Branch (12 zodiac)
# ==================================================
STEM_ELEMENT = {
    "甲": "목", "乙": "목",
    "丙": "화", "丁": "화",
    "戊": "토", "己": "토",
    "庚": "금", "辛": "금",
    "壬": "수", "癸": "수",
}
ELEMENT_COLOR_KR = {
    "목": "푸른",
    "화": "붉은",
    "토": "누런",
    "금": "하얀",
    "수": "검은",
}
BRANCH_ANIMAL_KR = {
    "子": "쥐", "丑": "소", "寅": "호랑이", "卯": "토끼",
    "辰": "용", "巳": "뱀", "午": "말", "未": "양",
    "申": "원숭이", "酉": "닭", "戌": "개", "亥": "돼지",
}
ANIMAL_EMOJI = {
    "쥐": "🐭", "소": "🐮", "호랑이": "🐯", "토끼": "🐰",
    "용": "🐲", "뱀": "🐍", "말": "🐴", "양": "🐑",
    "원숭이": "🐵", "닭": "🐔", "개": "🐶", "돼지": "🐷",
}

def get_ilju_animal(day_gan: str, day_ji: str) -> str:
    """Return e.g. '하얀 돼지'. Deterministic mapping only."""
    elem = STEM_ELEMENT.get(day_gan, "")
    color = ELEMENT_COLOR_KR.get(elem, "")
    animal = BRANCH_ANIMAL_KR.get(day_ji, "")
    if not color or not animal:
        return ""
    return f"{color} {animal}"

def get_ilju_emoji(day_ji: str) -> str:
    animal = BRANCH_ANIMAL_KR.get(day_ji, "")
    return ANIMAL_EMOJI.get(animal, "🐾")

DAY_PILLAR_JDN_OFFSET = 49

def gregorian_to_jdn(y, m, d):
    a = (14 - m) // 12
    y2 = y + 4800 - a
    m2 = m + 12 * a - 3
    return d + (153*m2+2)//5 + 365*y2 + y2//4 - y2//100 + y2//400 - 32045

def get_day_pillar(dt: date):
    idx = (gregorian_to_jdn(dt.year, dt.month, dt.day) + DAY_PILLAR_JDN_OFFSET) % 60
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

MONTH_TERM_TO_BRANCH = [
    ("입춘", "寅"), ("경칩", "卯"), ("청명", "辰"),
    ("입하", "巳"), ("망종", "午"), ("소서", "未"),
    ("입추", "申"), ("백로", "酉"), ("한로", "戌"),
    ("입동", "亥"), ("대설", "子"), ("소한", "丑"),
]

YEAR_STEM_TO_YIN_MONTH_STEM = {
    "甲": "丙", "己": "丙", "乙": "戊", "庚": "戊",
    "丙": "庚", "辛": "庚", "丁": "壬", "壬": "壬",
    "戊": "甲", "癸": "甲",
}

MONTH_BRANCH_SEQ = ["寅","卯","辰","巳","午","未","申","酉","戌","亥","子","丑"]

def _get_month_branch_by_jul(input_dt, this_year_terms, prev_year_terms):
    candidates = []
    for term, branch in MONTH_TERM_TO_BRANCH:
        dt = this_year_terms.get(term)
        if dt:
            candidates.append((dt, branch))
    prev_daeseol = prev_year_terms.get("대설")
    if prev_daeseol:
        candidates.append((prev_daeseol, "子"))
    valid = [c for c in candidates if c[0] <= input_dt]
    if not valid:
        return "丑"
    valid.sort(key=lambda x: x[0])
    return valid[-1][1]

def get_month_pillar(input_dt, saju_year_pillar, jieqi_this_year, jieqi_prev_year):
    this_map = _jieqi_term_dt_map(jieqi_this_year)
    prev_map = _jieqi_term_dt_map(jieqi_prev_year)
    month_branch = _get_month_branch_by_jul(input_dt, this_map, prev_map)
    year_stem = saju_year_pillar["stem"]
    yin_month_stem = YEAR_STEM_TO_YIN_MONTH_STEM[year_stem]
    month_index = MONTH_BRANCH_SEQ.index(month_branch)
    stem_index = (STEMS.index(yin_month_stem) + month_index) % 10
    month_stem = STEMS[stem_index]
    return {"stem": month_stem, "branch": month_branch, "ganji": month_stem + month_branch}

HOUR_BRANCH_SEQ = ["子","丑","寅","卯","辰","巳","午","未","申","酉","戌","亥"]

DAY_STEM_TO_ZI_HOUR_STEM = {
    "甲": "甲", "己": "甲", "乙": "丙", "庚": "丙",
    "丙": "戊", "辛": "戊", "丁": "庚", "壬": "庚",
    "戊": "壬", "癸": "壬",
}

def _get_hour_branch(hh, mm):
    total = hh * 60 + mm
    shifted = (total - 23 * 60) % (24 * 60)
    return HOUR_BRANCH_SEQ[int(shifted // 120)]

def get_hour_pillar(day_pillar, hh, mm):
    hour_branch = _get_hour_branch(hh, mm)
    zi_hour_stem = DAY_STEM_TO_ZI_HOUR_STEM[day_pillar["stem"]]
    stem_index = (STEMS.index(zi_hour_stem) + HOUR_BRANCH_SEQ.index(hour_branch)) % 10
    hour_stem = STEMS[stem_index]
    return {"stem": hour_stem, "branch": hour_branch, "ganji": hour_stem + hour_branch}

@app.get("/api/saju/calc")
def calc_saju(
    birth: str = Query(...),
    calendar: str = Query("solar"),
    birth_time: str = Query("unknown"),
    gender: str = Query("unknown"),
    is_leap_month: bool = Query(False),
):
    from fastapi import HTTPException

    # --------------------------------------------------
    # 1) Interpret input date by calendar type
    # - calendar=solar: birth is solar YYYY-MM-DD
    # - calendar=lunar: birth is lunar YYYY-MM-DD (+ is_leap_month)
    # Always compute pillars based on confirmed solar date (SSOT for calculation).
    # --------------------------------------------------
    try:
        birth_date_in = datetime.strptime(birth, "%Y-%m-%d").date()

        if (calendar or "").lower() == "lunar":
            sol = kasi_lun_to_sol(
                birth_date_in.year, birth_date_in.month, birth_date_in.day, bool(is_leap_month)
            )
            solar_confirmed = date(sol["year"], sol["month"], sol["day"])
        else:
            solar_confirmed = birth_date_in

        # For UI/infographic: always provide normalized lunar derived from confirmed solar
        lunar_meta = kasi_sol_to_lun(solar_confirmed.year, solar_confirmed.month, solar_confirmed.day)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"KASI calendar conversion failed: {e}")

    # --------------------------------------------------
    # 2) Time handling (kept as-is)
    # --------------------------------------------------
    bt = (birth_time or "").strip().lower()
    if bt and bt not in ("unknown", "null", "none"):
        hh, mm = map(int, bt.split(":"))
        has_time = True
    else:
        hh, mm = 0, 0
        has_time = False

    input_dt = datetime(solar_confirmed.year, solar_confirmed.month, solar_confirmed.day, hh, mm, tzinfo=KST)
    calc_dt = input_dt - timedelta(minutes=SEOUL_FIXED_OFFSET_MINUTES) if has_time else input_dt

    # --------------------------------------------------
    # 3) Pillar calculation (solar-based)
    # --------------------------------------------------
    jieqi_this = get_jieqi_with_fallback(str(input_dt.year))
    ipchun_dt = find_ipchun_dt(jieqi_this)
    saju_year = input_dt.year if input_dt >= ipchun_dt else input_dt.year - 1

    year_pillar = get_year_pillar(saju_year)
    day_pillar = get_day_pillar(input_dt.date())

    jieqi_prev = get_jieqi_with_fallback(str(input_dt.year - 1))
    month_pillar = get_month_pillar(input_dt, year_pillar, jieqi_this, jieqi_prev)
    hour_pillar = get_hour_pillar(day_pillar, calc_dt.hour, calc_dt.minute) if has_time else None

    # --------------------------------------------------
    # 3.5) Enrich pillars for infographic (ten gods + hidden stems)
    # --------------------------------------------------
    pillars = {"year": year_pillar, "month": month_pillar, "day": day_pillar, "hour": hour_pillar}
    day_stem = (day_pillar or {}).get("stem", "")
    for _k in ("year", "month", "day", "hour"):
        _p = pillars.get(_k)
        if _p:
            enrich_pillar(_p, day_stem)

    return {
        "input": {
            "birth": birth,
            "calendar": calendar,
            "birth_time": birth_time,
            "gender": gender,
            "is_leap_month": is_leap_month,
        },
        "meta": {
            "solar_confirmed": {
                "year": input_dt.year,
                "month": input_dt.month,
                "day": input_dt.day,
                "label_kr": f"양력 {input_dt.year}년 {input_dt.month}월 {input_dt.day}일",
            },
            "lunar": lunar_meta,
        },
        "pillars": pillars,
        "ilju_animal": get_ilju_animal(day_pillar.get("stem", ""), day_pillar.get("branch", "")),
        "ilju_emoji": get_ilju_emoji(day_pillar.get("branch", "")),
        "debug": {
            "timezone": "KST",
            "fixed_offset_minutes": SEOUL_FIXED_OFFSET_MINUTES if has_time else 0,
            "input_dt": input_dt.isoformat(),
            "calc_dt": calc_dt.isoformat(),
            "saju_year": saju_year,
        },
    }



# =========================
# API - PDF Generation
# =========================
from fastapi import HTTPException
from fastapi.responses import Response
from playwright.async_api import async_playwright
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from io import BytesIO
import requests
import cairosvg

@app.get("/api/pdf/generate")
async def generate_pdf(rid: str = Query(...), token: str = Query(...)):
    try:
        url = f"https://saju-baksa.com/report/{rid}?t={token}&print=1"
        bg_url = "https://saju-baksa.com/report-bg.png"
        logo_url = "https://saju-baksa.com/logo-text.svg"
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(args=['--no-sandbox', '--disable-setuid-sandbox'])
            page = await browser.new_page()
            await page.goto(url, wait_until="networkidle", timeout=60000)
            await page.wait_for_timeout(3000)
            
            # PDF 생성 시에만 여백 강제 제거 (웹은 그대로)
            await page.evaluate("""
                // 표지 여백 제거
                const cover = document.querySelector('.report-cover');
                if (cover) {
                    cover.style.padding = '0';
                    cover.style.margin = '0';
                    cover.style.pageBreakAfter = 'always';
                    cover.style.breakAfter = 'page';
                }
                
                // 본문 컨테이너 여백 최소화
                const container = document.querySelectorAll('[style*="max-width"]')[0];
                if (container) {
                    container.style.padding = '15mm 15mm';
                    container.style.margin = '0';
                    container.style.pageBreakBefore = 'always';
                }
                
                // 프린트 색상 유지
                document.documentElement.style.webkitPrintColorAdjust = 'exact';
                document.documentElement.style.printColorAdjust = 'exact';
            """)
            
            await page.wait_for_timeout(1000)
            
            # margin 0으로 (표지 꽉 차게)
            pdf_bytes = await page.pdf(
                format="A4",
                print_background=True,
                margin={
                    "top": "0mm",
                    "bottom": "0mm",
                    "left": "0mm",
                    "right": "0mm"
                },
                prefer_css_page_size=False
            )
            
            await browser.close()
        
        # pypdf에서 배경+로고 추가
        pdf_bytes = add_background_and_logo(pdf_bytes, bg_url, logo_url)
        
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename=report-{rid}.pdf"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def add_background_and_logo(original_pdf_bytes, bg_url, logo_url):
    from PIL import Image
    
    original_pdf = PdfReader(BytesIO(original_pdf_bytes))
    output = PdfWriter()
    
    try:
        bg_response = requests.get(bg_url, timeout=10)
        bg_image = Image.open(BytesIO(bg_response.content))
    except:
        bg_image = None
    
    # SVG 로고를 PNG로 변환 (크기 지정)
    logo_image = None
    try:
        logo_response = requests.get(logo_url, timeout=10)
        logo_png = cairosvg.svg2png(
            bytestring=logo_response.content,
            output_width=240,   # 80pt * 3 (고해상도)
            output_height=72    # 24pt * 3
        )
        logo_image = Image.open(BytesIO(logo_png))
        print(f"[LOGO] Successfully loaded: {logo_image.size}")
    except Exception as e:
        print(f"[LOGO ERROR] {e}")
    
    page_width, page_height = A4
    
    for page_num in range(len(original_pdf.pages)):
        page = original_pdf.pages[page_num]
        
        # 표지(0페이지)는 그대로
        if page_num == 0:
            output.add_page(page)
            continue
        
        # 나머지 페이지: 1. 배경 먼저, 2. 페이지, 3. 로고 맨 위
        
        # 1. 배경 레이어
        bg_packet = BytesIO()
        bg_canvas = canvas.Canvas(bg_packet, pagesize=A4)
        if bg_image:
            img_reader = ImageReader(bg_image)
            bg_canvas.drawImage(
                img_reader,
                0, 0,
                width=page_width,
                height=page_height,
                preserveAspectRatio=False,
                mask='auto'
            )
        bg_canvas.save()
        bg_packet.seek(0)
        bg_pdf = PdfReader(bg_packet)
        bg_page = bg_pdf.pages[0]
        
        # 2. 배경 밑에 페이지 올리기 (본문이 위로)
        bg_page.merge_page(page)
        
        # 3. 로고 레이어 (맨 위)
        if logo_image:
            try:
                logo_packet = BytesIO()
                logo_canvas = canvas.Canvas(logo_packet, pagesize=A4)
                
                logo_reader = ImageReader(logo_image)
                logo_width = 80
                logo_height = 24
                logo_x = (page_width - logo_width) / 2
                logo_y = 40
                
                print(f"[LOGO] Drawing at x={logo_x}, y={logo_y}, size={logo_width}x{logo_height}")
                logo_canvas.drawImage(
                    logo_reader,
                    logo_x,
                    logo_y,
                    width=logo_width,
                    height=logo_height,
                    preserveAspectRatio=True,
                    mask='auto'
                )
                logo_canvas.save()
                
                logo_packet.seek(0)
                logo_pdf = PdfReader(logo_packet)
                logo_page = logo_pdf.pages[0]
                
                # 로고를 페이지 맨 위에 합치기
                bg_page.merge_page(logo_page)
                print(f"[LOGO] Page {page_num}: Successfully drawn on top")
            except Exception as e:
                print(f"[LOGO DRAW ERROR] Page {page_num}: {e}")
        
        output.add_page(bg_page)
    
    final_pdf = BytesIO()
    output.write(final_pdf)
    final_pdf.seek(0)
    return final_pdf.read()
