from fastapi import FastAPI, Query
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
import json
import os
from pathlib import Path

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
):
    birth_date = datetime.strptime(birth, "%Y-%m-%d")
    bt = (birth_time or "").strip().lower()
    if bt and bt not in ("unknown", "null", "none"):
        hh, mm = map(int, bt.split(":"))
        has_time = True
    else:
        hh, mm = 0, 0
        has_time = False

    input_dt = datetime(birth_date.year, birth_date.month, birth_date.day, hh, mm, tzinfo=KST)
    calc_dt = input_dt - timedelta(minutes=SEOUL_FIXED_OFFSET_MINUTES) if has_time else input_dt

    jieqi_this = get_jieqi_with_fallback(str(input_dt.year))
    ipchun_dt = find_ipchun_dt(jieqi_this)
    saju_year = input_dt.year if input_dt >= ipchun_dt else input_dt.year - 1

    year_pillar = get_year_pillar(saju_year)
    day_pillar = get_day_pillar(input_dt.date())

    jieqi_prev = get_jieqi_with_fallback(str(input_dt.year - 1))
    month_pillar = get_month_pillar(input_dt, year_pillar, jieqi_this, jieqi_prev)
    hour_pillar = get_hour_pillar(day_pillar, calc_dt.hour, calc_dt.minute) if has_time else None

    return {
        "input": {"birth": birth, "calendar": calendar, "birth_time": birth_time, "gender": gender},
        "pillars": {"year": year_pillar, "month": month_pillar, "day": day_pillar, "hour": hour_pillar},
        "debug": {
            "timezone": "KST",
            "fixed_offset_minutes": SEOUL_FIXED_OFFSET_MINUTES if has_time else 0,
            "input_dt": input_dt.isoformat(),
            "calc_dt": calc_dt.isoformat(),
            "saju_year": saju_year
        }
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
                
                // 본문 컨테이너 여백 제거
                const container = document.querySelectorAll('[style*="max-width"]')[0];
                if (container) {
                    container.style.padding = '0';
                    container.style.margin = '0';
                    container.style.maxWidth = '100%';
                    container.style.pageBreakBefore = 'always';
                }
                
                // 모든 section 여백 제거
                const sections = document.querySelectorAll('section');
                sections.forEach(s => {
                    s.style.margin = '0';
                    s.style.padding = '15mm';  // 내부 컨텐츠만 padding
                });
                
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
    
    # SVG 로고를 PNG로 변환
    logo_image = None
    try:
        logo_response = requests.get(logo_url, timeout=10)
        logo_png = cairosvg.svg2png(bytestring=logo_response.content)
        logo_image = Image.open(BytesIO(logo_png))
    except Exception as e:
        print(f"[LOGO ERROR] {e}")
    
    page_width, page_height = A4
    
    for page_num in range(len(original_pdf.pages)):
        page = original_pdf.pages[page_num]
        
        # 표지(0페이지)는 그대로
        if page_num == 0:
            output.add_page(page)
            continue
        
        # 나머지 페이지: 배경 + 로고
        packet = BytesIO()
        can = canvas.Canvas(packet, pagesize=A4)
        
        # 배경 이미지
        if bg_image:
            img_reader = ImageReader(bg_image)
            can.drawImage(
                img_reader,
                0, 0,
                width=page_width,
                height=page_height,
                preserveAspectRatio=False,
                mask='auto'
            )
        
        # 로고 이미지 하단 중앙
        if logo_image:
            try:
                logo_reader = ImageReader(logo_image)
                logo_width = 80
                logo_height = 24
                can.drawImage(
                    logo_reader,
                    (page_width - logo_width) / 2,
                    20,
                    width=logo_width,
                    height=logo_height,
                    preserveAspectRatio=True,
                    mask='auto'
                )
            except Exception as e:
                print(f"[LOGO DRAW ERROR] Page {page_num}: {e}")
        
        can.save()
        
        packet.seek(0)
        bg_pdf = PdfReader(packet)
        bg_page = bg_pdf.pages[0]
        bg_page.merge_page(page)
        output.add_page(bg_page)
    
    final_pdf = BytesIO()
    output.write(final_pdf)
    final_pdf.seek(0)
    return final_pdf.read()
