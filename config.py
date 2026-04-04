\"\"\"
config.py - الإعدادات المركزية v26.0
المفاتيح محمية عبر Streamlit Secrets / Railway Env
\"\"\"
import json as _json
import os as _os
import tempfile

_APP_ROOT = _os.path.dirname(_os.path.abspath(__file__))

def _s(key, default=\"\"):
    v = _os.environ.get(key, \"\")
    if v: return v
    try:
        import streamlit as st
        v = st.secrets[key]
        if v is not None: return str(v) if not isinstance(v, (list, dict)) else v
    except: pass
    return default

def _parse_gemini_keys():
    keys = []
    raw = _s(\"GEMINI_API_KEYS\", \"\")
    if isinstance(raw, list): keys = [k for k in raw if k]
    elif raw and isinstance(raw, str):
        if raw.startswith('['):
            try:
                parsed = _json.loads(raw)
                if isinstance(parsed, list): keys = [k for k in parsed if k]
            except:
                clean = raw.strip(\"[]\").replace('\"','').replace(\"'\",'')
                keys = [k.strip() for k in clean.split(',') if k.strip()]
        else: keys = [raw]
    
    for n in [\"GEMINI_API_KEY\", \"GEMINI_KEY\", \"GOOGLE_API_KEY\", \"GEMINI_KEY_1\"]:
        k = _s(n, \"\")
        if k and k not in keys: keys.append(k)
    return [k.strip() for k in keys if len(k.strip()) >= 12]

GEMINI_API_KEYS = _parse_gemini_keys()
OPENROUTER_API_KEY = _s(\"OPENROUTER_API_KEY\") or _s(\"OPENROUTER_KEY\") or \"\"
APIFY_TOKEN = _s(\"APIFY_TOKEN\") or \"\"

COLORS = {\"raise\": \"#dc3545\", \"lower\": \"#ffc107\", \"approved\": \"#28a745\", \"missing\": \"#007bff\", \"review\": \"#ff9800\", \"primary\": \"#6C63FF\"}

# --- إعدادات المطابقة المحسنة لزيادة المنتجات الظاهرة ---
MATCH_THRESHOLD    = 85
HIGH_CONFIDENCE    = 95
REVIEW_THRESHOLD   = 40  # خفضنا العتبة من 75 إلى 40 للسماح بظهور المزيد في المراجعة
PRICE_TOLERANCE    = 5
MIN_MATCH_SCORE    = 40  # خفضنا الحد الأدنى للمطابقة من 62 إلى 40
AUTO_DECISION_CONFIDENCE = 92
SMART_MISSING_FUZZ_THRESHOLD = 75 # خفضنا حاجز المفقودات من 88 إلى 75

# --- فلاتر المنتجات (تم تخفيفها) ---
REJECT_KEYWORDS = [\"sample\",\"عينة\",\"عينه\",\"decant\",\"split\",\"vial\"] # حذفنا 'تقسيم' و 'طقم' للسماح بظهورها
TESTER_KEYWORDS = [\"tester\",\"تستر\"]
SET_KEYWORDS    = [\"set\",\"طقم\",\"مجموعة\"]

# --- إعدادات الكشط ---
SCRAPER_PIPELINE_EVERY = 1 # تحديث فوري عند كل منتج مكسوب
MAHWOUS_UI_LIVE_REFRESH_MS = 2000

DB_PATH = _os.path.join(tempfile.gettempdir(), \"pricing_v18.db\")
