"""
engines/ai_engine.py v26.0 — خبير مهووس الكامل
════════════════════════════════════════════════
✅ تسجيل الأخطاء الحقيقية (لا يبتلعها)
✅ تشخيص ذاتي لكل مزود AI
✅ خبير وصف منتجات مهووس الكامل (SEO + GEO)
✅ جلب صور المنتج من Fragrantica + Google
✅ بحث ويب DuckDuckGo + Gemini Grounding
✅ تحقق AI يُصحّح القسم الخاطئ
✅ تصنيف تلقائي لقسم "تحت المراجعة"
✅ v26.0: بحث أشمل في المتاجر السعودية مع تحليل JSON دقيق
"""
import base64
import os as _os
import requests, json, re, time, traceback
from config import get_gemini_api_keys, get_openrouter_api_key, get_cohere_api_key

# أخطاء متوقعة عند استدعاءات HTTP/JSON — لا تُبتلع كل شيء بـ except عريض
_NARROW_IO = (
    ValueError,
    TypeError,
    KeyError,
    IndexError,
    json.JSONDecodeError,
    requests.exceptions.RequestException,
)


def _clip_prompt(s, max_chars: int = 24000) -> str:
    """يقلّص مدخلات المستخدم/النصوص الطويلة قبل إرسالها للـ API."""
    if s is None:
        return ""
    t = str(s)
    if len(t) <= max_chars:
        return t
    return t[: max(0, max_chars - 24)] + "\n…[تم اقتصار النص]"


def _gemini_response_text(data: dict) -> str:
    """استخراج آمن لنصِّ ردّ Gemini من هيكل candidates."""
    if not isinstance(data, dict):
        return ""
    cands = data.get("candidates")
    if not isinstance(cands, list) or not cands:
        return ""
    first = cands[0]
    if not isinstance(first, dict):
        return ""
    content = first.get("content")
    if not isinstance(content, dict):
        return ""
    parts = content.get("parts")
    if not isinstance(parts, list):
        return ""
    chunks: list[str] = []
    for p in parts:
        if isinstance(p, dict):
            tx = p.get("text")
            if isinstance(tx, str) and tx:
                chunks.append(tx)
    return "".join(chunks)


def _openrouter_message_content(data: dict) -> str:
    if not isinstance(data, dict):
        return ""
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    if not isinstance(msg, dict):
        return ""
    c = msg.get("content")
    return c.strip() if isinstance(c, str) else ""


def _ddg_result_line(r: dict, cap: int = 120) -> str:
    """سطر سياق ويب من نتيجة DuckDuckGo (قد لا تحتوي title)."""
    sn = str(r.get("snippet") or "").strip()
    tit = str(r.get("title") or "").strip()
    if tit:
        return f"- {tit[:cap]}: {sn[:cap]}"
    return f"- {sn[: cap * 2]}"


try:
    from engines.engine import _clean_ai_json
except ImportError:
    from engine import _clean_ai_json

_GM  = "gemini-2.0-flash"  # ← النموذج المستقر الموصى به (يدعم الرؤية)
_GV = _os.environ.get("GEMINI_VISION_MODEL", _GM)
_GU  = f"https://generativelanguage.googleapis.com/v1beta/models/{_GM}:generateContent"
_GVU = f"https://generativelanguage.googleapis.com/v1beta/models/{_GV}:generateContent"
_OR  = "https://openrouter.ai/api/v1/chat/completions"

# نماذج افتراضية — بدون معرّفات أُزيلت من الطبقة المجانية (404 No endpoints).
# يمكن تجاوزها بالكامل عبر OPENROUTER_MODELS (مفصولة بفواصل) في البيئة.
_OPENROUTER_DEFAULT_MODELS = (
    "openrouter/free",
    "google/gemma-3-27b-it:free",
    "meta-llama/llama-3.3-70b-instruct:free",
    "google/gemma-2-9b-it:free",
    "meta-llama/llama-3.2-3b-instruct:free",
)


def _openrouter_models_list():
    raw = _os.environ.get("OPENROUTER_MODELS", "").strip()
    if raw:
        return [m.strip() for m in raw.split(",") if m.strip()]
    return list(_OPENROUTER_DEFAULT_MODELS)


# توافق خلفي مع أي استيراد لاسم القائمة القديم
OPENROUTER_FALLBACK_MODELS = list(_OPENROUTER_DEFAULT_MODELS)
_CO  = "https://api.cohere.ai/v1/generate"

# Cohere اختياري — بعد 401 لا نكرر الاستدعاءات ولا نملأ سجل الأخطاء
_COHERE_KEY_INVALID = False

# ── سجل الأخطاء الأخيرة (يُعرض في صفحة التشخيص) ─────────────────────────
_LAST_ERRORS: list = []

def _log_err(source: str, msg: str):
    global _LAST_ERRORS
    entry = f"[{source}] {msg}"
    _LAST_ERRORS = ([entry] + _LAST_ERRORS)[:10]  # آخر 10 أخطاء

def get_last_errors() -> list:
    return _LAST_ERRORS.copy()

# ── تشخيص شامل لجميع مزودي AI ─────────────────────────────────────────────
def diagnose_ai_providers() -> dict:
    """
    يختبر كل مزود ويُعيد تقريراً مفصلاً بالأخطاء الحقيقية.
    يُستدعى من صفحة الإعدادات.
    """
    results = {}

    # ── Gemini ────────────────────────────────────────────────────────────
    gemini_results = []
    for i, key in enumerate(get_gemini_api_keys() or []):
        if not key:
            gemini_results.append({"key": i+1, "status": "❌ مفتاح فارغ"})
            continue
        try:
            payload = {
                "contents": [{"parts": [{"text": "test"}]}],
                "generationConfig": {"maxOutputTokens": 5}
            }
            r = requests.post(f"{_GU}?key={key}", json=payload, timeout=15)
            if r.status_code == 200:
                gemini_results.append({"key": i+1, "status": "✅ يعمل"})
            elif r.status_code == 400:
                try:
                    body = r.json()
                    msg = (body.get("error") or {}).get("message", "Bad Request")
                except _NARROW_IO:
                    msg = r.text[:100]
                gemini_results.append({"key": i+1, "status": f"❌ 400 — {msg[:80]}"})
            elif r.status_code == 403:
                gemini_results.append({"key": i+1, "status": "❌ 403 — مفتاح غير مصرح أو IP محظور"})
            elif r.status_code == 429:
                gemini_results.append({
                    "key": i+1,
                    "status": "⚠️ 429 — تجاوز الحد (انتظر أو جرّب مفتاحاً آخر / ارفع الحصة في Google AI)",
                })
            elif r.status_code == 404:
                gemini_results.append({"key": i+1, "status": f"❌ 404 — النموذج {_GM} غير متاح"})
            else:
                try:
                    body = r.json()
                    msg = (body.get("error") or {}).get("message", "")
                except _NARROW_IO:
                    msg = r.text[:100]
                gemini_results.append({"key": i+1, "status": f"❌ {r.status_code} — {msg[:80]}"})
        except requests.exceptions.ConnectionError as e:
            gemini_results.append({"key": i+1, "status": f"❌ لا يوجد اتصال بالإنترنت أو جدار حماية: {str(e)[:60]}"})
        except requests.exceptions.Timeout:
            gemini_results.append({"key": i+1, "status": "❌ انتهت المهلة (Timeout 15s)"})
        except _NARROW_IO as e:
            gemini_results.append({"key": i+1, "status": f"❌ خطأ: {str(e)[:80]}"})
    results["gemini"] = gemini_results

    # ── OpenRouter — تجربة نماذج صالحة (المعرّفات القديمة مثل google/gemini-2.0-flash تُرفض بـ 400)
    _or_key = get_openrouter_api_key()
    if _or_key:
        or_ok = False
        last_or = ""
        for _model in _openrouter_models_list():
            try:
                r = requests.post(
                    _OR,
                    json={
                        "model": _model,
                        "messages": [{"role": "user", "content": "test"}],
                        "max_tokens": 5,
                    },
                    headers={
                        "Authorization": f"Bearer {_or_key}",
                        "HTTP-Referer": "https://mahwous.com",
                    },
                    timeout=15,
                )
                if r.status_code == 200:
                    results["openrouter"] = f"✅ يعمل (نموذج: {_model})"
                    or_ok = True
                    break
                if r.status_code == 401:
                    results["openrouter"] = "❌ 401 — مفتاح OpenRouter غير صحيح"
                    or_ok = True
                    break
                if r.status_code == 402:
                    results["openrouter"] = "❌ 402 — رصيد OpenRouter منتهٍ"
                    or_ok = True
                    break
                if r.status_code == 429:
                    last_or = f"⚠️ 429 — تجاوز الحد ({_model})"
                    continue
                try:
                    body = r.json()
                    msg = (body.get("error") or {}).get("message", "")
                except _NARROW_IO:
                    msg = r.text[:100]
                msg_l = (msg or "").lower()
                if r.status_code == 404 or "no endpoints" in msg_l:
                    continue
                last_or = f"❌ {r.status_code} — {msg[:100]} ({_model})"
            except requests.exceptions.ConnectionError:
                results["openrouter"] = "❌ لا اتصال بـ openrouter.ai — قد يكون محظوراً"
                or_ok = True
                break
            except requests.exceptions.Timeout:
                last_or = f"❌ Timeout ({_model})"
            except _NARROW_IO as e:
                last_or = f"❌ {str(e)[:80]} ({_model})"
        if not or_ok:
            results["openrouter"] = last_or or (
                "❌ لا يوجد نموذج يعمل — راجع المعرفات على openrouter.ai أو OPENROUTER_MODELS"
            )
    else:
        results["openrouter"] = "⚠️ مفتاح غير موجود"

    # ── Cohere ────────────────────────────────────────────────────────────
    _ch_key = get_cohere_api_key()
    if _ch_key:
        try:
            r = requests.post("https://api.cohere.com/v2/chat", json={
                "model": "command-a-03-2025",
                "messages": [{"role": "user", "content": "test"}],
            }, headers={
                "Authorization": f"Bearer {_ch_key}",
                "Content-Type": "application/json",
            }, timeout=15)
            if r.status_code == 200:
                results["cohere"] = "✅ يعمل (command-a-03-2025)"
            elif r.status_code == 401:
                # اختياري — لا يُعرض كخطأ أحمر؛ التطبيق يعمل بدون Cohere
                results["cohere"] = (
                    "⚠️ 401 — مفتاح Cohere غير صالح (اختياري). "
                    "احذف COHERE_API_KEY من Secrets أو ضع مفتاحاً صحيحاً من dashboard.cohere.com"
                )
            elif r.status_code == 402:
                results["cohere"] = "❌ 402 — رصيد Cohere منتهٍ"
            else:
                try:
                    body = r.json()
                    msg = body.get("message", "") if isinstance(body, dict) else ""
                except _NARROW_IO:
                    msg = r.text[:100]
                results["cohere"] = f"❌ {r.status_code} — {str(msg)[:80]}"
        except requests.exceptions.ConnectionError:
            results["cohere"] = "❌ لا اتصال بـ api.cohere.com"
        except _NARROW_IO as e:
            results["cohere"] = f"❌ {str(e)[:80]}"
    else:
        results["cohere"] = "⚠️ مفتاح غير موجود"

    return results


def _vision_fetch_image(url: str):
    """تحميل صورة لـ Gemini inline_data. يعيد (bytes|None, mime)."""
    if not url or not str(url).startswith("http"):
        return None, "image/jpeg"
    try:
        r = requests.get(
            str(url).strip(),
            timeout=16,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            },
        )
        if r.status_code != 200 or not r.content:
            return None, "image/jpeg"
        ct = (r.headers.get("Content-Type") or "image/jpeg").split(";")[0].strip().lower()
        if "png" in ct:
            return r.content, "image/png"
        if "webp" in ct:
            return r.content, "image/webp"
        return r.content, "image/jpeg"
    except _NARROW_IO:
        return None, "image/jpeg"


def vision_match_court(
    our_name: str,
    comp_name: str,
    our_price: float,
    comp_price: float,
    our_img_url: str,
    comp_img_url: str,
    fuzzy_score: float,
) -> dict:
    """
    المحكمة البصرية: مقارنة صورتي المنتج عبر Gemini Vision.
    يعيد: same_product (bool), reason (str), ok (نجح الطلب).
    """
    out: dict = {"same_product": False, "reason": "", "ok": False}
    keys = get_gemini_api_keys() or []
    if not keys:
        out["reason"] = "لا يوجد مفتاح Gemini"
        return out

    b1, m1 = _vision_fetch_image(our_img_url)
    b2, m2 = _vision_fetch_image(comp_img_url)
    if not b1 or not b2:
        out["reason"] = "تعذر تحميل إحدى الصورتين"
        return out

    prompt = (
        "بصفتك خبير عطور، انظر للصورتين واقرأ النصوص التالية.\n"
        f"منتجنا: {our_name} — السعر {our_price:.2f} ر.س\n"
        f"المنافس: {comp_name} — السعر {comp_price:.2f} ر.س\n"
        f"نسبة التطابق النصي السابقة: {fuzzy_score:.1f}%\n\n"
        "الصورة الأولى لمنتجنا، الثانية لمنتج المنافس.\n"
        "هل يمثلان نفس العطر بالتجزئة (نفس الحجم ml تقريباً، نفس التركيز EDP/EDT/EDC، وليس تستر مقابل عطر كامل)؟\n"
        'أجب JSON فقط بدون شرح إضافي: {"same_product":true أو false,"reason":"سبب مختصر بالعربية"}'
    )

    parts: list = [{"text": prompt}]
    parts.append({"inline_data": {"mime_type": m1, "data": base64.b64encode(b1).decode("ascii")}})
    parts.append({"inline_data": {"mime_type": m2, "data": base64.b64encode(b2).decode("ascii")}})

    payload = {
        "contents": [{"parts": parts}],
        "generationConfig": {"temperature": 0, "maxOutputTokens": 256, "topP": 1, "topK": 1},
    }

    for key in keys:
        if not key:
            continue
        try:
            r = requests.post(f"{_GVU}?key={key}", json=payload, timeout=45)
            if r.status_code != 200:
                continue
            data = r.json()
            txt = _gemini_response_text(data)
            if not txt:
                continue
            clean = _clean_ai_json(txt)
            try:
                obj = json.loads(clean)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            out["same_product"] = bool(obj.get("same_product"))
            out["reason"] = str(obj.get("reason", "")).strip() or "—"
            out["ok"] = True
            return out
        except _NARROW_IO as e:
            _log_err("vision_match_court", str(e)[:120])
            continue

    out["reason"] = "فشل كل مفاتيح Gemini للمحكمة البصرية"
    return out



# ══ استدعاءات AI ═══════════════════════════════════════════════════════════
from engines.prompts import MAHWOUS_EXPERT_SYSTEM, MISSING_PAGE_SYSTEM, PAGE_PROMPTS
def _call_gemini(prompt, system="", grounding=False, temperature=0.3, max_tokens=8192):
    # لا نقتصّ الـ system الطويل (مثل MAHWOUS_EXPERT_SYSTEM) — فقط مدخلات المستخدم/البيانات الديناميكية
    prompt = _clip_prompt(prompt, 24000)
    full = f"{system}\n\n{prompt}" if system else prompt
    payload = {
        "contents": [{"parts": [{"text": full}]}],
        "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens, "topP": 0.85}
    }
    if grounding:
        payload["tools"] = [{"google_search": {}}]

    keys = get_gemini_api_keys()
    if not keys:
        _log_err("Gemini", "لا توجد مفاتيح API")
        return None

    for i, key in enumerate(keys):
        if not key:
            continue
        try:
            r = requests.post(f"{_GU}?key={key}", json=payload, timeout=45)
            if r.status_code == 200:
                data = r.json()
                text = _gemini_response_text(data)
                if text:
                    return text
                reason = (data.get("promptFeedback") or {}).get("blockReason", "") if isinstance(data, dict) else ""
                _log_err("Gemini", f"مفتاح {i+1}: لا نتائج — {reason}")
            elif r.status_code == 429:
                _log_err("Gemini", f"مفتاح {i+1}: Rate Limit (429) — انتظار 2 ثانية")
                time.sleep(2)  # ← 2 ثانية للـ 429
                continue
            elif r.status_code == 403:
                _log_err("Gemini", f"مفتاح {i+1}: IP محظور أو مفتاح غير مصرح (403)")
            elif r.status_code == 404:
                _log_err("Gemini", f"مفتاح {i+1}: نموذج غير متاح {_GM} (404)")
            else:
                try:
                    body = r.json()
                    msg = (body.get("error") or {}).get("message", "")
                except _NARROW_IO:
                    msg = r.text[:100]
                _log_err("Gemini", f"مفتاح {i+1}: {r.status_code} — {msg[:80]}")
        except requests.exceptions.ConnectionError as e:
            _log_err("Gemini", f"مفتاح {i+1}: لا اتصال — {str(e)[:80]}")
        except requests.exceptions.Timeout:
            _log_err("Gemini", f"مفتاح {i+1}: Timeout (45s)")
        except _NARROW_IO as e:
            _log_err("Gemini", f"مفتاح {i+1}: {str(e)[:80]}")
    return None

def _call_openrouter(prompt, system=""):
    or_key = get_openrouter_api_key()
    if not or_key:
        return None

    prompt = _clip_prompt(prompt, 24000)

    msgs = []
    if system:
        msgs.append({"role": "system", "content": system})
    msgs.append({"role": "user", "content": prompt})

    models = _openrouter_models_list()
    logged_429 = False
    for model in models:
        try:
            r = requests.post(_OR, json={
                "model": model,
                "messages": msgs,
                "temperature": 0.3,
                "max_tokens": 8192
            }, headers={
                "Authorization": f"Bearer {or_key}",
                "HTTP-Referer": "https://mahwous.com",
                "X-Title": "Mahwous"
            }, timeout=45)

            if r.status_code == 200:
                data = r.json()
                content = _openrouter_message_content(data)
                if content and content.strip():
                    return content
            elif r.status_code == 429:
                if not logged_429:
                    _log_err(
                        "OpenRouter",
                        "429 — تجاوز الحد؛ جرّب نموذجاً آخر في القائمة (بدون انتظار طويل)",
                    )
                    logged_429 = True
                # نموذج آخر قد يكون تحت حدّ مختلف — لا ننام هنا لتسريع السلسلة
                continue
            elif r.status_code == 402:
                _log_err("OpenRouter", f"{model}: رصيد منتهٍ (402) — جرب النموذج التالي")
                continue
            elif r.status_code == 401:
                _log_err("OpenRouter", "مفتاح غير صحيح (401)")
                return None  # لا فائدة من تجربة نماذج أخرى
            else:
                try:
                    body = r.json()
                    msg = (body.get("error") or {}).get("message", "")
                except _NARROW_IO:
                    msg = r.text[:100]
                msg_l = (msg or "").lower()
                if r.status_code == 404 or "no endpoints" in msg_l:
                    continue
                _log_err("OpenRouter", f"{model}: {r.status_code} — {msg[:80]}")
                continue

        except requests.exceptions.ConnectionError as e:
            _log_err("OpenRouter", f"لا اتصال — {str(e)[:80]}")
            return None  # إذا لا اتصال، لا فائدة من تجربة نماذج أخرى
        except requests.exceptions.Timeout:
            _log_err("OpenRouter", f"{model}: Timeout (45s)")
            continue
        except _NARROW_IO as e:
            _log_err("OpenRouter", f"{model}: {str(e)[:80]}")
            continue

    return None

def _call_cohere(prompt, system=""):
    """
    Cohere — Fallback صامت فقط.
    أي خطأ (401/402/429/...) يُسجَّل ويُعاد None بدون إيقاف سير العمل.
    """
    global _COHERE_KEY_INVALID
    if _COHERE_KEY_INVALID:
        return None
    ch_key = get_cohere_api_key()
    if not ch_key:
        return None
    prompt = _clip_prompt(prompt, 24000)
    try:
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        r = requests.post(
            "https://api.cohere.com/v2/chat",
            json={"model": "command-r-plus", "messages": messages, "temperature": 0.3},
            headers={"Authorization": f"Bearer {ch_key}",
                     "Content-Type": "application/json"},
            timeout=30
        )
        if r.status_code == 200:
            data = r.json()
            if not isinstance(data, dict):
                return None
            msg_obj = data.get("message")
            parts = msg_obj.get("content") if isinstance(msg_obj, dict) else None
            if isinstance(parts, list) and parts and isinstance(parts[0], dict):
                tx = parts[0].get("text")
                return tx if isinstance(tx, str) else ""
            return ""
        elif r.status_code == 401:
            _COHERE_KEY_INVALID = True
            _log_err("Cohere", "مفتاح غير صحيح (401) — تجاوز Cohere")
            return None  # ← لا يوقف العمل، يمرر للـ fallback التالي
        elif r.status_code in (402, 403):
            _log_err("Cohere", f"غير مصرح ({r.status_code}) — تجاوز")
            return None
        elif r.status_code == 429:
            _log_err("Cohere", "Rate Limit (429) — انتظار 2 ثانية")
            time.sleep(2)
            return None
        else:
            try:
                body = r.json()
                msg = body.get("message", "") if isinstance(body, dict) else ""
            except _NARROW_IO:
                msg = r.text[:100]
            _log_err("Cohere", f"{r.status_code} — {str(msg)[:80]}")
    except _NARROW_IO as e:
        _log_err("Cohere", f"Fallback صامت — {str(e)[:60]}")
    return None

def _parse_json(txt):
    if not txt:
        return None
    try:
        clean = _clean_ai_json(txt)
        return json.loads(clean)
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        _log_err("_parse_json", str(e)[:120])
    return None

def _search_ddg(query, num_results=5):
    """بحث DuckDuckGo مجاني"""
    try:
        r = requests.get(
            "https://api.duckduckgo.com/",
            params={"q": query, "format": "json", "no_html": "1", "skip_disambig": "1"},
            timeout=8,
        )
        if r.status_code == 200:
            data = r.json()
            if not isinstance(data, dict):
                return []
            results: list[dict] = []
            if data.get("AbstractText"):
                results.append(
                    {
                        "title": str(data.get("Heading") or "").strip(),
                        "snippet": data["AbstractText"],
                        "url": data.get("AbstractURL", ""),
                    }
                )
            for rel in (data.get("RelatedTopics") or [])[:num_results]:
                if isinstance(rel, dict) and rel.get("Text"):
                    results.append(
                        {
                            "title": "",
                            "snippet": rel.get("Text", ""),
                            "url": rel.get("FirstURL", ""),
                        }
                    )
            return results
    except _NARROW_IO:
        pass
    return []

def call_ai(prompt, page="general"):
    # «missing» فقط = وصف منتج مفقود كامل + SEO (البرومبت الموحّد لخبير مهووس)
    if page == "missing":
        sys = f"{MISSING_PAGE_SYSTEM}\n\nتعليمات إضافية:\n{PAGE_PROMPTS.get('missing', '')}"
    else:
        sys = PAGE_PROMPTS.get(page, PAGE_PROMPTS["general"])
    # بحث Google مع Gemini عند توليد وصف مفقود (مكونات موثوقة)
    _miss_ground = page == "missing"
    for fn, src in [
        (lambda: _call_gemini(prompt, sys, grounding=_miss_ground), "Gemini"),
        (lambda: _call_openrouter(prompt, sys), "OpenRouter"),
        (lambda: _call_cohere(prompt, sys), "Cohere")
    ]:
        r = fn()
        if r: return {"success":True,"response":r,"source":src}
    return {"success":False,"response":"فشل الاتصال بجميع مزودي AI","source":"none"}

# ══ Gemini Chat ══════════════════════════════════════════════════════════════
def gemini_chat(message, history=None, system_extra=""):
    message = _clip_prompt(message, 24000)
    system_extra = _clip_prompt(system_extra, 8000) if system_extra else ""
    sys = PAGE_PROMPTS["general"]
    if system_extra:
        sys = f"{sys}\n\nسياق: {system_extra}"
    needs_web = any(k in message.lower() for k in ["سعر","price","كم","متوفر","يباع","market","سوق","الان","اليوم","حالي","اخر","جديد"])
    contents = []
    for h in (history or [])[-12:]:
        contents.append({"role":"user","parts":[{"text":h["user"]}]})
        contents.append({"role":"model","parts":[{"text":h["ai"]}]})
    contents.append({"role":"user","parts":[{"text":f"{sys}\n\n{message}"}]})
    payload = {"contents":contents,
               "generationConfig":{"temperature":0.4,"maxOutputTokens":4096,"topP":0.9}}
    if needs_web:
        payload["tools"] = [{"google_search":{}}]
    for key in get_gemini_api_keys():
        if not key: continue
        try:
            r = requests.post(f"{_GU}?key={key}", json=payload, timeout=40)
            if r.status_code == 200:
                data = r.json()
                text = _gemini_response_text(data)
                if text:
                    return {
                        "success": True,
                        "response": text,
                        "source": "Gemini Flash" + (" + بحث ويب" if needs_web else ""),
                    }
            elif r.status_code == 429:
                time.sleep(1); continue
        except _NARROW_IO:
            continue
    r = _call_openrouter(message, sys)
    if r: return {"success":True,"response":r,"source":"OpenRouter"}
    return {"success":False,"response":"فشل الاتصال","source":"none"}

# ══ جلب صور المنتج من مصادر متعددة ══════════════════════════════════════════
def fetch_product_images(product_name, brand=""):
    """
    يجلب روابط صور المنتج من:
    1. Fragrantica Arabia (المصدر الأساسي)
    2. Google Images عبر Gemini Grounding
    3. DuckDuckGo كبديل
    يُرجع: {"images": [{"url":"...","source":"...","alt":"..."}], "fragrantica_url": "..."}
    """
    images = []
    fragrantica_url = ""

    # ── 1. Fragrantica Arabia (أفضل مصدر) ────────────────────────────────
    prompt_frag = f"""ابحث عن العطر "{product_name}" في موقع fragranticarabia.com وابحث أيضاً في fragrantica.com

أريد فقط:
1. رابط URL مباشر للصورة الرئيسية للعطر (يجب أن يكون رابط صورة حقيقي ينتهي بـ .jpg أو .png أو .webp)
2. روابط صور إضافية إذا وجدت (2-3 صور)
3. رابط صفحة المنتج على Fragrantica Arabia

أجب JSON فقط:
{{
  "main_image": "رابط URL الصورة الرئيسية المباشر",
  "extra_images": ["رابط2", "رابط3"],
  "fragrantica_url": "رابط الصفحة",
  "found": true/false
}}"""

    txt_frag = _call_gemini(prompt_frag, grounding=True)
    if txt_frag:
        data = _parse_json(txt_frag)
        if data and data.get("found") and data.get("main_image"):
            main = data["main_image"]
            if main and main.startswith("http") and any(ext in main.lower() for ext in [".jpg",".png",".webp",".jpeg"]):
                images.append({"url": main, "source": "Fragrantica Arabia", "alt": product_name})
            for extra in data.get("extra_images", []):
                if extra and extra.startswith("http") and len(images) < 4:
                    images.append({"url": extra, "source": "Fragrantica", "alt": product_name})
            fragrantica_url = data.get("fragrantica_url", "")

    # ── 2. Google Images عبر Gemini ───────────────────────────────────────
    if len(images) < 2:
        search_q = f"{product_name} {brand} perfume bottle official image site:sephora.com OR site:nocibé.fr OR site:parfumdreams.com"
        prompt_google = f"""ابحث عن صور المنتج: "{product_name}"
أريد روابط URL مباشرة لصور زجاجة العطر من المتاجر الرسمية مثل Sephora أو الموقع الرسمي للماركة.
الروابط يجب أن تنتهي بـ .jpg أو .png أو .webp وتكون صور حقيقية للمنتج.
أجب JSON: {{"images": ["رابط1","رابط2","رابط3"], "sources": ["مصدر1","مصدر2","مصدر3"]}}"""

        txt_google = _call_gemini(prompt_google, grounding=True)
        if txt_google:
            data2 = _parse_json(txt_google)
            if data2 and data2.get("images"):
                sources = data2.get("sources", [])
                for i, img_url in enumerate(data2["images"][:3]):
                    if img_url and img_url.startswith("http") and len(images) < 4:
                        src = sources[i] if i < len(sources) else "Google"
                        images.append({"url": img_url, "source": src, "alt": product_name})

    # ── 3. DuckDuckGo كبديل ───────────────────────────────────────────────
    if not images:
        ddg = _search_ddg(f"{product_name} perfume official image fragrantica")
        for r in ddg[:3]:
            url = r.get("url","")
            if url and any(ext in url.lower() for ext in [".jpg",".png",".webp"]):
                images.append({"url": url, "source": "DuckDuckGo", "alt": product_name})
                if len(images) >= 2: break

    # ── إذا لم نجد صور مباشرة، نُعيد رابط بحث ──────────────────────────
    if not images:
        search_url = f"https://www.fragranticarabia.com/?s={requests.utils.quote(product_name)}"
        images.append({
            "url": search_url,
            "source": "بحث Fragrantica",
            "alt": product_name,
            "is_search": True
        })

    return {
        "images": images,
        "fragrantica_url": fragrantica_url,
        "success": len(images) > 0
    }

# ══ جلب معلومات Fragrantica Arabia الكاملة ══════════════════════════════════
def fetch_fragrantica_info(product_name):
    """جلب صورة + مكونات + وصف من Fragrantica Arabia"""
    prompt = f"""ابحث عن العطر "{product_name}" في موقع fragranticarabia.com

احتاج:
1. رابط صورة المنتج المباشر (.jpg/.png/.webp)
2. مكونات العطر (top notes, middle notes, base notes)
3. وصف قصير بالعربية
4. الماركة والنوع (EDP/EDT) والحجم
5. رابط الصفحة

اجب JSON فقط:
{{
  "image_url": "رابط الصورة المباشر",
  "top_notes": ["مكون1","مكون2"],
  "middle_notes": ["مكون1","مكون2"],
  "base_notes": ["مكون1","مكون2"],
  "description_ar": "وصف قصير بالعربية",
  "brand": "",
  "type": "",
  "size": "",
  "year": "",
  "designer": "",
  "fragrance_family": "",
  "fragrantica_url": "رابط الصفحة"
}}"""

    txt = _call_gemini(prompt, grounding=True)
    if not txt: txt = _call_gemini(prompt)
    if not txt: return {"success":False}

    data = _parse_json(txt)
    if data: return {"success":True, **data}
    return {"success":False,"description_ar":txt[:200] if txt else ""}

# ══ خبير وصف مهووس الكامل (مع SEO + GEO) ══════════════════════════════════
def generate_mahwous_description(product_name, price, fragrantica_data=None, extra_info=None):
    """
    يولّد وصفاً احترافياً كاملاً بنظام خبير مهووس:
    - 1200-1500 كلمة
    - 9 أقسام: مقدمة + تفاصيل + هرم عطري + لماذا + متى/أين + لمسة خبير + FAQ + روابط + خاتمة
    - SEO محسّن + GEO محسّن
    - أسلوب مهووس: راقٍ + ودود + عاطفي + تسويقي
    """
    # جمع المعلومات المتاحة
    frag_info = ""
    if fragrantica_data and fragrantica_data.get("success"):
        top    = ", ".join(fragrantica_data.get("top_notes",[])[:5])
        mid    = ", ".join(fragrantica_data.get("middle_notes",[])[:5])
        base   = ", ".join(fragrantica_data.get("base_notes",[])[:5])
        desc   = fragrantica_data.get("description_ar","")
        brand  = fragrantica_data.get("brand","")
        ptype  = fragrantica_data.get("type","")
        size   = fragrantica_data.get("size","")
        year   = fragrantica_data.get("year","")
        designer = fragrantica_data.get("designer","")
        family = fragrantica_data.get("fragrance_family","")
        frag_url = fragrantica_data.get("fragrantica_url","")

        frag_info = f"""
معلومات من Fragrantica Arabia:
- الماركة: {brand}
- المصمم: {designer}
- سنة الإصدار: {year}
- العائلة العطرية: {family}
- الحجم: {size}
- التركيز: {ptype}
- النفحات العليا: {top}
- النفحات الوسطى: {mid}
- النفحات الأساسية: {base}
- الوصف: {desc}
- رابط Fragrantica: {frag_url}"""

    extra = ""
    if extra_info:
        extra = f"\nمعلومات إضافية: {extra_info}"

    prompt = f"""اكتب وصفاً احترافياً كاملاً لهذا العطر بتنسيق متجر مهووس:

**اسم المنتج:** {product_name}
**السعر:** {price:.0f} ريال سعودي
{frag_info}{extra}

اكتب وصفاً من 1200-1500 كلمة يتضمن الأقسام التسعة التالية:

## [عنوان المنتج — الكلمة الرئيسية الكاملة]

[فقرة افتتاحية عاطفية قوية — الكلمة الرئيسية في أول 50 كلمة — دعوة مبكرة للشراء]

## تفاصيل المنتج
[نقاط نقطية: الماركة، المصمم، الجنس، العائلة العطرية، الحجم، التركيز، سنة الإصدار]

## رحلة العطر: اكتشف الهرم العطري الفاخر
[النفحات العليا + الوسطى + الأساسية — وصف حسي عاطفي، ليس مجرد قائمة]

## لماذا تختار عطر {product_name}؟
[4-6 نقاط تبدأ بـ **كلمة مفتاحية بولد** — فوائد لا ميزات]

## متى وأين ترتدي هذا العطر؟
[الفصول + الأوقات المثالية + المناسبات + الفئة العمرية]

## لمسة خبير من مهووس: تقييمنا الاحترافي
[تحليل حسي بضمير "نحن" + الأداء بالساعات + مقارنات + توصية + نصيحة عملية]

## الأسئلة الشائعة حول عطر {product_name}
[6-8 أسئلة حوارية — كل سؤال = كلمة مفتاحية — إجابة 50-80 كلمة]

## اكتشف المزيد من عطور مهووس
[3-5 روابط داخلية + رابط Fragrantica Arabia]

## عالمك العطري يبدأ من مهووس
[الكلمة الرئيسية مرتين + تعزيز الثقة + دعوة قوية للشراء]

**ملاحظات مهمة:**
- لا تستخدم الإيموجي
- استخدم **Bold** للكلمات المفتاحية
- الكلمة الرئيسية 5-7 مرات في المجموع
- أسلوب: راقٍ + ودود + عاطفي + تسويقي
- اكتب الوصف مباشرة بدون أي شرح أو تعليمات"""

    # Gemini أولاً (مع Grounding إذا أمكن لجلب معلومات إضافية)
    txt = _call_gemini(prompt, MAHWOUS_EXPERT_SYSTEM, grounding=not bool(frag_info), max_tokens=8192)
    if not txt:
        txt = _call_gemini(prompt, MAHWOUS_EXPERT_SYSTEM, grounding=False, max_tokens=8192)
    if not txt:
        txt = _call_openrouter(prompt, MAHWOUS_EXPERT_SYSTEM)
    if not txt:
        txt = _call_cohere(prompt, MAHWOUS_EXPERT_SYSTEM)

    if txt:
        return txt
    return f"## {product_name}\n\nعطر فاخر من الدرجة الأولى متوفر الآن في مهووس.\n\n**السعر:** {price:.0f} ريال سعودي\n\nعالمك العطري يبدأ من مهووس!"

# ══ تحقق منتج + تحديد القسم الصحيح ════════════════════════════════════════
def verify_match(p1, p2, pr1=0, pr2=0):
    from utils.decision_labels import ui_decision_from_verify_section
    p1 = _clip_prompt(p1, 12000)
    p2 = _clip_prompt(p2, 12000)
    diff = pr1 - pr2 if pr1 > 0 and pr2 > 0 else 0
    if pr1 > 0 and pr2 > 0:
        if diff > 10:     expected = "سعر اعلى"
        elif diff < -10:  expected = "سعر اقل"
        else:             expected = "موافق"
    else:
        expected = "تحت المراجعة"

    prompt = f"""تحقق من تطابق هذين المنتجين بدقة متناهية (99.9%):
منتج 1 (مهووس): {p1} | السعر: {pr1:.0f} ريال
منتج 2 (المنافس): {p2} | السعر: {pr2:.0f} ريال

قواعد المطابقة الصارمة:
1. يجب أن تكون الماركة متطابقة تماماً.
2. يجب أن يكون اسم العطر متطابقاً (مثلاً: Sauvage ليس Sauvage Elixir).
3. يجب أن يكون الحجم متطابقاً (مثلاً: 100ml ليس 50ml).
4. يجب أن يكون التركيز متطابقاً (EDP ليس EDT).
5. يجب أن يكون الجنس متطابقاً (Men ليس Women).

إذا كانت كل الشروط أعلاه متوفرة، أجب بـ:
- القسم الصحيح = {expected}
خلاف ذلك، أجب بـ:
- القسم الصحيح = مفقود"""

    sys = PAGE_PROMPTS["verify"]
    txt = _call_gemini(prompt, sys, temperature=0.1) or _call_openrouter(prompt, sys)
    if not txt:
        return {"success":False,"match":False,"confidence":0,"reason":"فشل AI","correct_section":"تحت المراجعة","suggested_price":0,
                "ui_decision": ui_decision_from_verify_section("تحت المراجعة", False)}
    data = _parse_json(txt)
    if data:
        sec = data.get("correct_section","")
        if "اعلى" in sec or "أعلى" in sec: data["correct_section"] = "سعر اعلى"
        elif "اقل" in sec or "أقل" in sec:  data["correct_section"] = "سعر اقل"
        elif "موافق" in sec:                 data["correct_section"] = "موافق"
        elif "مفقود" in sec:                 data["correct_section"] = "مفقود"
        else: data["correct_section"] = expected if data.get("match") else "مفقود"
        data["ui_decision"] = ui_decision_from_verify_section(data.get("correct_section", ""), bool(data.get("match")))
        return {"success":True, **data}
    match = "true" in txt.lower() or "نعم" in txt
    cs = expected if match else "مفقود"
    return {"success":True,"match":match,"confidence":65,"reason":txt[:200],"correct_section":cs,"suggested_price":0,
            "ui_decision": ui_decision_from_verify_section(cs, match)}

# ══ إعادة تصنيف قسم "تحت المراجعة" ════════════════════════════════════════
def reclassify_review_items(items):
    if not items: return []
    lines = []
    for i, it in enumerate(items):
        diff = it.get("our_price",0) - it.get("comp_price",0)
        lines.append(f"[{i+1}] منتجنا: {it['our']} ({it.get('our_price',0):.0f}ر.س)"
                     f" vs منافس: {it['comp']} ({it.get('comp_price',0):.0f}ر.س) | فرق: {diff:+.0f}ر.س")
    prompt = f"""حلل هذه المنتجات وحدد القسم الصحيح لكل منها:
{chr(10).join(lines)}
- سعر اعلى: نفس المنتج + سعرنا اعلى بـ10+ ريال
- سعر اقل: نفس المنتج + سعرنا اقل بـ10+ ريال
- موافق: نفس المنتج + فرق 10 ريال او اقل
- مفقود: ليسا نفس المنتج"""
    sys = PAGE_PROMPTS["reclassify"]
    txt = _call_gemini(prompt, sys, temperature=0.1) or _call_openrouter(prompt, sys)
    if not txt: return []
    data = _parse_json(txt)
    if data and "results" in data:
        for r in data["results"]:
            sec = r.get("section","")
            if "اعلى" in sec or "أعلى" in sec: r["section"] = "🔴 سعر أعلى"
            elif "اقل" in sec or "أقل" in sec:  r["section"] = "🟢 سعر أقل"
            elif "موافق" in sec:                 r["section"] = "✅ موافق"
            elif "مفقود" in sec:                 r["section"] = "🔵 مفقود"
            else:                                 r["section"] = "⚠️ تحت المراجعة"
            try:
                r["confidence"] = float(r.get("confidence") or 0)
            except (TypeError, ValueError):
                r["confidence"] = 0.0
        return data["results"]
    return []


def apply_gemini_reclassify_to_analysis_df(analysis_df, min_confidence: float = 75.0, batch_size: int = 30):
    """
    بعد المطابقة: يمرّ على صفوف «⚠️ تحت المراجعة» ويطلب من Gemini إعادة التصنيف على دفعات.
    يحدّث عمود «القرار» في نفس DataFrame عند ثقة كافية (مثل زر المراجعة اليدوي).
    """
    if analysis_df is None or getattr(analysis_df, "empty", True):
        return analysis_df
    if "القرار" not in analysis_df.columns:
        return analysis_df
    try:
        mask = analysis_df["القرار"].astype(str).str.contains("مراجعة", na=False)
    except (TypeError, ValueError, KeyError, AttributeError):
        return analysis_df
    row_indices = analysis_df.index[mask].tolist()
    if not row_indices:
        return analysis_df

    for start in range(0, len(row_indices), batch_size):
        chunk_idx = row_indices[start : start + batch_size]
        items = []
        for ri in chunk_idx:
            r = analysis_df.loc[ri]
            try:
                op = float(r.get("السعر", 0) or 0)
            except (TypeError, ValueError):
                op = 0.0
            try:
                cp = float(r.get("سعر_المنافس", 0) or 0)
            except (TypeError, ValueError):
                cp = 0.0
            items.append({
                "our": str(r.get("المنتج", "")),
                "comp": str(r.get("منتج_المنافس", "")),
                "our_price": op,
                "comp_price": cp,
            })
        try:
            results = reclassify_review_items(items)
        except _NARROW_IO:
            continue
        if not results:
            continue
        by_idx = {}
        for res in results:
            ix = res.get("idx")
            if ix is None:
                continue
            try:
                ix = int(ix)
            except (TypeError, ValueError):
                continue
            by_idx[ix] = res
        for pos, ri in enumerate(chunk_idx, start=1):
            res = by_idx.get(pos)
            if not res:
                continue
            sec = str(res.get("section", "") or "")
            try:
                conf = float(res.get("confidence", 0) or 0)
            except (TypeError, ValueError):
                conf = 0.0
            if sec and "مراجعة" not in sec and conf >= min_confidence:
                analysis_df.at[ri, "القرار"] = sec
    return analysis_df

# ══ بحث أسعار السوق ══════════════════════════════════════════════════════
def search_market_price(product_name, our_price=0):
    # البحث في أشهر المتاجر السعودية (سلة، زد، نايس ون، قولدن سنت، خبير العطور)
    try:
        op = float(our_price or 0)
    except (TypeError, ValueError):
        op = 0.0
    try:
        product_name = _clip_prompt(product_name, 4000)
        queries = [
            f"سعر {product_name} السعودية نايس ون قولدن سنت سلة",
            f"سعر {product_name} في المتاجر السعودية 2026",
            f"مقارنة أسعار {product_name} السعودية",
            f"{product_name} price Saudi Arabia perfume shop",
        ]
        all_results = []
        for q in queries:  # العربية + الإنجليزي (كان [:3] يستبعد الاستعلام الأخير)
            ddg = _search_ddg(q)
            if ddg:
                all_results.extend(ddg[:3])

        web_ctx = "\n".join(_ddg_result_line(r, 120) for r in all_results) if all_results else ""

        prompt = f"""تحليل سوق دقيق للمنتج في السعودية (مارس 2026):
المنتج: {product_name}
سعرنا الحالي: {op:.0f} ريال

المعلومات المستخرجة من الويب:
{web_ctx}

المطلوب تحليل JSON مفصل:
1. متوسط السعر في السوق السعودي.
2. أرخص سعر متاح حالياً واسم المتجر.
3. قائمة المنافسين المباشرين وأسعارهم (نايس ون، قولدن سنت، لودوريه، بيوتي ستور، إلخ).
4. حالة التوفر (متوفر/غير متوفر).
5. توصية تسعير ذكية لمتجر مهووس ليكون الأكثر تنافسية.
6. نسبة الثقة في البيانات (0-100)."""
        sys = PAGE_PROMPTS["market_search"]
        txt = _call_gemini(prompt, sys, grounding=True)
        if not txt: txt = _call_gemini(prompt, sys)
        if not txt: txt = _call_openrouter(prompt, sys)
        if not txt: return {"success": False, "market_price": 0}
        data = _parse_json(txt)
        if data:
            data["web_context"] = web_ctx
            return {"success": True, **data}
        return {
            "success": True,
            "market_price": op,
            "recommendation": txt[:400],
            "web_context": web_ctx,
        }
    except (requests.exceptions.RequestException, ValueError, TypeError, KeyError):
        return {"success": False, "market_price": 0}

# ══ تحليل عميق ══════════════════════════════════════════════════════════════
def ai_deep_analysis(our_product, our_price, comp_product, comp_price, section="general", brand=""):
    diff = our_price - comp_price if our_price > 0 and comp_price > 0 else 0
    diff_pct = (abs(diff)/comp_price*100) if comp_price > 0 else 0
    ddg = _search_ddg(f"سعر {our_product} السعودية")
    web_ctx = "\n".join(f"- {r['snippet'][:80]}" for r in ddg[:2]) if ddg else ""
    guidance = {
        "🔴 سعر أعلى": f"سعرنا اعلى بـ{diff:.0f}ريال ({diff_pct:.1f}%). هل يجب خفضه؟",
        "🟢 سعر أقل":  f"سعرنا اقل بـ{abs(diff):.0f}ريال ({diff_pct:.1f}%). كم يمكن رفعه؟",
        "✅ موافق":     "السعر تنافسي. هل نحافظ عليه؟",
        "⚠️ تحت المراجعة": "المطابقة غير مؤكدة. هل هما نفس المنتج؟",
    }.get(section, "")
    prompt = f"""تحليل تسعير عميق:
منتجنا: {our_product} | سعرنا: {our_price:.0f} ريال
المنافس: {comp_product} | سعره: {comp_price:.0f} ريال
الفرق: {diff:+.0f} ريال | {diff_pct:.1f}% | {guidance}
{f"معلومات السوق:{chr(10)}{web_ctx}" if web_ctx else ""}
اجب بتقرير مختصر: هل المطابقة صحيحة؟ السعر المقترح بالرقم؟ الاجراء الفوري؟"""
    txt = _call_gemini(prompt, grounding=bool(web_ctx)) or _call_openrouter(prompt)
    if txt: return {"success":True,"response":txt,"source":"Gemini" + (" + ويب" if web_ctx else "")}
    return {"success":False,"response":"فشل التحليل"}

# ══ بحث mahwous.com ══════════════════════════════════════════════════════════
def search_mahwous(product_name):
    ddg = _search_ddg(f"site:mahwous.com {product_name}")
    web_ctx = "\n".join(r["snippet"][:100] for r in ddg[:2]) if ddg else ""
    prompt = f"""هل العطر {product_name} متوفر في متجر مهووس؟
{f"نتائج:{chr(10)}{web_ctx}" if web_ctx else ""}
اجب JSON: {{"likely_available":true/false,"confidence":0-100,"similar_products":[],
"add_recommendation":"عالية/متوسطة/منخفضة","reason":"","suggested_price":0}}"""
    txt = _call_gemini(prompt, grounding=True) or _call_gemini(prompt)
    if not txt: return {"success":False}
    data = _parse_json(txt)
    if data: return {"success":True, **data}
    return {"success":True,"likely_available":False,"confidence":50,"reason":txt[:150]}

# ══ تحقق مكرر ════════════════════════════════════════════════════════════════
def check_duplicate(product_name, our_products):
    if not our_products: return {"success":True,"response":"لا توجد بيانات"}
    prompt = f"""هل العطر {product_name} موجود بشكل مشابه في هذه القائمة؟
القائمة: {', '.join(str(p) for p in our_products[:30])}
اجب: نعم (وذكر اقرب مطابقة) او لا مع السبب."""
    return call_ai(prompt, "general")

# ══ وصف مفقود — خبير مهووس (Gemini + البرومبت الموحّد) ═══════════════════════
def generate_missing_product_description(
    product_name: str,
    *,
    brand: str = "",
    size_concentration: str = "",
    competitor_price: float = 0.0,
    competitor_name: str = "",
    extra_context: str = "",
) -> dict:
    """
    يولّد وصفاً كاملاً + JSON SEO عبر call_ai(page='missing') وMISSING_PAGE_SYSTEM.
    """
    product_name = _clip_prompt(product_name, 4000)
    brand = _clip_prompt(brand, 2000)
    size_concentration = _clip_prompt(size_concentration, 2000)
    competitor_name = _clip_prompt(competitor_name, 2000)
    extra_context = _clip_prompt(extra_context, 8000)
    prompt = f"""اكتب وصف المنتج المفقود التالي وفق النظام والهيكل المحددين.

**الاسم:** {product_name}
**الماركة:** {brand or "غير محدد"}
**الحجم والتركيز:** {size_concentration or "استخرجه من الاسم إن أمكن"}
**سعر مرجعي (منافس):** {competitor_price:.2f} ر.س إن وُجد
**اسم المنافس:** {competitor_name or "—"}
{f"**ملاحظات إضافية:** {extra_context}" if extra_context.strip() else ""}
"""
    return call_ai(prompt, "missing")

# ══ تحليل مجمع ════════════════════════════════════════════════════════════════
def bulk_verify(items, section="general"):
    if not items: return {"success":False,"response":"لا توجد منتجات"}
    lines = "\n".join(
        f"{i+1}. {it.get('our','')} vs {it.get('comp','')} | "
        f"سعرنا: {it.get('our_price',0):.0f} | منافس: {it.get('comp_price',0):.0f} | "
        f"فرق: {it.get('our_price',0)-it.get('comp_price',0):+.0f}"
        for i,it in enumerate(items))
    instructions = {
        "price_raise": "سعرنا اعلى. لكل منتج: هل المطابقة صحيحة؟ هل نخفض؟ السعر المقترح.",
        "price_lower": "سعرنا اقل = ربح ضائع. لكل منتج: هل يمكن رفعه؟ السعر الامثل.",
        "review": "مطابقات غير مؤكدة. لكل منتج: هل هما نفس العطر فعلا؟ نعم/لا/غير متاكد.",
        "approved": "منتجات موافق عليها. راجعها وتاكد انها لا تزال تنافسية.",
    }
    prompt = f"{instructions.get(section,'حلل واعط توصية.')}\n\nالمنتجات:\n{lines}"
    return call_ai(prompt, section)

# ══ معالجة النص الملصوق ═══════════════════════════════════════════════════
def analyze_paste(text, context=""):
    text = _clip_prompt(text, 24000)
    context = _clip_prompt(context, 4000)
    prompt = f"""المستخدم لصق هذا النص:
---
{text}
---
{f"سياق إضافي: {context}" if context.strip() else ""}
حلل واستخرج: قائمة منتجات؟ اسعار؟ اوامر؟ اعط توصيات مفيدة. اجب بالعربية منظم."""
    return call_ai(prompt, "general")

# ══ دوال متوافقة مع app.py ════════════════════════════════════════════════
def chat_with_ai(msg, history=None, ctx=""): return gemini_chat(msg, history, ctx)
def analyze_product(p, price=0): return call_ai(f"حلل: {p} ({price:.0f}ريال)", "general")
def suggest_price(p, comp_price): return call_ai(f"اقترح سعرا لـ {p} بدلا من {comp_price:.0f}ريال", "general")
def process_paste(text): return analyze_paste(text)
