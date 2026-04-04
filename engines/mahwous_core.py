"""
نواة فلاتر الأتمتة الصارمة — متوافقة مع سلة / تصدير Make.
يُستورد من محرك الحاجز الذكي ومن مدقق الإرسال النهائي.
"""
from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

_log = logging.getLogger(__name__)

try:
    from config import REJECT_KEYWORDS
except ImportError:
    REJECT_KEYWORDS = [
        "sample", "عينة", "عينه", "decant", "تقسيم", "split", "miniature",
        "مينياتشر", "travel size", "travel", "fial", "vial",
    ]

# كلمات إضافية للحاجز قبل قسم المفقودات
_EXTRA_BANNED = (
    "tester", "تستر", "تيستر", "عينة مجانية", "مجانية", "free sample",
    "سمبل", "vial",
)

# إكسسوارات — استبعاد فوري من قائمة المفقودات
_ACCESSORY_BANNED = (
    "حقيبة",
    "مِرشة",
    "مرشة",
    "علبة فارغة",
    "كيس حماية",
    "travel case",
    "empty box",
    "dust bag",
    "pouch only",
)

# حجم عينة: < 8 مل (أو ما يعادلها تقريباً في الأونصة) — يدعم 1.5ml وغيره
_ML_SAMPLE_RE = re.compile(
    r"(?P<n>\d+(?:\.\d+)?)\s*(ml|مل|ملي|milliliter)\b",
    re.IGNORECASE,
)
_OZ_SMALL_RE = re.compile(
    r"(?P<n>\d+(?:[.,]\d+)?)\s*(oz|أونصة|ounce)\b",
    re.IGNORECASE,
)


def _phrase_is_ascii_only(s: str) -> bool:
    return bool(s) and all(ord(c) < 128 for c in s)


def _has_banned_phrase(text: str, phrase: str) -> bool:
    """
    كشف عبارة ممنوعة دون إيجابيات خاطئة شائعة للعبارات ASCII فقط (حدود كلمات \\w).
    للعربية والنصوص غير ASCII يُستخدم البحث الجزئي كالسابق.
    """
    if not phrase or not text:
        return False
    p = str(phrase).strip()
    if not p:
        return False
    hay = str(text).lower()
    pl = p.lower()
    if _phrase_is_ascii_only(pl):
        try:
            pat = r"(?<!\w)" + re.escape(pl) + r"(?!\w)"
            return re.search(pat, hay, re.IGNORECASE) is not None
        except re.error:
            return pl in hay
    return pl in hay


def _volume_indicates_sample_size(text: str) -> bool:
    """يُستبعد إن وُجد في الاسم حجم < 8 مل (عينة)، أو أونصة صغيرة جداً."""
    if not text:
        return False
    s = str(text)
    for m in _ML_SAMPLE_RE.finditer(s):
        try:
            v = float(m.group("n").replace(",", "."))
            if 0 < v < 8.0:
                return True
        except (TypeError, ValueError):
            continue
    for m in _OZ_SMALL_RE.finditer(s):
        try:
            oz = float(m.group("n").replace(",", "."))
            if 0 < oz <= 0.07:
                return True
        except (TypeError, ValueError):
            continue
    return False


def _combined_name_text(row: pd.Series, name_col: str, desc_col: str | None) -> str:
    parts = [str(row.get(name_col, "") or "")]
    if desc_col and desc_col in row.index:
        parts.append(str(row.get(desc_col, "") or ""))
    return " ".join(parts)


def product_has_volume_hint(text: str) -> bool:
    """يتحقق من وجود حجم (ml / oz / مل) في النص."""
    if not text or not str(text).strip():
        return False
    t = str(text).lower()
    if _ML_SAMPLE_RE.search(t) or _OZ_SMALL_RE.search(t):
        return True
    if re.search(r"\b\d+\s*(مل|ملي)\b", t):
        return True
    return False


def tag_missing_volume_status(
    df: pd.DataFrame,
    name_col: str = "منتج_المنافس",
    desc_col: str | None = None,
) -> pd.DataFrame:
    """
    يضيف عمود حالة_البيانات: «كامل» إن وُجد حجم في الاسم أو الوصف، وإلا «بيانات ناقصة».
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    if desc_col and desc_col not in out.columns:
        desc_col = None
    if name_col not in out.columns:
        for alt in ("منتج_المنافس", "المنتج", "اسم المنتج", "Product", "name"):
            if alt in out.columns:
                name_col = alt
                break
    statuses = []
    for _, row in out.iterrows():
        blob = _combined_name_text(row, name_col, desc_col)
        statuses.append("كامل" if product_has_volume_hint(blob) else "بيانات ناقصة")
    out["حالة_البيانات"] = statuses
    return out


def apply_strict_pipeline_filters(
    df: pd.DataFrame,
    name_col: str = "منتج_المنافس",
    desc_col: str | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    فلترة صارمة: عينات (≤8مل + كلمات)، إكسسوارات، كلمات ممنوعة.
    يعيد (dataframe_مفلتر، تقرير).
    """
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame(), {"dropped": 0}

    work = df.copy()
    col = name_col
    if col not in work.columns:
        for alt in ("منتج_المنافس", "المنتج", "اسم المنتج", "Product", "name"):
            if alt in work.columns:
                col = alt
                break
        else:
            return work, {"dropped": 0, "warning": "no_name_column"}

    dcol = desc_col if (desc_col and desc_col in work.columns) else None

    n0 = len(work)
    mask = pd.Series(True, index=work.index)

    def row_text(i) -> str:
        r = work.loc[i]
        return _combined_name_text(r, col, dcol)

    for kw in list(REJECT_KEYWORDS) + list(_EXTRA_BANNED):
        if not kw:
            continue
        mask &= ~work.index.to_series().map(lambda i: _has_banned_phrase(row_text(i), kw))

    for kw in _ACCESSORY_BANNED:
        if not kw:
            continue
        mask &= ~work.index.to_series().map(lambda i: _has_banned_phrase(row_text(i), kw))

    mask &= ~work.index.to_series().map(lambda i: _volume_indicates_sample_size(row_text(i)))

    filtered = work[mask].copy()
    dropped = n0 - len(filtered)
    return filtered, {
        "dropped": int(dropped),
        "name_col_used": col,
        "desc_col_used": dcol or "",
    }


# قيمة افتراضية لاستيراد سلة عند تعذّر استنتاج الماركة من الاسم
_EXPORT_BRAND_FALLBACK = "غير محدد"
_BRAND_COL_CANDS = ("الماركة", "brand", "Brand", "العلامة")


def _row_brand_explicit(row: pd.Series) -> str:
    """أول قيمة صالحة من أعمدة الماركة المعروفة."""
    for key in _BRAND_COL_CANDS:
        try:
            b = str(row.get(key, "") or "").strip()
        except Exception as e:
            _log.debug("_row_brand_explicit skip key=%r: %s", key, e, exc_info=True)
            continue
        if b and b.lower() not in ("nan", "none", "—", "-"):
            return b
    return ""


def _infer_brand_from_name(product_name: str, known_brands: list[str]) -> str:
    """يطابق أطول علامة من القائمة تظهر في اسم المنتج (حروف غير حساسة)."""
    if not product_name or not str(product_name).strip():
        return ""
    low = str(product_name).lower()
    best = ""
    brands_sorted = sorted(
        (str(b).strip() for b in known_brands if b and str(b).strip()),
        key=len,
        reverse=True,
    )
    for bs in brands_sorted:
        if len(bs) < 2:
            continue
        if bs.lower() in low:
            if len(bs) > len(best):
                best = bs
    return best


def _effective_brand_for_export(row: pd.Series, known_brands: list[str]) -> str:
    explicit = _row_brand_explicit(row)
    if explicit:
        return explicit
    name = ""
    for nc in ("منتج_المنافس", "المنتج", "اسم المنتج"):
        try:
            n = str(row.get(nc, "") or "").strip()
        except Exception as e:
            _log.debug("_effective_brand_for_export name col %r failed: %s", nc, e, exc_info=True)
            n = ""
        if n:
            name = n
            break
    inferred = _infer_brand_from_name(name, known_brands)
    return inferred if inferred else _EXPORT_BRAND_FALLBACK


def ensure_export_brands(df: pd.DataFrame) -> pd.DataFrame:
    """
    يضمن وجود عمود «الماركة» غير فارغ للتصدير إلى سلة / Make.
    يملأ من أعمدة بديلة، أو مطابقة اسم المنتج مع KNOWN_BRANDS، ثم «غير محدد».
    """
    if df is None or df.empty:
        return df
    try:
        from config import KNOWN_BRANDS
    except ImportError:
        KNOWN_BRANDS = []
    out = df.copy()
    if "الماركة" not in out.columns:
        out["الماركة"] = ""
    kb = list(KNOWN_BRANDS) if isinstance(KNOWN_BRANDS, (list, tuple)) else []
    out["الماركة"] = out.apply(lambda r: _effective_brand_for_export(r, kb), axis=1)
    return out


def validate_export_product_dataframe(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """
    مدقق التصدير قبل Make / CSV — اسم، سعر، ماركة، تاريخ YYYY-MM-DD عند وجوده.
    يعيد (سليم؟، قائمة مشاكل).
    """
    issues: list[str] = []
    if df is None or df.empty:
        return False, ["لا توجد بيانات للتصدير أو الإرسال"]

    date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    for i, (_, row) in enumerate(df.iterrows()):
        name = (
            str(row.get("منتج_المنافس", "") or "").strip()
            or str(row.get("المنتج", "") or "").strip()
            or str(row.get("اسم المنتج", "") or "").strip()
        )
        if not name or name.lower() in ("nan", "none", "—", "-"):
            issues.append(f"صف {i + 1}: اسم المنتج فارغ أو غير صالح")
            continue

        if len(name) > 500:
            issues.append(f"صف {i + 1}: الاسم يتجاوز 500 حرفاً (سلة)")

        brand = str(row.get("الماركة", "") or "").strip()
        if not brand or brand.lower() in ("nan", "none", "—", "-"):
            issues.append(f"صف {i + 1}: الماركة مطلوبة للتصدير")

        raw_p = row.get("سعر_المنافس", row.get("السعر", row.get("price", 0)))
        try:
            p = float(str(raw_p).replace(",", "").replace("ر.س", "").strip())
            if p <= 0:
                issues.append(f"صف {i + 1}: السعر يجب أن يكون أكبر من صفر (القيمة: {raw_p})")
        except (TypeError, ValueError):
            issues.append(f"صف {i + 1}: السعر غير رقمي ({raw_p})")

        raw_date = row.get("تاريخ_الرصد", row.get("تاريخ", ""))
        if raw_date is not None and str(raw_date).strip():
            if hasattr(raw_date, "strftime"):
                ds = raw_date.strftime("%Y-%m-%d")
            else:
                ds = str(raw_date).strip()
            if not date_re.match(ds):
                issues.append(
                    f"صف {i + 1}: التاريخ يجب أن يكون بصيغة YYYY-MM-DD (القيمة: {raw_date})"
                )

        if len(issues) >= 100:
            issues.append("... توقف جمع المشاكل عند 100 بند — راجع الدفعة يدوياً")
            break

    return (len(issues) == 0, issues)
