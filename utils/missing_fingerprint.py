"""بصمة جدول المفقودات لتتبّع تغيّر العرض بعد تجهيز ملف سلة."""
from __future__ import annotations

import hashlib
import logging

import pandas as pd

_logger = logging.getLogger(__name__)


def missing_df_fingerprint(edf: pd.DataFrame) -> str:
    """بصمة آمنة لا تستهلك الذاكرة عند التعامل مع 100,000+ صف."""
    if edf is None or edf.empty:
        return "0"
    try:
        return f"{edf.shape[0]}_{hashlib.md5(str(edf.head(10)).encode()).hexdigest()}"
    except Exception as e:
        _logger.error("missing_df_fingerprint failed: %s", e, exc_info=True)
        return str(edf.shape[0])
