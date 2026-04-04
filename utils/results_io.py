"""
تحويل نتائج التحليل لـ JSON آمن للتخزين واستعادتها.
"""
from __future__ import annotations

import json
import logging

import pandas as pd

_logger = logging.getLogger(__name__)


def safe_results_for_json(results_list):
    """تحويل النتائج لصيغة آمنة للحفظ في JSON/SQLite — يحول القوائم المتداخلة"""
    safe = []
    for r in results_list:
        row = {}
        for k, v in (r.items() if isinstance(r, dict) else {}):
            if isinstance(v, list):
                try:
                    row[k] = json.dumps(v, ensure_ascii=False, default=str)
                except Exception as e:
                    _logger.warning(
                        "safe_results_for_json: json.dumps failed for key=%r: %s",
                        k,
                        e,
                        exc_info=True,
                    )
                    row[k] = str(v)
            elif pd.isna(v) if isinstance(v, float) else False:
                row[k] = 0
            else:
                row[k] = v
        safe.append(row)
    return safe


def export_results_dataframe_csv(
    df: pd.DataFrame, filepath: str, *, index: bool = False
) -> None:
    """تصدير CSV بترميز utf-8-sig لعرض العربية بشكل صحيح في Excel على Windows."""
    df.to_csv(filepath, encoding="utf-8-sig", index=index)


def results_df_to_csv_bytes(
    df: pd.DataFrame, *, index: bool = False, encoding: str = "utf-8-sig"
) -> bytes:
    """بايتات CSV بنفس الترميز (BOM) لتفادي فساد الأحرف في Excel."""
    return df.to_csv(index=index, encoding=encoding).encode(encoding)


def restore_results_from_json(results_list):
    """استعادة النتائج من JSON — يحول نصوص القوائم لقوائم فعلية"""
    restored = []
    for r in results_list:
        row = dict(r) if isinstance(r, dict) else {}
        for k in ["جميع_المنافسين", "جميع المنافسين"]:
            v = row.get(k)
            if isinstance(v, str):
                try:
                    row[k] = json.loads(v)
                except Exception as e:
                    _logger.debug(
                        "restore_results_from_json: json.loads failed for key=%r: %s",
                        k,
                        e,
                        exc_info=True,
                    )
                    row[k] = []
            elif v is None:
                row[k] = []
        restored.append(row)
    return restored
