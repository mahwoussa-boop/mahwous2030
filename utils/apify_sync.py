"""
مزامنة مخرجات Apify (ممثل المهووس) مع جدول comp_catalog — تلقائية أو يدوية.
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from config import (
    apify_auto_import_state_path,
    get_apify_competitor_label,
    get_apify_default_actor_id,
    get_apify_token,
)
from utils.apify_helper import (
    fetch_dataset_items,
    get_actor_run,
    get_latest_succeeded_run,
)
from utils.db_manager import upsert_comp_catalog

logger = logging.getLogger(__name__)

_APIFY_RUN_POLL_TIMEOUT_SEC = 300.0
_APIFY_RUN_POLL_INTERVAL_SEC = 3.0


def _read_state() -> dict[str, Any]:
    p = apify_auto_import_state_path()
    if not os.path.isfile(p):
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, dict) else {}
    except Exception as e:
        logger.error("apify _read_state failed path=%s: %s", p, e, exc_info=True)
        return {}


def _write_state(last_run_id: str, row_count: int) -> None:
    p = apify_auto_import_state_path()
    payload = {
        "last_run_id": last_run_id,
        "row_count": row_count,
        "last_import_at": datetime.now(timezone.utc).isoformat(),
    }
    d = os.path.dirname(p) or "."
    try:
        os.makedirs(d, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=d, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, p)
        except Exception:
            try:
                if os.path.isfile(tmp_path):
                    os.unlink(tmp_path)
            except OSError as oe:
                logger.debug("apify _write_state tmp cleanup failed: %s", oe, exc_info=True)
            raise
    except OSError as e:
        logger.warning("apify state write failed: %s", e)


def wait_for_apify_run_terminal(
    token: str,
    run_id: str,
    *,
    timeout_sec: float = _APIFY_RUN_POLL_TIMEOUT_SEC,
    poll_interval_sec: float = _APIFY_RUN_POLL_INTERVAL_SEC,
) -> dict[str, Any]:
    """
    يستطلع حالة تشغيل الممثل حتى تنتهي (نجاح/فشل/إلغاء) أو يُرفع TimeoutError.
    """
    t0 = time.time()
    rid = (run_id or "").strip()
    tok = (token or "").strip()
    if not rid or not tok:
        raise ValueError("run_id and token are required")
    while True:
        if time.time() - t0 > timeout_sec:
            raise TimeoutError(
                f"Apify run {rid} did not reach a terminal status within {timeout_sec}s"
            )
        data = get_actor_run(tok, rid)
        status = ""
        if isinstance(data, dict):
            status = str(data.get("status") or "").strip().upper()
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT", "TIMED_OUT"):
            return data
        time.sleep(max(0.5, poll_interval_sec))


def _parse_price(val: Any) -> float:
    if val is None:
        return 0.0
    s = str(val).strip().replace(",", "")
    if not s:
        return 0.0
    m = re.search(r"[-+]?\d*\.?\d+", s)
    if not m:
        return 0.0
    try:
        return float(m.group(0))
    except ValueError:
        return 0.0


def _field(item: dict[str, Any], *candidates: str) -> str:
    for c in candidates:
        if c in item and item[c] is not None:
            return str(item[c]).strip()
    low = {str(k).strip().lower(): k for k in item}
    for c in candidates:
        k = low.get(c.lower().replace(" ", "_"))
        if k is None:
            k = low.get(c.lower())
        if k is not None and item[k] is not None:
            return str(item[k]).strip()
    return ""


def apify_items_to_competitor_df(items: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        name = _field(it, "Name", "name", "اسم المنتج")
        if not name or len(name) < 2:
            continue
        url = _field(it, "Product URL", "ProductURL", "product_url", "url", "link")
        price = _parse_price(_field(it, "Price", "price", "السعر") or 0)
        img = _field(it, "Image URL", "ImageURL", "image_url", "image", "رابط_الصورة")
        rows.append(
            {
                "اسم المنتج": name,
                "السعر": price,
                "رابط_الصورة": img,
                "معرف": (url or "")[:500],
            }
        )
    return pd.DataFrame(rows)


def sync_apify_catalog_from_cloud(
    *,
    force: bool = False,
    token: str | None = None,
    actor_id: str | None = None,
    competitor_label: str | None = None,
) -> dict[str, Any]:
    """
    يجلب أحدث تشغيل ناجح للممثل ويُحدّث comp_catalog.
    """
    tok = (token or get_apify_token()).strip()
    aid = (actor_id or get_apify_default_actor_id()).strip().replace("/", "~")
    label = (competitor_label or get_apify_competitor_label()).strip() or "Apify"
    out: dict[str, Any] = {
        "ok": False,
        "skipped": True,
        "reason": "",
        "run_id": "",
        "rows": 0,
        "error": "",
    }
    if not tok or not aid:
        out["reason"] = "no_token_or_actor"
        return out
    try:
        run = get_latest_succeeded_run(tok, aid)
    except Exception as e:
        out["error"] = str(e)[:300]
        out["reason"] = "api_list_runs"
        return out
    if not run:
        out["reason"] = "no_succeeded_run"
        return out
    rid = str(run.get("id") or "").strip()
    ds = str(run.get("defaultDatasetId") or "").strip()
    if not rid:
        out["reason"] = "run_without_id"
        return out
    if not ds:
        out["reason"] = "no_dataset"
        return out
    st_data = _read_state()
    prev = str(st_data.get("last_run_id") or "").strip()
    if not force and prev == rid:
        out["skipped"] = True
        out["reason"] = "already_imported"
        out["run_id"] = rid
        return out
    try:
        # تقليص الحد الأقصى لمنع قنبلة الذاكرة (OOM Kill) عند تحويل البيانات إلى Pandas
        items = fetch_dataset_items(tok, ds, limit=4000)
    except Exception as e:
        out["error"] = str(e)[:300]
        out["reason"] = "fetch_dataset"
        out["run_id"] = rid
        return out
    df = apify_items_to_competitor_df(items)
    n = len(df)
    if n == 0:
        out["skipped"] = True
        out["reason"] = "empty_dataset"
        out["run_id"] = rid
        return out
    try:
        upsert_comp_catalog({label: df})
    except Exception as e:
        out["error"] = str(e)[:300]
        out["reason"] = "upsert_db"
        out["run_id"] = rid
        return out
    _write_state(rid, n)
    out["ok"] = True
    out["skipped"] = False
    out["run_id"] = rid
    out["rows"] = n
    out["reason"] = "imported"
    return out


def try_apify_auto_import_sidebar() -> None:
    """للشريط الجانبي: يفعّل مع APIFY_AUTO_IMPORT وحد أدنى 90 ثانية بين المحاولات."""
    import streamlit as st

    from config import get_apify_auto_import

    if not get_apify_auto_import():
        return
    tok = get_apify_token().strip()
    aid = get_apify_default_actor_id().strip()
    if not tok or not aid:
        return
    now = time.monotonic()
    prev = st.session_state.get("_apify_auto_import_monotonic")
    if prev is not None and (now - prev) < 90.0:
        return
    st.session_state["_apify_auto_import_monotonic"] = now
    try:
        res = sync_apify_catalog_from_cloud(force=False)
    except Exception as e:
        logger.error("apify auto-import on startup failed: %s", e, exc_info=True)
        return
    if res.get("ok") and int(res.get("rows") or 0) > 0:
        rid = str(res.get("run_id") or "")
        st.toast(
            f"🎭 Apify: دُمج {res['rows']} منتجًا في المنافس «{get_apify_competitor_label()}» "
            f"(تشغيل {rid[:12]}…)",
            icon="✅",
        )
