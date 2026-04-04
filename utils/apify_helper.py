"""
ربط Apify REST — جلب نتائج dataset والتحقق من الرمز دون تخزينه في الكود.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import requests

APIFY_V2 = "https://api.apify.com/v2"
TIMEOUT = 60
_STARTURLS_MAX_PER_PAYLOAD = 2000

logger = logging.getLogger(__name__)


def validate_token(token: str) -> tuple[bool, str]:
    """GET /users/me — يعيد (نجاح، رسالة قصيرة)."""
    t = (token or "").strip()
    if not t:
        return False, "لا يوجد رمز (APIFY_TOKEN)"
    try:
        r = requests.get(
            f"{APIFY_V2}/users/me",
            params={"token": t},
            timeout=min(TIMEOUT, 20),
        )
        if r.status_code == 200:
            return True, "متصل"
        if r.status_code in (401, 403):
            return False, "مرفوض (تحقق من الرمز)"
        return False, f"HTTP {r.status_code}"
    except requests.RequestException as e:
        return False, str(e)[:120]


def get_latest_succeeded_run(token: str, actor_id: str) -> dict[str, Any] | None:
    """أحدث تشغيل بحالة SUCCEEDED للممثل (أول عنصر عند ترتيب تنازلي)."""
    t = (token or "").strip()
    aid = (actor_id or "").strip().replace("/", "~")
    if not t or not aid:
        return None
    r = requests.get(
        f"{APIFY_V2}/acts/{aid}/runs",
        params={
            "token": t,
            "status": "SUCCEEDED",
            "limit": 1,
            "desc": "true",
        },
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    body = r.json()
    data = body.get("data") if isinstance(body, dict) else None
    items = data.get("items") if isinstance(data, dict) else None
    if not items or not isinstance(items, list):
        return None
    first = items[0]
    return first if isinstance(first, dict) else None


def get_actor_run(token: str, run_id: str) -> dict[str, Any]:
    """تفاصيل تشغيل — يتضمن defaultDatasetId عند النجاح."""
    t = (token or "").strip()
    rid = (run_id or "").strip()
    if not t or not rid:
        raise ValueError("run_id والرمز مطلوبان")
    r = requests.get(
        f"{APIFY_V2}/actor-runs/{rid}",
        params={"token": t},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    return data.get("data") if isinstance(data, dict) else data


def fetch_dataset_items(
    token: str,
    dataset_id: str,
    *,
    limit: int = 1000,
    offset: int = 0,
    clean: bool = True,
) -> list[dict[str, Any]]:
    """عناصر dataset افتراضي لأي تشغيل."""
    t = (token or "").strip()
    ds = (dataset_id or "").strip()
    if not t or not ds:
        raise ValueError("dataset_id والرمز مطلوبان")
    params: dict[str, Any] = {"token": t, "limit": limit, "offset": offset}
    if clean:
        params["clean"] = 1
    r = requests.get(
        f"{APIFY_V2}/datasets/{ds}/items",
        params=params,
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    out = r.json()
    if not isinstance(out, list):
        return []
    return [x for x in out if isinstance(x, dict)]


def _post_single_actor_run(
    token: str,
    actor_id: str,
    run_input: dict[str, Any],
    *,
    memory_mbytes: int | None = None,
) -> dict[str, Any]:
    t = (token or "").strip()
    aid = (actor_id or "").strip().replace("/", "~")
    if not t or not aid:
        raise ValueError("actor_id والرمز مطلوبان")
    url = f"{APIFY_V2}/acts/{aid}/runs"
    params: dict[str, Any] = {"token": t}
    if memory_mbytes is not None and memory_mbytes > 0:
        params["memory"] = memory_mbytes
    r = requests.post(
        url,
        params=params,
        data=json.dumps(run_input),
        headers={"Content-Type": "application/json; charset=utf-8"},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    return data.get("data") if isinstance(data, dict) else data


def start_actor_run(
    token: str,
    actor_id: str,
    run_input: dict[str, Any],
    *,
    memory_mbytes: int | None = None,
) -> dict[str, Any]:
    """
    يبدأ تشغيل ممثل. actor_id بالصيغة username~actorName.
    run_input يُرسل كجسم JSON (INPUT في Apify).
    قوائم startUrls الطويلة تُجزّأ لتفادي HTTP 413 (Payload Too Large).
    """
    ri = dict(run_input or {})
    urls = ri.get("startUrls")
    if isinstance(urls, list) and len(urls) > _STARTURLS_MAX_PER_PAYLOAD:
        last: dict[str, Any] | None = None
        n = len(urls)
        for i in range(0, n, _STARTURLS_MAX_PER_PAYLOAD):
            chunk = dict(ri)
            chunk["startUrls"] = urls[i : i + _STARTURLS_MAX_PER_PAYLOAD]
            logger.info(
                "Apify startUrls chunk %s–%s of %s",
                i,
                min(i + _STARTURLS_MAX_PER_PAYLOAD, n) - 1,
                n,
            )
            last = _post_single_actor_run(
                token, actor_id, chunk, memory_mbytes=memory_mbytes
            )
        return last if last is not None else {}
    return _post_single_actor_run(
        token, actor_id, ri, memory_mbytes=memory_mbytes
    )
