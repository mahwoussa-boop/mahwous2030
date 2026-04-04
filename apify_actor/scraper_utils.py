import asyncio
import json
import logging
import re
import xml.etree.ElementTree as ET
from html import unescape
from typing import Any
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

def _is_ld_product_group_node(node: dict) -> bool:
    raw = node.get("@type")
    parts = raw if isinstance(raw, list) else [raw]
    for p in parts:
        if p is None:
            continue
        s = str(p)
        if re.search(r"(^|/|#)ProductGroup$", s, re.I):
            return True
        if str(p).strip().lower() == "productgroup":
            return True
    return False

def _is_ld_product_node(node: dict) -> bool:
    raw = node.get("@type")
    parts = raw if isinstance(raw, list) else [raw]
    for p in parts:
        if p is None:
            continue
        s = str(p)
        if re.search(r"(^|/|#)Product$", s, re.I):
            return True
        if str(p).strip().lower() == "product":
            return True
    return False

def _ld_pick_first_image(val: Any) -> str | None:
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        return s if s else None
    if isinstance(val, dict):
        u = val.get("url") or val.get("contentUrl") or val.get("contentURL")
        if isinstance(u, str) and u.strip():
            return u.strip()
        if isinstance(u, list) and u:
            return _ld_pick_first_image(u[0])
        nested = val.get("image")
        if nested is not None and nested is not val:
            got = _ld_pick_first_image(nested)
            if got:
                return got
        uid = val.get("@id")
        if isinstance(uid, str) and uid.startswith("http"):
            return uid.strip()
        return None
    if isinstance(val, list):
        for x in val:
            got = _ld_pick_first_image(x)
            if got:
                return got
    return None

def _iter_ld_product_dicts(node: Any, out: list[dict[str, Any]]) -> None:
    if isinstance(node, dict):
        if _is_ld_product_group_node(node):
            for sub in node.get("hasVariant") or []:
                _iter_ld_product_dicts(sub, out)
        elif _is_ld_product_node(node):
            out.append(node)
        g = node.get("@graph")
        if isinstance(g, list):
            for x in g:
                _iter_ld_product_dicts(x, out)
    elif isinstance(node, list):
        for x in node:
            _iter_ld_product_dicts(x, out)

def _extract_from_json_ld(html: str, page_url: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    fallback_img: str | None = None
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.I | re.DOTALL,
    ):
        raw = m.group(1).strip()
        try:
            data = json.loads(raw)
        except Exception:
            logger.warning(
                "JSON-LD script parse failed (step=json_ld_loads skip block) page_url=%s raw_len=%s",
                page_url or "",
                len(raw),
                exc_info=True,
            )
            continue
        products: list[dict[str, Any]] = []
        _iter_ld_product_dicts(data, products)
        if not products and page_url:
            logger.debug("JSON-LD found but no Product nodes detected: %s", page_url)
        for it in products:
            name = it.get("name")
            if isinstance(name, str) and name.strip():
                out.setdefault("name", unescape(name.strip()))
            img = _ld_pick_first_image(it.get("image"))
            if img:
                fallback_img = fallback_img or img
                out.setdefault("image", img)
            offers = it.get("offers")
            if isinstance(offers, dict):
                p = offers.get("price") or offers.get("lowPrice")
                if p is not None:
                    try:
                        out.setdefault(
                            "price",
                            float(str(p).replace(",", "").replace("\u00a0", "")),
                        )
                    except Exception:
                        logger.warning(
                            "JSON-LD offer price parse failed (step=json_ld_offer_dict) page_url=%s p=%r",
                            page_url or "",
                            p,
                            exc_info=True,
                        )
            elif isinstance(offers, list) and offers:
                o0 = offers[0]
                if isinstance(o0, dict):
                    p = o0.get("price") or o0.get("lowPrice")
                    if p is not None:
                        try:
                            out.setdefault(
                                "price",
                                float(str(p).replace(",", "").replace("\u00a0", "")),
                            )
                        except Exception:
                            logger.warning(
                                "JSON-LD list offer price parse failed (step=json_ld_offer_list) page_url=%s p=%r",
                                page_url or "",
                                p,
                                exc_info=True,
                            )
    if not out.get("image") and fallback_img:
        out["image"] = fallback_img
    
    if out.get("name") and not out.get("price") and page_url:
        logger.info("JSON-LD extraction: Found name '%s' but NO price at %s", out["name"], page_url)
    elif out.get("name") and out.get("price"):
        logger.debug("JSON-LD extraction success: %s (%.2f) at %s", out["name"], out["price"], page_url)
        
    return out

def _extract_meta_fallback(html: str, page_url: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    m = re.search(
        r'<meta\s+property=["\']og:title["\']\s+content=["\']([^"\']+)["\']',
        html,
        re.I,
    )
    if m:
        out["name"] = unescape(m.group(1))
    for pat in (
        r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
        r'<meta\s+content=["\']([^"\']+)["\']\s+property=["\']og:image["\']',
        r'<meta\s+property=["\']og:image:secure_url["\']\s+content=["\']([^"\']+)["\']',
        r'<meta\s+content=["\']([^"\']+)["\']\s+property=["\']og:image:secure_url["\']',
        r'<meta\s+name=["\']twitter:image["\']\s+content=["\']([^"\']+)["\']',
        r'<meta\s+name=["\']twitter:image:src["\']\s+content=["\']([^"\']+)["\']',
    ):
        m = re.search(pat, html, re.I)
        if m:
            out["image"] = m.group(1).strip()
            break
    if not out.get("image"):
        m = re.search(
            r'<link[^>]+rel=["\']image_src["\'][^>]+href=["\']([^"\']+)["\']',
            html,
            re.I,
        )
        if m:
            out["image"] = m.group(1).strip()
    for pat in (
        r'"price"\s*:\s*([\d.]+)',
        r'itemprop=["\']price["\']\s+content=["\']([\d.]+)["\']',
        r'data-price=["\']([\d.]+)["\']',
    ):
        m = re.search(pat, html, re.I)
        if m:
            try:
                out["price"] = float(m.group(1))
                break
            except Exception:
                logger.warning(
                    "meta fallback regex price parse failed (step=meta_price_regex) page_url=%s m=%r",
                    page_url or "",
                    m.group(0)[:80] if m else None,
                    exc_info=True,
                )
    return out

def _absolutize_image_url(page_url: str, img: str | None) -> str | None:
    if not img:
        return None
    u = str(img).strip()
    if not u or u.lower() in ("none", "null", "undefined"):
        return None
    if u.startswith("//"):
        return "https:" + u
    if u.startswith(("http://", "https://")):
        return u
    try:
        return urljoin(page_url, u)
    except Exception:
        logger.exception(
            "absolutize image urljoin failed page_url=%r img=%r",
            page_url,
            (u[:120] + "...") if len(u) > 120 else u,
        )
        return u

_NON_PRODUCT_URL_TOKENS = (
    "/privacy", "/policy", "/policies", "/terms", "/shipping", "/returns",
    "/refund", "/contact", "/about", "/faq", "/blog", "/track", "/cart",
    "/checkout", "/account", "/login", "/register", "/wishlist",
    "/category/", "/categories/", "/brand/", "/brands/", "/tag/", "/tags/", 
    "/pages/", "/collections/", "/collection/", "/author/", "/users/",
    "/?currency=", "/ar/", "/en/"
)
_NON_PRODUCT_NAME_TOKENS = (
    "سياسة", "الخصوصية", "الشحن", "التوصيل", "الشروط", "الاحكام",
    "طرق الدفع", "الاستبدال", "الاسترجاع", "اتصل بنا", "من نحن",
    "المدونة", "تتبع الطلب", "الأسئلة الشائعة",
    "privacy", "policy", "terms", "shipping", "returns", "refund",
    "contact us", "about us", "blog", "faq",
)

def _product_url_heuristic(url: str) -> bool:
    if "#has_img_guarantee" in url:
        return True
    try:
        path = urlparse(url).path
    except Exception:
        logger.exception("urlparse failed for product URL heuristic url=%r", url)
        path = ""
    pl = path.rstrip("/")
    if re.search(r"/p\\d+$", pl, re.I):
        return True
    u = url.lower()
    if any(tok in u for tok in _NON_PRODUCT_URL_TOKENS):
        return False
    if any(x in u for x in ("/product/", "/products/", "/item/", "/perfume")):
        return True
    if "عطر" in u and "/c" not in u:
        return True
    if re.search(r"/[^/]+-\\d{3,}", u):
        return True
    return False

def _looks_non_product_name(name: str) -> bool:
    n = (name or "").strip().lower()
    if not n:
        return True
    return any(tok in n for tok in _NON_PRODUCT_NAME_TOKENS)

def _strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag

def _parse_sitemap_xml(content: bytes) -> tuple[list[str], bool]:
    urls: list[str] = []
    is_index = False
    try:
        root = ET.fromstring(content)
    except Exception:
        logger.exception(
            "sitemap XML parse failed (ElementTree) content_len=%s",
            len(content) if content else 0,
        )
        return [], False
    root_tag = _strip_ns(root.tag).lower()
    if root_tag == "sitemapindex":
        is_index = True
        
    if not is_index:
        for url_node in root.findall(".//*"):
            t_url = _strip_ns(url_node.tag).lower()
            if t_url == "url":
                loc_text = None
                has_image = False
                for child in url_node.iter():
                    t_child = _strip_ns(child.tag).lower()
                    if t_child == "loc" and child.text and loc_text is None:
                        loc_text = child.text.strip()
                    elif t_child == "image":
                        has_image = True
                if loc_text:
                    if has_image:
                        urls.append(loc_text + "#has_img_guarantee")
                    else:
                        urls.append(loc_text)
        if not urls:
            for el in root.iter():
                t = _strip_ns(el.tag).lower()
                if t == "loc" and el.text:
                    urls.append(el.text.strip())
    else:
        for el in root.iter():
            t = _strip_ns(el.tag).lower()
            if t == "loc" and el.text:
                urls.append(el.text.strip())
    return urls, is_index

async def _async_jitter_sleep() -> None:
    await asyncio.sleep(random.uniform(0.5, 1.5))

async def _async_backoff_sleep(attempt: int) -> None:
    base = min(5.0 * (2.0 ** attempt), 60.0)
    jitter = random.uniform(0.5, 2.5)
    await asyncio.sleep(base + jitter)

async def _expand_sitemap_to_page_urls_async(
    fetcher: Any,
    start_url: str,
) -> list[str]:
    page_urls: list[str] = []
    seen_sm: set[str] = set()
    q: list[str] = [start_url]
    
    _SITEMAP_LOC_CAP = 200000
    _MAX_SITEMAP_INDEX_ENTRIES = 200000
    _MAX_SITEMAP_BYTES = 32 * 1024 * 1024
    _SITEMAP_EXPAND_TIMEOUT_SEC = 600

    t0 = asyncio.get_event_loop().time()
    while q and len(page_urls) < _SITEMAP_LOC_CAP:
        if _SITEMAP_EXPAND_TIMEOUT_SEC > 0 and (asyncio.get_event_loop().time() - t0) > _SITEMAP_EXPAND_TIMEOUT_SEC:
            break
        sm_url = q.pop(0)
        if sm_url in seen_sm:
            continue
        if len(seen_sm) >= _MAX_SITEMAP_INDEX_ENTRIES:
            continue
        seen_sm.add(sm_url)
        
        await _async_jitter_sleep()
        got = await fetcher.get_text_armored(sm_url, timeout=30.0)
        if got is None:
            continue
        _code, body = got
        if _code != 200 or not body:
            continue
        raw = body.encode("utf-8", errors="replace")
        if len(raw) > _MAX_SITEMAP_BYTES:
            continue
        locs, is_index = _parse_sitemap_xml(raw)
        if is_index:
            q_set = set(q)
            for loc in locs:
                if (
                    loc.startswith("http")
                    and loc not in seen_sm
                    and loc not in q_set
                ):
                    q.append(loc)
                    q_set.add(loc)
        else:
            for loc in locs:
                if loc.startswith("http"):
                    page_urls.append(loc.strip())
                    if len(page_urls) >= _SITEMAP_LOC_CAP:
                        break
    return page_urls

async def scrape_page_and_extract_product(fetcher: Any, page_url: str) -> dict[str, Any] | None:
    got = await fetcher.get_text_armored(page_url, timeout=22.0)
    if got is None:
        return None
    _code, html = got
    if _code != 200 or not html:
        return None
    data = _extract_from_json_ld(html, page_url=page_url)
    fb = _extract_meta_fallback(html, page_url=page_url)
    if not data.get("name"):
        data.update({k: v for k, v in fb.items() if v is not None})
    if not data.get("name"):
        return None
    if data.get("price") is None and fb.get("price") is not None:
        data["price"] = fb["price"]
    if not data.get("image") and fb.get("image"):
        data["image"] = fb["image"]
    if data.get("image"):
        abs_u = _absolutize_image_url(page_url, data.get("image"))
        if abs_u:
            data["image"] = abs_u
    data["url"] = page_url

    # Basic validation for product data
    if not data.get("name") or not data.get("price") or float(data["price"]) <= 0:
        logger.warning(f"Skipping product from {page_url} due to missing name or invalid price.")
        return None

    return data
