import asyncio
import logging
import random
import time
from collections import deque
from typing import Any

import httpx
from curl_cffi import requests as curl_requests

logger = logging.getLogger(__name__)

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]

_RECENT_HTTP_STATUS = deque(maxlen=10)

class AsyncScraperHTTP:
    def __init__(self):
        self._client = None
        self._curl_client = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(http2=True, follow_redirects=True)
        try:
            self._curl_client = curl_requests.AsyncSession(impersonate="chrome120")
        except Exception as e:
            logger.warning(f"Failed to initialize curl_cffi AsyncSession, falling back to httpx: {e}")
            self._curl_client = None
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()
        if self._curl_client:
            await self._curl_client.close()

    async def _async_jitter_sleep(self) -> None:
        await asyncio.sleep(random.uniform(0.5, 1.5))

    async def _async_backoff_sleep(self, attempt: int) -> None:
        base = min(5.0 * (2.0 ** attempt), 60.0)
        jitter = random.uniform(0.5, 2.5)
        await asyncio.sleep(base + jitter)

    async def get_text_armored(self, url: str, timeout: float = 25.0, max_attempts: int = 6) -> tuple[int, str] | None:
        last_status = 0
        for attempt in range(max_attempts):
            await self._async_jitter_sleep()
            try:
                headers = {"User-Agent": random.choice(_USER_AGENTS)}
                if self._curl_client:
                    response = await self._curl_client.get(url, timeout=timeout, headers=headers, allow_redirects=True)
                else:
                    response = await self._client.get(url, timeout=timeout, headers=headers)
                
                last_status = response.status_code
                _RECENT_HTTP_STATUS.append(last_status)

                if last_status in (429, 403, 503, 502, 500, 504):
                    await self._async_backoff_sleep(attempt)
                    continue
                if last_status == 200 and response.text:
                    return 200, response.text
                if last_status == 200 and not response.text:
                    await self._async_backoff_sleep(attempt)
                    continue
                if last_status > 0:
                    await self._async_backoff_sleep(attempt)
                    continue
                await self._async_backoff_sleep(attempt)
            except Exception:
                logger.warning(
                    "async HTTP GET failed url=%s attempt=%s/%s",
                    url[:200],
                    attempt + 1,
                    max_attempts,
                    exc_info=True,
                )
                _RECENT_HTTP_STATUS.append(0)
                await self._async_backoff_sleep(attempt)
        return None

