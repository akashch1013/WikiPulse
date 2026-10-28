import random
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from importlib.metadata import version
from typing import Any

import httpx

from wikipulse_common.config import HttpSettings
from wikipulse_common.telemetry import HttpMetrics

RETRYABLE_STATUS = frozenset({429, 502, 503, 504})
# 429 and 503 are the statuses Wikimedia's rate limiter and overload protection send; without a
# Retry-After header the documented minimum wait applies.
RATE_LIMIT_STATUS = frozenset({429, 503})


class RetriesExhausted(RuntimeError):
    pass


class ApiError(RuntimeError):
    def __init__(self, code: str, info: str) -> None:
        super().__init__(f"{code}: {info}")
        self.code = code


def user_agent(settings: HttpSettings) -> str:
    if not settings.contact.strip():
        raise ValueError("http.contact is empty; set WIKIPULSE_HTTP__CONTACT (Wikimedia User-Agent policy)")
    package_version = version("wikipulse-common")
    return f"{settings.product}/{package_version} ({settings.contact}) python-httpx/{httpx.__version__}"


def parse_retry_after(value: str) -> float | None:
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, (when - datetime.now(UTC)).total_seconds())


class TokenBucket:
    def __init__(
        self,
        per_minute: int,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.rate = per_minute / 60.0
        self.capacity = max(1.0, self.rate)
        self.tokens = self.capacity
        self.updated = clock()
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()

    def acquire(self) -> float:
        # Reserve a token under the lock and sleep outside it; a negative balance makes later callers
        # queue behind earlier ones instead of all waking at once.
        with self._lock:
            now = self._clock()
            self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
            self.updated = now
            self.tokens -= 1
            wait = max(0.0, -self.tokens / self.rate)
        if wait:
            self._sleep(wait)
        return wait


class WikimediaClient:
    def __init__(
        self,
        settings: HttpSettings,
        *,
        metrics: HttpMetrics | None = None,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self.settings = settings
        self._metrics = metrics
        self._sleep = sleep
        self._rng = rng or random.Random()
        self._client = httpx.Client(
            headers={"User-Agent": user_agent(settings), "Accept-Encoding": "gzip"},
            timeout=settings.timeout_seconds,
            follow_redirects=True,
            transport=transport,
        )
        self._buckets = {
            host: TokenBucket(limit, clock, sleep) for host, limit in settings.rate_limits_per_minute.items()
        }
        self._slots = {host: threading.BoundedSemaphore(n) for host, n in settings.max_connections.items()}

    def __enter__(self) -> "WikimediaClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def get_json(self, url: str, params: dict[str, str] | None = None) -> Any:
        host = httpx.URL(url).host
        with self._slot(host):
            response = self._send(url, self._with_api_defaults(url, params), stream=False)
        payload = response.json()
        if isinstance(payload, dict) and "error" in payload:
            error = payload["error"]
            raise ApiError(error.get("code", "unknown"), error.get("info", ""))
        return payload

    @contextmanager
    def stream(self, url: str, params: dict[str, str] | None = None) -> Iterator[httpx.Response]:
        host = httpx.URL(url).host
        with self._slot(host):
            response = self._send(url, params, stream=True)
            try:
                yield response
            finally:
                response.close()

    def _with_api_defaults(self, url: str, params: dict[str, str] | None) -> dict[str, str] | None:
        if not httpx.URL(url).path.endswith("/w/api.php"):
            return params
        defaults = {"format": "json", "formatversion": "2", "maxlag": str(self.settings.maxlag_seconds)}
        return {**defaults, **(params or {})}

    def _slot(self, host: str) -> threading.BoundedSemaphore | nullcontext[None]:
        return self._slots.get(host) or nullcontext()

    def _send(self, url: str, params: dict[str, str] | None, *, stream: bool) -> httpx.Response:
        host = httpx.URL(url).host
        if host not in self._buckets:
            raise ValueError(f"no rate limit configured for {host}; add it to http.rate_limits_per_minute")
        request = self._client.build_request("GET", url, params=params)
        last_error = ""
        for attempt in range(1, self.settings.max_attempts + 1):
            self._record_wait(host, self._buckets[host].acquire())
            try:
                response = self._client.send(request, stream=stream)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                delay = self._backoff(attempt)
            else:
                if self._metrics:
                    self._metrics.record_response(host, response.status_code)
                maxlag = response.headers.get("MediaWiki-API-Error") == "maxlag"
                if response.status_code not in RETRYABLE_STATUS and not maxlag:
                    if response.is_error:
                        response.close()
                    response.raise_for_status()
                    return response
                last_error = "maxlag" if maxlag else f"HTTP {response.status_code}"
                delay = self._retry_delay(response, attempt, rate_limited=maxlag)
                response.close()
            if attempt < self.settings.max_attempts:
                self._sleep(delay)
                self._record_wait(host, delay)
        raise RetriesExhausted(f"GET {url} failed after {self.settings.max_attempts} attempts: {last_error}")

    def _retry_delay(self, response: httpx.Response, attempt: int, *, rate_limited: bool) -> float:
        floor = self.settings.min_retry_after_seconds
        header = response.headers.get("Retry-After")
        parsed = parse_retry_after(header) if header else None
        if parsed is not None:
            return max(parsed, floor)
        if rate_limited or response.status_code in RATE_LIMIT_STATUS:
            return max(self._backoff(attempt), floor)
        return self._backoff(attempt)

    def _backoff(self, attempt: int) -> float:
        ceiling = min(
            self.settings.backoff_max_seconds, self.settings.backoff_base_seconds * 2 ** (attempt - 1)
        )
        return self._rng.uniform(0, ceiling)

    def _record_wait(self, host: str, seconds: float) -> None:
        if self._metrics:
            self._metrics.record_wait(host, seconds)
