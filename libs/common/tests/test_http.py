import hashlib
import json
import re
import threading
import time
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from wikipulse_common.config import HttpSettings
from wikipulse_common.http import ApiError, RetriesExhausted, TokenBucket, WikimediaClient
from wikipulse_common.telemetry import HttpMetrics

STREAM_BODY = bytes(range(256)) * 12_000  # about 3 MB


class ScriptedServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), ScriptedHandler)
        self.scripts: dict[str, list[tuple[int, dict[str, str], bytes]]] = {}
        self.requests: list[tuple[str, dict[str, list[str]], dict[str, str]]] = []
        self.slow_first: set[str] = set()

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"


class ScriptedHandler(BaseHTTPRequestHandler):
    server: ScriptedServer

    def do_GET(self) -> None:
        parts = urlsplit(self.path)
        self.server.requests.append((parts.path, parse_qs(parts.query), dict(self.headers)))
        if parts.path in self.server.slow_first:
            self.server.slow_first.discard(parts.path)
            time.sleep(0.6)
        script = self.server.scripts[parts.path]
        status, headers, body = script.pop(0) if len(script) > 1 else script[0]
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        pass


def json_body(payload: object) -> bytes:
    return json.dumps(payload).encode()


@pytest.fixture
def server():
    srv = ScriptedServer()
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield srv
    srv.shutdown()
    srv.server_close()


def http_settings(**overrides: object) -> HttpSettings:
    values = {
        "contact": "ops@example.org",
        "product": "WikiPulse",
        "timeout_seconds": 5.0,
        "max_attempts": 3,
        "backoff_base_seconds": 0.01,
        "backoff_max_seconds": 0.05,
        "min_retry_after_seconds": 0.5,
        "maxlag_seconds": 5,
        "rate_limits_per_minute": {"127.0.0.1": 6000},
        "max_connections": {"127.0.0.1": 2},
    }
    return HttpSettings(**(values | overrides))


@pytest.fixture
def sleeps():
    return []


@pytest.fixture
def client(sleeps):
    with WikimediaClient(http_settings(), sleep=sleeps.append) as c:
        yield c


def test_user_agent_identifies_project_and_contact(server, client):
    server.scripts["/ok"] = [(200, {"Content-Type": "application/json"}, json_body({"ok": True}))]
    assert client.get_json(f"{server.base}/ok") == {"ok": True}
    user_agent = server.requests[0][2]["User-Agent"]
    assert re.fullmatch(r"WikiPulse/0\.1\.0 \(ops@example\.org\) python-httpx/[\d.]+", user_agent)


def test_empty_contact_is_refused():
    with pytest.raises(ValueError, match="WIKIPULSE_HTTP__CONTACT"):
        WikimediaClient(http_settings(contact=""))


def test_unconfigured_host_is_refused(server, client):
    server.scripts["/ok"] = [(200, {}, b"{}")]
    with pytest.raises(ValueError, match="no rate limit configured for localhost"):
        client.get_json(f"http://localhost:{server.server_address[1]}/ok")
    assert server.requests == []


def test_429_honours_retry_after_seconds(server, client, sleeps):
    server.scripts["/limited"] = [
        (429, {"Retry-After": "1"}, b"rate limited"),
        (200, {}, json_body({"pages": [1]})),
    ]
    assert client.get_json(f"{server.base}/limited") == {"pages": [1]}
    assert sleeps == [1.0]
    assert len(server.requests) == 2


def test_503_honours_retry_after_http_date(server, client, sleeps):
    retry_at = format_datetime(datetime.now(UTC) + timedelta(seconds=30), usegmt=True)
    server.scripts["/overloaded"] = [(503, {"Retry-After": retry_at}, b""), (200, {}, b"[]")]
    assert client.get_json(f"{server.base}/overloaded") == []
    assert 28.0 <= sleeps[0] <= 30.0


def test_retry_after_below_floor_is_raised_to_floor(server, client, sleeps):
    server.scripts["/limited"] = [(429, {"Retry-After": "0"}, b""), (200, {}, b"{}")]
    client.get_json(f"{server.base}/limited")
    assert sleeps == [0.5]


def test_maxlag_is_retried_and_action_api_defaults_are_sent(server, client, sleeps):
    maxlag = {"error": {"code": "maxlag", "info": "Waiting for 10.64.0.1: 7 seconds lagged."}}
    server.scripts["/w/api.php"] = [
        (200, {"MediaWiki-API-Error": "maxlag", "Retry-After": "2"}, json_body(maxlag)),
        (200, {}, json_body({"query": {"pages": [{"pageid": 736, "ns": 0}]}})),
    ]
    payload = client.get_json(f"{server.base}/w/api.php", {"action": "query", "pageids": "736"})
    assert payload["query"]["pages"][0]["pageid"] == 736
    assert sleeps == [2.0]
    query = server.requests[0][1]
    assert query["maxlag"] == ["5"]
    assert query["formatversion"] == ["2"]
    assert query["format"] == ["json"]
    assert query["pageids"] == ["736"]


def test_other_api_errors_raise(server, client):
    error = {"error": {"code": "toomanyvalues", "info": 'Too many values supplied for parameter "pageids".'}}
    server.scripts["/w/api.php"] = [(200, {"MediaWiki-API-Error": "toomanyvalues"}, json_body(error))]
    with pytest.raises(ApiError, match="toomanyvalues") as info:
        client.get_json(f"{server.base}/w/api.php", {"action": "query"})
    assert info.value.code == "toomanyvalues"
    assert len(server.requests) == 1


def test_attempts_are_exhausted(server, client, sleeps):
    server.scripts["/limited"] = [(429, {}, b"")]
    with pytest.raises(RetriesExhausted, match="after 3 attempts: HTTP 429"):
        client.get_json(f"{server.base}/limited")
    assert len(server.requests) == 3
    assert len(sleeps) == 2
    assert all(s >= 0.5 for s in sleeps)


def test_client_errors_are_not_retried(server, client):
    server.scripts["/missing"] = [(404, {}, b"not found")]
    with pytest.raises(httpx.HTTPStatusError):
        client.get_json(f"{server.base}/missing")
    assert len(server.requests) == 1


def test_read_timeout_is_retried(server, sleeps):
    server.scripts["/slow"] = [(200, {}, b'{"done": true}')]
    server.slow_first.add("/slow")
    with WikimediaClient(http_settings(timeout_seconds=0.2), sleep=sleeps.append) as c:
        assert c.get_json(f"{server.base}/slow") == {"done": True}
    assert len(sleeps) == 1
    assert 0 <= sleeps[0] <= 0.01


def test_stream_delivers_large_body_in_chunks(server, client):
    server.scripts["/pageviews-20261027-120000.gz"] = [(200, {}, STREAM_BODY)]
    digest = hashlib.sha256()
    chunks = 0
    with client.stream(f"{server.base}/pageviews-20261027-120000.gz") as response:
        for chunk in response.iter_bytes(chunk_size=256 * 1024):
            digest.update(chunk)
            chunks += 1
    assert chunks > 1
    assert digest.hexdigest() == hashlib.sha256(STREAM_BODY).hexdigest()


def test_stream_retries_before_body_is_read(server, client, sleeps):
    server.scripts["/file.gz"] = [(503, {"Retry-After": "1"}, b""), (200, {}, b"payload")]
    with client.stream(f"{server.base}/file.gz") as response:
        assert response.read() == b"payload"
    assert sleeps == [1.0]


def test_metrics_record_responses_and_waits(server, sleeps):
    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter("test")
    server.scripts["/limited"] = [(429, {"Retry-After": "1"}, b""), (200, {}, b"{}")]
    with WikimediaClient(http_settings(), metrics=HttpMetrics(meter), sleep=sleeps.append) as c:
        c.get_json(f"{server.base}/limited")

    points = {
        metric.name: metric.data.data_points
        for resource in reader.get_metrics_data().resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }
    by_status = {p.attributes["status"]: p.value for p in points["http_requests_total"]}
    assert by_status == {"429": 1, "200": 1}
    assert sum(p.value for p in points["http_throttled_seconds_total"]) == pytest.approx(1.0)


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_token_bucket_spaces_requests_at_the_configured_rate():
    clock = FakeClock()
    waits: list[float] = []
    bucket = TokenBucket(60, clock=clock, sleep=waits.append)
    assert [bucket.acquire() for _ in range(3)] == [0.0, 1.0, 2.0]
    assert waits == [1.0, 2.0]
    clock.now += 10
    assert bucket.acquire() == 0.0


def test_token_bucket_allows_one_second_of_burst():
    clock = FakeClock()
    bucket = TokenBucket(150, clock=clock, sleep=lambda s: None)
    assert [bucket.acquire() for _ in range(2)] == [0.0, 0.0]
    assert bucket.acquire() == pytest.approx(0.2)
