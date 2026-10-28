from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

from wikipulse_common.config import TelemetrySettings


def configure_metrics(settings: TelemetrySettings, service: str) -> None:
    # With exporter "none" the OpenTelemetry API keeps its no-op provider, so instruments cost nothing.
    if settings.exporter == "none":
        return
    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=settings.otlp_endpoint),
        export_interval_millis=settings.export_interval_seconds * 1000,
    )
    metrics.set_meter_provider(
        MeterProvider(resource=Resource.create({"service.name": service}), metric_readers=[reader])
    )


def get_meter(service: str) -> metrics.Meter:
    return metrics.get_meter(service)


class HttpMetrics:
    def __init__(self, meter: metrics.Meter) -> None:
        self._requests = meter.create_counter(
            "http_requests_total", description="Outbound HTTP responses by host and status code"
        )
        self._throttled = meter.create_counter(
            "http_throttled_seconds_total",
            unit="s",
            description="Time spent waiting on rate limits, Retry-After and backoff",
        )

    def record_response(self, host: str, status: int) -> None:
        self._requests.add(1, {"host": host, "status": str(status)})

    def record_wait(self, host: str, seconds: float) -> None:
        if seconds > 0:
            self._throttled.add(seconds, {"host": host})
