import os
import re
import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

CONFIG_RELATIVE_PATH = Path("config") / "wikipulse.toml"


class Section(BaseModel):
    model_config = ConfigDict(extra="forbid")


class HttpSettings(Section):
    contact: str
    product: str
    timeout_seconds: float
    max_attempts: int = Field(ge=1)
    backoff_base_seconds: float
    backoff_max_seconds: float
    min_retry_after_seconds: float
    maxlag_seconds: int
    rate_limits_per_minute: dict[str, int]
    max_connections: dict[str, int]


class WikisSettings(Section):
    page_level: list[str]
    pageview_codes: dict[str, dict[str, str]]


class StreamSettings(Section):
    url: str
    topic: str
    dlq_topic: str
    late_topic: str
    partitions: int
    checkpoint_interval_seconds: float
    checkpoint_interval_events: int
    reconnect_backoff_min_seconds: float
    reconnect_backoff_max_seconds: float
    future_tolerance_seconds: int
    max_payload_bytes: int


class KafkaSettings(Section):
    profile: Literal["local"]
    bootstrap_servers: str
    eventhubs_namespace: str
    compression: Literal["none", "gzip", "snappy", "lz4", "zstd"]
    retention_hours: int


class WindowsSettings(Section):
    wiki_window_seconds: int
    page_window_seconds: int
    page_window_step_seconds: int
    grace_seconds: int
    dedup_ttl_hours: int


class PostgresSettings(Section):
    host: str
    port: int
    database: str
    catalog_database: str
    user: str
    password: SecretStr
    reader_user: str
    reader_password: SecretStr
    auth: Literal["password"]
    pool_min_size: int
    pool_max_size: int


class LakeSettings(Section):
    data_root: Path
    azure_account: str
    azure_container: str
    raw_retention_days: int
    duckdb_memory_limit: str
    duckdb_threads: int
    duckdb_temp_directory: Path
    duckdb_max_temp_directory_size: str


class BatchProfile(Section):
    backfill_days: int = Field(ge=1)


class BatchSettings(Section):
    profile: str
    dumps_base_url: str
    pageviews_path: str
    pageview_complete_path: str
    history_path: str
    history_months: int
    republication_sweep_days: int
    reconciliation_weekday: int = Field(ge=1, le=7)
    row_group_size: int
    profiles: dict[str, BatchProfile]

    @model_validator(mode="after")
    def _profile_exists(self) -> "BatchSettings":
        if self.profile not in self.profiles:
            raise ValueError(f"batch.profile {self.profile!r} is not one of {sorted(self.profiles)}")
        return self

    @property
    def active_profile(self) -> BatchProfile:
        return self.profiles[self.profile]


class EnrichmentSettings(Section):
    cache_ttl_days: int
    ids_per_request: int = Field(ge=1, le=50)


class TrendingSettings(Section):
    baseline_hours: int
    min_robust_z: float
    min_ratio: float
    view_floor: dict[str, int]


class EditBurstSettings(Section):
    window_minutes: int
    step_minutes: int
    baseline_days: int
    max_p_value: float
    min_edits: int
    min_distinct_editors: int
    min_rate_per_hour: float


class DetectionSettings(Section):
    trending: TrendingSettings
    edit_burst: EditBurstSettings


class EvaluationSettings(Section):
    wiki: str
    top_k: int
    label_day_radius: int


class FreshnessSettings(Section):
    stream_max_lag_seconds: int
    pageviews_max_lag_hours: int
    snapshot_max_age_days: int
    history_max_age_days: int


class ApiSettings(Section):
    default_page_size: int
    max_page_size: int
    max_range_hours_hourly: int
    max_range_hours_minute: int


class TelemetrySettings(Section):
    exporter: Literal["otlp", "none"]
    otlp_endpoint: str
    export_interval_seconds: float
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"]


class PrivacySettings(Section):
    hmac_key: SecretStr

    def require_hmac_key(self) -> bytes:
        key = self.hmac_key.get_secret_value()
        if not re.fullmatch(r"[0-9a-fA-F]{64}", key):
            raise ValueError("privacy.hmac_key must be 64 hex characters (set WIKIPULSE_PRIVACY__HMAC_KEY)")
        return bytes.fromhex(key)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WIKIPULSE_", env_nested_delimiter="__", extra="forbid")

    http: HttpSettings
    wikis: WikisSettings
    stream: StreamSettings
    kafka: KafkaSettings
    windows: WindowsSettings
    postgres: PostgresSettings
    lake: LakeSettings
    batch: BatchSettings
    enrichment: EnrichmentSettings
    detection: DetectionSettings
    evaluation: EvaluationSettings
    freshness: FreshnessSettings
    api: ApiSettings
    telemetry: TelemetrySettings
    privacy: PrivacySettings

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # The TOML file arrives as init kwargs; environment variables take precedence over it.
        return (env_settings, init_settings)


def find_config_file(start: Path | None = None) -> Path:
    if env_path := os.environ.get("WIKIPULSE_CONFIG"):
        return Path(env_path)
    directory = (start or Path.cwd()).resolve()
    for candidate in (directory, *directory.parents):
        if (candidate / CONFIG_RELATIVE_PATH).is_file():
            return candidate / CONFIG_RELATIVE_PATH
    raise FileNotFoundError(f"{CONFIG_RELATIVE_PATH} not found from {directory} upward; set WIKIPULSE_CONFIG")


def load_settings(config_path: Path | None = None) -> Settings:
    path = config_path or find_config_file()
    with path.open("rb") as handle:
        return Settings(**tomllib.load(handle))
