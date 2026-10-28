import shutil
from pathlib import Path

import pytest
from pydantic import ValidationError

from wikipulse_common.config import find_config_file, load_settings

REPO_CONFIG = Path(__file__).resolve().parents[3] / "config" / "wikipulse.toml"


def test_repository_config_loads():
    settings = load_settings(REPO_CONFIG)
    assert settings.kafka.profile == "local"
    assert settings.stream.topic == "wikipulse.page_change.v1"
    assert settings.wikis.pageview_codes["dewiki"]["mobile-web"] == "de.m"
    assert settings.http.rate_limits_per_minute["dumps.wikimedia.org"] == 120
    assert settings.batch.active_profile.backfill_days == 35
    assert settings.detection.edit_burst.max_p_value == pytest.approx(1e-4)


def test_environment_overrides_nested_keys(monkeypatch):
    monkeypatch.setenv("WIKIPULSE_HTTP__CONTACT", "ops@example.org")
    monkeypatch.setenv("WIKIPULSE_DETECTION__TRENDING__MIN_RATIO", "4.5")
    monkeypatch.setenv("WIKIPULSE_BATCH__PROFILE", "quick")
    monkeypatch.setenv("WIKIPULSE_POSTGRES__PORT", "5432")
    settings = load_settings(REPO_CONFIG)
    assert settings.http.contact == "ops@example.org"
    assert settings.detection.trending.min_ratio == 4.5
    assert settings.detection.trending.baseline_hours == 168
    assert settings.batch.active_profile.backfill_days == 8
    assert settings.postgres.port == 5432


def test_unknown_batch_profile_is_rejected(monkeypatch):
    monkeypatch.setenv("WIKIPULSE_BATCH__PROFILE", "nightly")
    with pytest.raises(ValidationError, match=r"batch\.profile"):
        load_settings(REPO_CONFIG)


def test_unknown_key_in_file_is_rejected(tmp_path):
    config = tmp_path / "wikipulse.toml"
    config.write_text(REPO_CONFIG.read_text().replace("[windows]\n", "[windows]\ngrace_minutes = 5\n"))
    with pytest.raises(ValidationError, match="grace_minutes"):
        load_settings(config)


def test_config_is_found_from_a_subdirectory(tmp_path, monkeypatch):
    (tmp_path / "config").mkdir()
    shutil.copy(REPO_CONFIG, tmp_path / "config" / "wikipulse.toml")
    nested = tmp_path / "services" / "ingest"
    nested.mkdir(parents=True)
    assert find_config_file(nested) == tmp_path / "config" / "wikipulse.toml"
    monkeypatch.setenv("WIKIPULSE_CONFIG", "/etc/wikipulse/wikipulse.toml")
    assert find_config_file(nested) == Path("/etc/wikipulse/wikipulse.toml")


def test_hmac_key_must_be_64_hex_characters(monkeypatch):
    assert load_settings(REPO_CONFIG).privacy.hmac_key.get_secret_value() == ""
    with pytest.raises(ValueError, match="64 hex"):
        load_settings(REPO_CONFIG).privacy.require_hmac_key()
    monkeypatch.setenv("WIKIPULSE_PRIVACY__HMAC_KEY", "ab" * 32)
    assert load_settings(REPO_CONFIG).privacy.require_hmac_key() == bytes([0xAB]) * 32


def test_secrets_do_not_appear_in_repr(monkeypatch):
    monkeypatch.setenv("WIKIPULSE_POSTGRES__PASSWORD", "s3cret-value")
    settings = load_settings(REPO_CONFIG)
    assert "s3cret-value" not in repr(settings)
    assert settings.postgres.password.get_secret_value() == "s3cret-value"
