import json
import tomllib
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = ROOT / "docs" / "contracts"
EVENT_SCHEMAS = sorted((CONTRACTS / "events").glob("*.json"))
TABLES = sorted((CONTRACTS / "tables").glob("*.yaml"))
TABLE_VALIDATOR = Draft202012Validator(json.loads((CONTRACTS / "table-contract.schema.json").read_text()))


@pytest.mark.parametrize("path", EVENT_SCHEMAS, ids=lambda p: p.name)
def test_event_schema_is_valid_draft_2020_12(path):
    Draft202012Validator.check_schema(json.loads(path.read_text()))


@pytest.mark.parametrize("path", TABLES, ids=lambda p: p.name)
def test_table_contract(path):
    contract = yaml.safe_load(path.read_text())
    errors = [e.message for e in TABLE_VALIDATOR.iter_errors(contract)]
    assert not errors, errors
    assert contract["name"] == path.stem
    assert contract["store"].split(":")[1] == contract["name"].split(".")[0]

    columns = [c["name"] for c in contract["columns"]]
    assert len(columns) == len(set(columns)), "duplicate column names"
    assert set(contract["primary_key"]) <= set(columns)
    nullable = {c["name"] for c in contract["columns"] if c["nullable"]}
    assert not set(contract["primary_key"]) & nullable, "primary key columns must be non-nullable"
    for column in contract["tests"].get("accepted_values", {}):
        assert column in columns, f"accepted_values on unknown column {column}"


def test_silver_and_gold_are_owned_by_dbt():
    for path in TABLES:
        contract = yaml.safe_load(path.read_text())
        if contract["store"] in ("lake:silver", "lake:gold"):
            assert contract["owner_workstream"] == "WS-transform", path.name


def test_openapi_document_shape():
    spec = yaml.safe_load((CONTRACTS / "openapi.yaml").read_text())
    assert spec["openapi"].startswith("3.1")
    for path, operations in spec["paths"].items():
        for method, operation in operations.items():
            if method == "parameters":
                continue
            assert "operationId" in operation, f"{method.upper()} {path}"
            assert "200" in operation["responses"], f"{method.upper()} {path}"


def test_config_file_parses_and_has_no_operator_values():
    config = tomllib.loads((ROOT / "config" / "wikipulse.toml").read_text())
    assert config["http"]["contact"] == ""
    assert config["privacy"]["hmac_key"] == ""
    assert config["postgres"]["password"] == ""
