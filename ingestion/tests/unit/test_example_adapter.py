"""Tests for the example adapter."""

import pytest
import responses as resp_mock

from ingestion.src.adapters.example_adapter.adapter import ExampleAdapter, ExampleAdapterConfig
from ingestion.src.adapters.base import FetchResult


@pytest.fixture
def config() -> ExampleAdapterConfig:
    return ExampleAdapterConfig(
        base_url="https://test-api.example.com",
        api_key="test-key-123",
    )


@pytest.fixture
def adapter(config: ExampleAdapterConfig) -> ExampleAdapter:
    a = ExampleAdapter(config)
    a.authenticate()
    return a


@resp_mock.activate
def test_fetch_returns_records(adapter: ExampleAdapter) -> None:
    resp_mock.add(
        resp_mock.GET,
        "https://test-api.example.com/entities",
        json={"data": [{"id": 1, "name": "Test"}], "total": 1, "has_more": False},
        status=200,
    )
    result = adapter.fetch("entities")
    assert isinstance(result, FetchResult)
    assert len(result.records) == 1
    assert result.records[0]["id"] == 1


@resp_mock.activate
def test_fetch_handles_pagination(adapter: ExampleAdapter) -> None:
    resp_mock.add(
        resp_mock.GET,
        "https://test-api.example.com/entities",
        json={"data": [{"id": 1}], "total": 2, "page": 1, "has_more": True},
        status=200,
    )
    result = adapter.fetch("entities", params={"page": 1})
    assert result.has_more is True


def test_validate_empty_result_returns_false(adapter: ExampleAdapter) -> None:
    result = FetchResult(source="example", endpoint="entities", records=[], total_records=0)
    assert adapter.validate(result) is False


def test_validate_non_empty_result_returns_true(adapter: ExampleAdapter) -> None:
    result = FetchResult(
        source="example",
        endpoint="entities",
        records=[{"id": 1}],
        total_records=1,
    )
    assert adapter.validate(result) is True
