import pytest

from openneuro_pipeline.config import Settings
from openneuro_pipeline.errors import ConfigError


def test_defaults_are_valid():
    settings = Settings()
    assert settings.raw_dir.name == "raw"
    assert settings.warehouse_path.name == "warehouse.duckdb"


def test_reads_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("PIPELINE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PIPELINE_MAX_WORKERS", "8")
    settings = Settings.from_env()
    assert settings.data_dir == tmp_path
    assert settings.max_workers == 8


@pytest.mark.parametrize(
    "kwargs",
    [{"page_size": 0}, {"max_workers": 100}, {"request_timeout": 0}, {"insert_batch_size": 0}],
)
def test_rejects_invalid_values(kwargs):
    with pytest.raises(ConfigError):
        Settings(**kwargs)


def test_rejects_non_numeric_env(monkeypatch):
    monkeypatch.setenv("OPENNEURO_PAGE_SIZE", "lots")
    with pytest.raises(ConfigError):
        Settings.from_env()
