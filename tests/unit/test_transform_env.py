import os

from openneuro_pipeline.transform.dbt import _temporary_env


def test_temporary_env_restores_previous_values(monkeypatch):
    monkeypatch.setenv("KEEP_ME", "original")
    monkeypatch.delenv("NEW_VAR", raising=False)

    with _temporary_env({"KEEP_ME": "changed", "NEW_VAR": "x"}):
        assert os.environ["KEEP_ME"] == "changed"
        assert os.environ["NEW_VAR"] == "x"

    assert os.environ["KEEP_ME"] == "original"
    assert "NEW_VAR" not in os.environ
