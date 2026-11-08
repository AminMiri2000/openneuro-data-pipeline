import json

from click.testing import CliRunner

from openneuro_pipeline.cli import main
from tests.conftest import GRAPHQL_URL, S3_URL


def test_run_command_end_to_end(tmp_path, monkeypatch, mocked_api):
    monkeypatch.setenv("PIPELINE_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("OPENNEURO_GRAPHQL_URL", GRAPHQL_URL)
    monkeypatch.setenv("OPENNEURO_S3_BASE_URL", S3_URL)
    monkeypatch.setenv("OPENNEURO_PAGE_SIZE", "2")

    result = CliRunner().invoke(main, ["run", "--modality", "EEG"])

    assert result.exit_code == 0, result.output
    summary = json.loads(result.output[result.output.index("{") :])
    assert summary["datasets"]["records"] == 4
    assert summary["participants"]["fetched"] == 2
    assert summary["load"]["participants_rows"] == 4
    assert (tmp_path / "data" / "warehouse.duckdb").exists()
