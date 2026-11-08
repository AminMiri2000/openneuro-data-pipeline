"""dbt models built on top of the mocked extract + load (runs real dbt)."""

from datetime import date

import pytest

from openneuro_pipeline.errors import ConfigError, TransformError
from openneuro_pipeline.extract import ExtractParams
from openneuro_pipeline.pipeline import Pipeline
from openneuro_pipeline.storage import layout
from openneuro_pipeline.storage.formats import iter_jsonl_gz, write_jsonl_gz

DAY = date(2026, 11, 7)
PARAMS = ExtractParams(modality="EEG", ingest_date=DAY)


@pytest.fixture
def built(pipeline, mocked_api):
    """Run the full batch once; returns the pipeline and its result."""
    return pipeline, pipeline.run(PARAMS)


def query(pipeline, sql):
    with pipeline.warehouse.connect(read_only=True) as con:
        return con.execute(sql).fetchall()


def test_build_passes_all_tests(built):
    _, result = built
    counts = result.transform.status_counts
    assert not {"error", "fail", "skipped"} & counts.keys(), counts
    assert counts["pass"] >= 40  # data tests + unit tests
    assert result.transform.warnings == []


def test_fct_participant_standardises_demographics(built):
    pipeline, _ = built
    rows = query(
        pipeline,
        "SELECT participant_id, age_years, sex, handedness FROM marts.fct_participant "
        "WHERE dataset_id = 'ds000001' ORDER BY participant_id",
    )
    # From the fixture TSV: F/M codes and R/L hands; 'n/a' age stays unknown.
    assert rows == [
        ("sub-01", 24.0, "female", "right"),
        ("sub-02", None, "male", "left"),
        ("sub-03", 31.0, "female", None),
    ]


def test_dim_dataset_summarises_coverage(built):
    pipeline, _ = built
    row = query(
        pipeline,
        "SELECT n_subjects, has_participants_table, n_participant_rows, "
        "participants_match_subjects, pct_age_known, pct_sex_known, n_female, is_active, doi "
        "FROM marts.dim_dataset WHERE dataset_id = 'ds000001'",
    )
    assert row == [(3, True, 3, True, 0.6667, 1.0, 2, True, "10.18112/openneuro.ds000001.v1.0.2")]

    # ds000003 publishes no participants.tsv; ds000004 was never published.
    no_table = query(
        pipeline,
        "SELECT dataset_id FROM marts.dim_dataset WHERE NOT has_participants_table ORDER BY 1",
    )
    assert no_table == [("ds000003",), ("ds000004",)]


def test_incremental_history_appends_new_days_only(built, store):
    pipeline, _ = built
    next_day = layout.datasets_key(date(2026, 11, 8), "EEG")
    write_jsonl_gz(store, next_day, iter_jsonl_gz(store, layout.datasets_key(DAY, "EEG")))
    pipeline.load()

    pipeline.transform(select="fct_dataset_daily")
    pipeline.transform(select="fct_dataset_daily")  # re-run: no duplicates

    assert query(
        pipeline, "SELECT ingest_date, count(*) FROM marts.fct_dataset_daily GROUP BY 1 ORDER BY 1"
    ) == [
        (DAY, 4),
        (date(2026, 11, 8), 4),
    ]


def test_missing_raw_tables_raise_transform_error(pipeline):
    with pytest.raises(TransformError, match="dbt build failed"):
        pipeline.transform()


def test_missing_project_raises_config_error(settings, tmp_path):
    from dataclasses import replace

    broken = Pipeline(replace(settings, dbt_project_dir=tmp_path / "nowhere"))
    with pytest.raises(ConfigError, match="No dbt project"):
        broken.transform()
