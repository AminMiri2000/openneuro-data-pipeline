# ADR 0003: dbt modelling and attribute standardisation

- Status: accepted
- Date: 2026-11-08

## Context

The first full run loaded 469 EEG datasets and 26,174 participant rows from
435 participants tables. Samples showed:

- the same attribute under many names: `age`/`Age`, `sex`/`gender`/`Gender`,
  `hand`/`handedness`/`Handedness`;
- inconsistent values: `F`, `f`, `Female`, and numeric codes such as
  `"Gender": "2"` that only the dataset's `participants.json` explains;
- DOIs with and without a `doi:` prefix;
- datasets with no participants table at all (34 of 469).

## Decision

1. **dbt-core with dbt-duckdb** for all transformation, in three layers:
   staging (views, 1:1 with raw tables), intermediate (views) and marts
   (tables). dbt is run in-process through `dbtRunner`, so no `dbt`
   executable needs to be on PATH on any OS.
2. **Standardise through seeds, not CASE statements.** Column aliases and
   value spellings live in CSV seeds. Coverage grows by adding rows, and the
   mapping is reviewable by non-engineers.
3. **Decode with the dataset's own dictionary first.** A value is translated
   through `participants.json` `Levels` before lookup, so coded values are
   interpreted the way the dataset's authors defined them. Codes without a
   dictionary are reported as `unmapped`, never guessed.
4. **Classify, don't drop.** Every recognised value gets a `parse_status`.
   Data quality becomes a number (`dim_dataset.n_unusable_values`) and a
   worklist, not an invisible loss.
5. **Sex and gender share one output column.** Many datasets use the two
   words interchangeably, so separating them would be false precision. The
   `sex` column records which source column it came from, and `sex` is
   preferred over `gender` when a dataset has both.
6. **Incremental daily history.** `fct_dataset_daily` grows by one catalogue a
   day, so it is incremental with delete+insert over a 3-day look-back. Older
   backfills run with `--full-refresh` (`make backfill` does this).
7. **Warn, don't fail, on upstream data issues.** Duplicate participant ids
   within a dataset produce a dbt warning; contract violations in our own
   models (keys, accepted values, ranges) fail the build.
8. **Python 3.11–3.14.** dbt's dependencies had no Python 3.15 wheels at the
   time of writing; `make install` checks the version first and explains the
   fix instead of failing inside pip.

## Alternatives considered

- **pandas transformations in Python.** Less tooling, but no lineage,
  documentation site, or declarative tests, which are the point of the layer.
- **Typing participant columns in the loader.** Would bake cleaning rules
  into extraction code and lose the raw values needed to audit them.
- **dbt packages (dbt_utils, dbt_expectations).** Useful, but `dbt deps`
  adds a network step to every fresh checkout and CI run. The one custom test
  needed (`value_between`) is six lines of SQL.
- **Incremental fct_participant.** At ~26k rows a full rebuild takes well
  under a second. It should become incremental, keyed on
  `participant_row_key`, if the participant volume grows by orders of
  magnitude (e.g. all modalities plus phenotype files).

## Consequences

- `make run` now ends with `dbt build`: 3 seeds, 7 models, 42 data tests and
  2 unit tests.
- dbt artefacts (compiled SQL, logs, docs site) live in `data/dbt/`, outside
  git, isolated per data directory.
- The pipeline's own Python test suite runs a real `dbt build` on fixture
  data, so a model change that breaks the integration fails CI.
