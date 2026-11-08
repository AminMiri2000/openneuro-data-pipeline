{#
    Daily history of each dataset's size and subject count, one row per
    dataset per ingest day. It grows by one catalogue's worth of rows a day, so
    it is built incrementally: each run only re-processes the last few days
    (var `incremental_lookback_days`) and replaces them in place
    (delete+insert on the unique key), which also makes same-day re-runs safe.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['dataset_id', 'ingest_date'],
        on_schema_change='append_new_columns'
    )
}}

with catalogue as (

    select * from {{ ref('stg_openneuro__datasets') }}

    {% if is_incremental() %}
    where ingest_date >= (
        select max(ingest_date) - interval {{ var('incremental_lookback_days') }} day
        from {{ this }}
    )
    {% endif %}

),

-- The same dataset can appear in several modality extracts on one day
-- (e.g. "eeg" and "all"); keep the most recent record.
deduplicated as (

    select *
    from catalogue
    qualify row_number() over (
        partition by dataset_id, ingest_date order by ingested_at desc
    ) = 1

)

select
    dataset_id,
    ingest_date,
    snapshot_tag,
    n_subjects,
    n_sessions,
    size_bytes,
    total_files,
    ingested_at
from deduplicated
