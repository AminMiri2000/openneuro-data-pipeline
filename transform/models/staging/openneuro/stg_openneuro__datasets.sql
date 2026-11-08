-- One row per dataset per ingest day (and modality filter), with the API
-- payload unpacked into typed columns. No rows are filtered or deduplicated
-- here: staging stays 1:1 with its source.

with source as (

    select * from {{ source('openneuro', 'openneuro_datasets') }}

),

unpacked as (

    select
        dataset_id,
        ingest_date,
        modality_filter,
        run_id,
        ingested_at,
        payload,
        payload -> '$.latestSnapshot'                  as snapshot,
        payload -> '$.latestSnapshot.description'      as description,
        payload -> '$.latestSnapshot.summary'          as summary
    from source

),

typed as (

    select
        -- keys and lineage
        dataset_id,
        ingest_date,
        modality_filter,
        run_id,
        ingested_at,

        -- dataset
        try_cast(payload ->> '$.created' as timestamptz)       as dataset_created_at,

        -- latest snapshot (NULL when the dataset was never published)
        snapshot ->> '$.id'                                    as snapshot_id,
        snapshot ->> '$.tag'                                   as snapshot_tag,
        try_cast(snapshot ->> '$.created' as timestamptz)      as snapshot_created_at,

        -- dataset_description.json
        nullif(trim(description ->> '$.Name'), '')             as dataset_name,
        -- BIDS says Authors is a list, but some datasets use a single string.
        case json_type(description -> '$.Authors')
            when 'ARRAY' then try_cast(description -> '$.Authors' as varchar[])
            when 'VARCHAR' then [description ->> '$.Authors']
        end                                                    as authors,
        nullif(trim(description ->> '$.License'), '')          as license,
        -- DOIs arrive as "10.x/...", "doi:10.x/..." or "https://doi.org/10.x/...".
        nullif(
            regexp_replace(
                trim(description ->> '$.DatasetDOI'),
                '^(doi:|https?://(dx\.)?doi\.org/)', '', 'i'
            ),
            ''
        )                                                      as doi,
        nullif(trim(description ->> '$.BIDSVersion'), '')      as bids_version,

        -- snapshot summary
        list_transform(
            try_cast(summary -> '$.modalities' as varchar[]), lambda m: lower(m)
        )                                                      as modalities,
        try_cast(summary -> '$.tasks' as varchar[])            as tasks,
        json_array_length(summary -> '$.subjects')             as n_subjects,
        json_array_length(summary -> '$.sessions')             as n_sessions,
        try_cast(summary ->> '$.size' as bigint)               as size_bytes,
        try_cast(summary ->> '$.totalFiles' as integer)        as total_files

    from unpacked

)

select * from typed
