-- One row per line of a participants.tsv, for every snapshot ever fetched.
-- `is_latest_snapshot` marks the most recently ingested snapshot of each
-- dataset, which is what downstream models use.

with source as (

    select * from {{ source('openneuro', 'openneuro_participants') }}

),

renamed as (

    select
        -- Stable key for a row: dataset, snapshot and line number. The
        -- participant_id alone is not trusted to be unique or present.
        dataset_id || ':' || snapshot_tag || ':' || row_number   as participant_row_key,
        dataset_id,
        snapshot_tag,
        row_number,
        nullif(trim(participant_id), '')                         as participant_id,
        attributes,
        ingested_at
    from source

),

flagged as (

    select
        *,
        snapshot_tag = first_value(snapshot_tag) over (
            partition by dataset_id
            order by ingested_at desc, snapshot_tag desc
        ) as is_latest_snapshot
    from renamed

)

select * from flagged
