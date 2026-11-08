-- One row per column documented in a dataset's participants.json sidecar
-- (the BIDS data dictionary). `levels` maps coded values to labels, e.g.
-- {"1": "male", "2": "female"}; `units` says e.g. whether age is in months.

with source as (

    select * from {{ source('openneuro', 'openneuro_participant_columns') }}

),

renamed as (

    select
        dataset_id,
        snapshot_tag,
        column_name,
        {{ normalise_key('column_name') }}                     as column_key,
        -- A sidecar entry should be an object, but some are a bare string.
        json_type(definition) = 'OBJECT'                       as is_structured,
        case when json_type(definition) = 'OBJECT'
            then nullif(trim(definition ->> '$.Description'), '')
            else nullif(trim(definition ->> '$'), '')
        end                                                    as description,
        case when json_type(definition) = 'OBJECT'
            then nullif(trim(definition ->> '$.Units'), '')
        end                                                    as units,
        case when json_type(definition -> '$.Levels') = 'OBJECT'
            then definition -> '$.Levels'
        end                                                    as levels,
        definition
    from source

)

select * from renamed
