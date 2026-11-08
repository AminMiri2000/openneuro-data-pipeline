-- One row per participant (line of participants.tsv) in each dataset's latest
-- snapshot, with standardised age, sex and handedness.
--
-- When a dataset has several columns for one attribute (e.g. `sex` and
-- `gender`), the usable value with the lowest alias priority wins, and the
-- column it came from is recorded for transparency.

with participants as (

    select participant_row_key, dataset_id, snapshot_tag, participant_id, attributes
    from {{ ref('stg_openneuro__participants') }}
    where is_latest_snapshot

),

attribute_values as (

    select * from {{ ref('int_participant_attribute_values') }}

),

pivoted as (

    select
        participant_row_key,

        try_cast(
            min_by(standard_value, priority) filter (where attribute = 'age' and parse_status = 'ok')
            as double
        )                                                                         as age_years,
        min_by(source_column, priority)  filter (where attribute = 'age' and parse_status = 'ok')
                                                                                  as age_source_column,

        min_by(standard_value, priority) filter (where attribute = 'sex' and parse_status = 'ok')
                                                                                  as sex,
        min_by(source_column, priority)  filter (where attribute = 'sex' and parse_status = 'ok')
                                                                                  as sex_source_column,

        min_by(standard_value, priority) filter (where attribute = 'handedness' and parse_status = 'ok')
                                                                                  as handedness,
        min_by(source_column, priority)  filter (where attribute = 'handedness' and parse_status = 'ok')
                                                                                  as handedness_source_column,

        -- Values present in the file that could not be standardised.
        count(*) filter (where parse_status not in ('ok', 'missing'))             as n_unusable_values

    from attribute_values
    group by participant_row_key

)

select
    p.participant_row_key,
    p.dataset_id,
    p.snapshot_tag,
    p.participant_id,
    v.age_years,
    v.sex,
    v.handedness,
    v.age_source_column,
    v.sex_source_column,
    v.handedness_source_column,
    coalesce(v.n_unusable_values, 0)                     as n_unusable_values,
    -- Number of columns the dataset provides beyond participant_id.
    coalesce(len(json_keys(p.attributes)), 0)            as n_source_columns
from participants as p
left join pivoted as v
    on p.participant_row_key = v.participant_row_key
