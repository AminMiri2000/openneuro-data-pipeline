-- One row per dataset ever seen in the catalogue: its latest metadata plus
-- participant coverage statistics. Built to answer questions such as
-- "which EEG datasets have 20+ subjects with known age and sex?".

with catalogue as (

    select * from {{ ref('stg_openneuro__datasets') }}

),

latest_ingest as (

    select max(ingest_date) as ingest_date from catalogue

),

-- Most recent catalogue record per dataset.
current_record as (

    select *
    from catalogue
    qualify row_number() over (
        partition by dataset_id order by ingest_date desc, ingested_at desc
    ) = 1

),

seen as (

    select
        dataset_id,
        min(ingest_date) as first_seen_date,
        max(ingest_date) as last_seen_date
    from catalogue
    group by dataset_id

),

participant_stats as (

    select
        dataset_id,
        any_value(snapshot_tag)                                  as participants_snapshot_tag,
        count(*)                                                 as n_participant_rows,
        count(age_years)                                         as n_age_known,
        count(sex)                                               as n_sex_known,
        count(handedness)                                        as n_handedness_known,
        count(*) filter (where sex = 'female')                   as n_female,
        count(*) filter (where sex = 'male')                     as n_male,
        min(age_years)                                           as age_min_years,
        round(median(age_years), 2)                              as age_median_years,
        max(age_years)                                           as age_max_years,
        sum(n_unusable_values)                                   as n_unusable_values
    from {{ ref('fct_participant') }}
    group by dataset_id

)

select
    -- identity
    c.dataset_id,
    c.dataset_name,
    'https://openneuro.org/datasets/' || c.dataset_id            as openneuro_url,
    c.doi,
    c.license,
    c.authors,

    -- latest snapshot
    c.snapshot_tag,
    c.snapshot_created_at,
    c.dataset_created_at,
    c.bids_version,
    c.modalities,
    c.tasks,
    c.n_subjects,
    c.n_sessions,
    c.size_bytes,
    round(c.size_bytes / 1e9, 2)                                 as size_gb,
    c.total_files,

    -- catalogue presence
    s.first_seen_date,
    s.last_seen_date,
    -- False once a dataset disappears from the latest catalogue extract.
    s.last_seen_date = l.ingest_date                             as is_active,

    -- participants table coverage
    p.dataset_id is not null                                     as has_participants_table,
    p.participants_snapshot_tag,
    coalesce(p.n_participant_rows, 0)                            as n_participant_rows,
    -- Mismatches usually mean the table lists people with no recordings, or
    -- recordings were added without updating the table.
    p.n_participant_rows = c.n_subjects                          as participants_match_subjects,
    round(p.n_age_known / p.n_participant_rows, 4)               as pct_age_known,
    round(p.n_sex_known / p.n_participant_rows, 4)               as pct_sex_known,
    round(p.n_handedness_known / p.n_participant_rows, 4)        as pct_handedness_known,
    p.n_female,
    p.n_male,
    p.age_min_years,
    p.age_median_years,
    p.age_max_years,
    coalesce(p.n_unusable_values, 0)                             as n_unusable_values

from current_record as c
inner join seen as s on c.dataset_id = s.dataset_id
cross join latest_ingest as l
left join participant_stats as p on c.dataset_id = p.dataset_id
