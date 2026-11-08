{{ config(severity='warn') }}

-- Warns when the latest catalogue lists noticeably fewer datasets than the
-- day before. OpenNeuro rarely deletes datasets, so a sudden drop usually
-- means a partial API response or a broken filter, not real change.
-- Tolerance: var `max_catalogue_drop` (default 10%).

with daily_counts as (

    select ingest_date, count(*) as n_datasets
    from {{ ref('fct_dataset_daily') }}
    group by ingest_date

),

latest_two as (

    select
        ingest_date,
        n_datasets,
        lag(n_datasets) over (order by ingest_date) as previous_n_datasets
    from daily_counts
    qualify row_number() over (order by ingest_date desc) = 1

)

select *
from latest_two
where previous_n_datasets is not null
  and n_datasets < previous_n_datasets * (1 - {{ var('max_catalogue_drop') }})
