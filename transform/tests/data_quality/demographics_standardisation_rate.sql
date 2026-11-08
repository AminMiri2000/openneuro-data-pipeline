{{ config(severity='warn') }}

-- Warns when too few of the demographic values datasets actually provide
-- could be standardised (empty values are excluded: they are a property of
-- the source, not of our mapping). A drop usually means new spellings or
-- codes appeared; `int_participant_attribute_values` lists them by
-- parse_status. Threshold: var `min_standardisation_rate` (default 95%).

with rates as (

    select
        attribute,
        count(*) filter (where parse_status <> 'missing')                       as n_provided,
        count(*) filter (where parse_status = 'ok')                             as n_standardised,
        count(*) filter (where parse_status = 'ok')
            / nullif(count(*) filter (where parse_status <> 'missing'), 0)      as standardisation_rate
    from {{ ref('int_participant_attribute_values') }}
    group by attribute

)

select *
from rates
where standardisation_rate < {{ var('min_standardisation_rate') }}
