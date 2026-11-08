-- Standardised participant attributes in long format: one row per participant
-- per recognised source column (latest snapshot only).
--
-- Steps
--   1. unpivot   each participant's JSON attributes into (column, value) rows
--   2. match     column names to a standard attribute via the aliases seed
--                (Age -> age, gender -> sex, hand -> handedness, ...)
--   3. decode    coded values with the dataset's own data dictionary
--                ("Gender": "2" + Levels {"2": "Female"} -> "Female")
--   4. convert   to a standard value: age in years (honouring declared units);
--                sex and handedness by exact lookup, then by word patterns
--                ("Physiological female", "Right handed (LQ > 0)")
--   5. classify  each value with a parse_status, so data quality is measurable
--                instead of silently dropped
--
-- Columns that match no alias (task scores, questionnaires, ...) are left out.

with participants as (

    select participant_row_key, dataset_id, snapshot_tag, participant_id, attributes
    from {{ ref('stg_openneuro__participants') }}
    where is_latest_snapshot

),

aliases as (

    select * from {{ ref('participant_attribute_aliases') }}

),

dictionary as (

    select dataset_id, snapshot_tag, column_key, units, levels
    from {{ ref('stg_openneuro__participant_columns') }}
    -- Several sidecar keys can normalise to the same column_key; keep one.
    qualify row_number() over (
        partition by dataset_id, snapshot_tag, column_key order by column_name
    ) = 1

),

-- 1. unpivot ---------------------------------------------------------------
unpivoted as (

    select
        participant_row_key,
        dataset_id,
        snapshot_tag,
        participant_id,
        entry.key                                   as source_column,
        {{ normalise_key('entry.key') }}            as source_column_key,
        nullif(trim(entry.value), '')               as raw_value
    from (
        select
            *,
            -- Nested JSON values arrive as text, which is fine: they are not
            -- valid ages, sexes or handedness values and get classified as such.
            unnest(map_entries(try_cast(attributes as map(varchar, varchar)))) as entry
        from participants
    )

),

-- 2. match + 3. decode -----------------------------------------------------
matched as (

    select
        u.*,
        a.attribute,
        a.priority,
        d.units,
        map_extract_value(
            try_cast(d.levels as map(varchar, varchar)), u.raw_value
        )                                           as level_label
    from unpivoted as u
    inner join aliases as a
        on u.source_column_key = a.source_column_key
    left join dictionary as d
        on  u.dataset_id = d.dataset_id
        and u.snapshot_tag = d.snapshot_tag
        and u.source_column_key = d.column_key

),

decoded as (

    select
        *,
        coalesce(level_label, raw_value)            as decoded_value,
        level_label is not null                     as was_decoded
    from matched

),

-- 4. convert ---------------------------------------------------------------
exact_matched as (

    select
        dec.*,

        -- Age: numeric, scaled to years when the dictionary declares other units.
        case when dec.attribute = 'age' then
            try_cast(dec.decoded_value as double) / case
                when lower(dec.units) like '%month%' then 12.0
                when lower(dec.units) like '%week%'  then 52.1775
                when lower(dec.units) like '%day%'   then 365.25
                else 1.0
            end
        end                                         as age_years_unchecked,

        sv.value_key is not null or hv.value_key is not null as is_exact_match,
        coalesce(sv.sex, hv.handedness)             as exact_value

    from decoded as dec
    left join {{ ref('sex_values') }} as sv
        on  dec.attribute = 'sex'
        and {{ normalise_key('dec.decoded_value') }} = sv.value_key
    left join {{ ref('handedness_values') }} as hv
        on  dec.attribute = 'handedness'
        and {{ normalise_key('dec.decoded_value') }} = hv.value_key

),

-- Pattern fallback, tried only for category values with no exact match.
pattern_hits as (

    select
        e.participant_row_key,
        e.source_column,
        p.standard_value
    from exact_matched as e
    inner join {{ ref('attribute_value_patterns') }} as p
        on  e.attribute = p.attribute
        and not e.is_exact_match
        and e.decoded_value is not null
        and regexp_matches(e.decoded_value, p.pattern, 'i')

),

-- Patterns pointing to different categories make a value ambiguous.
pattern_resolved as (

    select
        participant_row_key,
        source_column,
        count(distinct coalesce(standard_value, '<unknown>')) as n_categories,
        any_value(standard_value)                             as pattern_value
    from pattern_hits
    group by participant_row_key, source_column

),

converted as (

    select
        e.*,
        case
            when e.is_exact_match     then 'exact'
            when r.n_categories = 1   then 'pattern'
        end                                         as match_method,
        case
            when e.is_exact_match     then e.exact_value
            when r.n_categories = 1   then r.pattern_value
        end                                         as category_value,
        coalesce(r.n_categories > 1, false)         as is_ambiguous
    from exact_matched as e
    left join pattern_resolved as r
        on  e.participant_row_key = r.participant_row_key
        and e.source_column = r.source_column

),

-- 5. classify --------------------------------------------------------------
classified as (

    select
        participant_row_key,
        dataset_id,
        snapshot_tag,
        participant_id,
        attribute,
        priority,
        source_column,
        raw_value,
        decoded_value,
        was_decoded,
        units,
        match_method,

        case
            when attribute = 'age'
                 and age_years_unchecked > 0 and age_years_unchecked < 120
                then cast(round(age_years_unchecked, 2) as varchar)
            when attribute in ('sex', 'handedness')
                then category_value
        end                                         as standard_value,

        case
            when raw_value is null                                      then 'missing'
            when attribute = 'age' and age_years_unchecked is null       then 'unparseable'
            when attribute = 'age'
                 and not (age_years_unchecked > 0 and age_years_unchecked < 120)
                                                                         then 'out_of_range'
            when attribute = 'age'                                       then 'ok'
            when is_ambiguous                                            then 'ambiguous'
            -- A bare number with no data dictionary: a code ("1") or a score
            -- ("0.88") whose meaning differs between datasets. Never guessed.
            when match_method is null
                 and regexp_full_match(trim(decoded_value), '[-+]?[0-9]+([.][0-9]+)?')
                                                                         then 'undocumented_numeric'
            when match_method is null                                    then 'unmapped'
            -- a known "unknown" value, e.g. "U" or "prefer not to respond"
            when category_value is null                                  then 'missing'
            else 'ok'
        end                                         as parse_status

    from converted

)

select * from classified
