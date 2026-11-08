-- The question this project was built to answer:
-- which EEG datasets have at least 20 subjects, known age and sex for nearly
-- every participant, and a visual task?
--
-- Compile with `openneuro-pipeline dbt compile`, or run the body directly with
-- `make query SQL="..."`.

select
    dataset_id,
    dataset_name,
    n_subjects,
    pct_age_known,
    pct_sex_known,
    age_median_years,
    tasks,
    openneuro_url
from {{ ref('dim_dataset') }}
where is_active
  and list_contains(modalities, 'eeg')
  and n_subjects >= 20
  and pct_age_known >= 0.95
  and pct_sex_known >= 0.95
  and len(list_filter(tasks, lambda t: regexp_matches(t, 'visual|image|face|picture|flicker|ssvep', 'i'))) > 0
order by n_subjects desc
