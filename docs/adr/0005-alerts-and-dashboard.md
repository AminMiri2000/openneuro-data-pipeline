# ADR 0005: Data-quality alerts and a static dashboard

- Status: accepted
- Date: 2026-11-08

## Context

The pipeline was scheduled and tested, but nobody would hear about a problem
unless they opened Dagster, and the results were only visible through SQL.
Phase 4 adds alerting and a public dashboard, still at no cost.

## Decision

1. **Quality rules are dbt tests.** New warn-level tests check that the
   catalogue did not shrink more than 10% day on day, and that at least 95% of
   the demographic values datasets provide are standardised. Empty values are
   excluded: they reflect the source, not our mapping. Thresholds are dbt
   vars. Rules therefore run in every build, appear as Dagster asset checks,
   and live next to the models they protect.
2. **One alert module, two runners.** `alerts/` turns outcomes into a short
   plain-text digest and sends it by Gmail SMTP using an app password from
   environment variables (`.env` locally, repository secrets in CI). The CLI
   (`run --notify`) and the Dagster sensors use the same module, so the email
   looks the same wherever the run happened.
3. **Silence when healthy, and no repeats.** Digests are sent only for
   failures or warnings, and each warning only when it first appears (or
   returns after clearing). A small alert memory in the pipeline state records
   what was already reported. The first real run showed why: one OpenNeuro
   dataset lists a participant twice, which would otherwise email every day.
   Failures are never filtered. The stale-data alert fires at most once a day.
4. **Alerting never breaks the pipeline.** A mail error is logged, not raised:
   a broken alert channel must not turn a good refresh into a failed one.
5. **A static dashboard on GitHub Pages.** A daily GitHub Actions workflow
   runs the pipeline, builds one self-contained HTML page from the marts (data
   embedded as JSON), adds the dbt docs, and deploys to Pages. Raw files are
   carried between runs in the Actions cache to keep downloads incremental;
   the warehouse is rebuilt from them each run.
6. **Charts drawn in plain SVG.** No charting library: six simple forms
   (columns, bars, a 100% stacked bar, a line) with hover tooltips, a data
   table behind every chart, a colour-blind-safe palette with separately chosen
   light and dark steps, and a layout tested at phone width.

## Alternatives considered

- **Streamlit (Community Cloud).** Free and quick, but it needs the warehouse
  available at view time, sleeps when idle, and adds a running service. The
  data changes once a day, so a page built once a day fits better.
- **Evidence or Observable Framework.** Good "BI as code" tools, but they add
  a Node toolchain to a Python project, and the DuckDB file version would
  need to match the one inside their bundled engine.
- **Slack or Discord webhooks.** Simpler than SMTP, but email was the
  preferred channel; webhooks can be added as another sender in `alerts/`.
- **Dagster freshness policies.** Built-in, but they need a sensor and an
  email hook anyway; a 30-line sensor with a once-a-day cursor is clearer.

## Consequences

- The repository must be public for free GitHub Pages; it holds code only.
- GitHub disables scheduled workflows in repositories with no activity for 60
  days. Any commit, or re-enabling the workflow, restarts it.
- Alerts from Dagster only fire while `make dagster` is running; the GitHub
  Actions run covers the always-on case.
