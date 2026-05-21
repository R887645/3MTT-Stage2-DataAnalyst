# 3MTT Data Pipeline

Ingests the four raw 3MTT datasets (fellows, ALC log, reflection survey, employer engagement), applies cleaning, deduplication, and phone-number standardisation, and writes cleaned CSVs plus a structured JSON run-log to an output directory.

## How to run

```bash
python 3mtt_pipeline.py \
  --fellows  fellows_cohort.csv \
  --alc      alc_weekly_log.csv \
  --survey   reflection_survey.xlsx \
  --employer employer_engagement.csv \
  --output   ./cleaned
```

## What it outputs

| File | Description |
|------|-------------|
| `fellows_cohort_clean.csv` | Deduped, title-cased states, missing ALC codes filled |
| `alc_weekly_log_clean.csv` | Deduped, out-of-range week (99) removed |
| `reflection_survey_clean.csv` | Deduped per fellow+week, phones → 234XXXXXXXXXX |
| `employer_engagement_clean.csv` | Deduped, missing ALC codes filled |
| `run_log.json` | Timestamp, rows in/out, all issues found, processing time, status |

## Idempotency

Running the script twice on identical inputs produces identical output files. All cleaning steps use deterministic logic (drop_duplicates keep='first', fixed regex rules) so there is no risk of accumulating duplicates across runs.

## Scheduling design note

For weekly production use, the recommended approach is a **GitHub Actions scheduled workflow** (`on: schedule: cron: '0 6 * * 1'`). This requires no server, version-controls the pipeline code, and provides a full audit trail of every run in the Actions log. The output CSVs can be pushed back to the repo or uploaded to Google Drive via the Drive API. Alternative: a `cron` job on a VM is simpler but requires infrastructure management and has no built-in alerting. Airflow is appropriate if this pipeline grows into a multi-step DAG with dependencies, but is overkill for a single-script weekly ingest at this data size.

## Requirements

Python 3.10+ with `pandas`, `numpy`, `openpyxl` installed.
