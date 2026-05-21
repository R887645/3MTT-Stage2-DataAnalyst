#!/usr/bin/env python3
"""
3MTT Data Analyst — Stage 2: Automation Script
================================================
Ingests the four 3MTT raw data files, applies cleaning, deduplication,
and phone standardisation, then emits a structured run-log.

Usage:
    python 3mtt_pipeline.py --fellows fellows_cohort.csv \
                            --alc alc_weekly_log.csv \
                            --survey reflection_survey.xlsx \
                            --employer employer_engagement.csv \
                            --output ./cleaned

Features:
    • Configurable input paths via CLI (nothing hardcoded)
    • Structured JSON run-log: timestamp, rows in/out, issues, time taken
    • Idempotent: running twice on the same input produces identical output
    • Graceful error handling: bad rows fail with clean messages, not tracebacks
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import numpy as np


# ── Helpers ───────────────────────────────────────────────────────────────────

def log_step(log: dict, key: str, value):
    """Append a note to the run-log."""
    log.setdefault("steps", []).append({key: value, "ts": datetime.now(timezone.utc).replace(tzinfo=None).isoformat()})


def standardise_phone(raw) -> str | None:
    """
    Normalise any Nigerian mobile number to 234XXXXXXXXXX (13 digits).
    Returns None if the number cannot be recovered.
    """
    if pd.isna(raw):
        return None
    s = re.sub(r"[\s\-().]+", "", str(raw))   # strip separators
    s = re.sub(r"^\+", "", s)                  # remove leading +
    s = re.sub(r"^00", "", s)                  # remove leading 00
    if s.startswith("0"):
        s = "234" + s[1:]                      # 080... → 23480...
    if not s.startswith("234"):
        s = "234" + s                           # bare 8-digit → prepend 234
    return s if re.fullmatch(r"234\d{10}", s) else None


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_fellows(path: str, log: dict) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"fellows file not found: {path}")
    df = pd.read_csv(path)
    required = {"fellow_id", "state", "alc_code", "cohort_number",
                "track", "enrollment_date", "completion_status", "certification_status"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"fellows_cohort is missing columns: {missing}")
    log_step(log, "fellows_raw_rows", len(df))
    return df


def load_alc(path: str, log: dict) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"ALC log file not found: {path}")
    df = pd.read_csv(path)
    log_step(log, "alc_raw_rows", len(df))
    return df


def load_survey(path: str, log: dict) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"survey file not found: {path}")
    # Row 0 is a title string — skip it
    df = pd.read_excel(path, skiprows=1, header=0)
    required = {"fellow_id", "week", "phone_number", "email"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"reflection_survey is missing columns: {missing}")
    log_step(log, "survey_raw_rows", len(df))
    return df


def load_employer(path: str, log: dict) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"employer file not found: {path}")
    df = pd.read_csv(path)
    log_step(log, "employer_raw_rows", len(df))
    return df


# ── Cleaning steps ────────────────────────────────────────────────────────────

def clean_fellows(df: pd.DataFrame, log: dict) -> pd.DataFrame:
    issues = []

    # 1. Full duplicate rows
    dup_rows = df.duplicated().sum()
    if dup_rows:
        issues.append(f"{dup_rows} fully duplicate rows dropped")
    df = df.drop_duplicates()

    # 2. Mixed-case state names
    df["state"] = df["state"].str.strip().str.title()
    issues.append("state names normalised to title-case")

    # 3. Missing alc_code
    missing_alc = df["alc_code"].isna().sum()
    if missing_alc:
        issues.append(f"{missing_alc} missing alc_code filled with UNKNOWN")
    df["alc_code"] = df["alc_code"].fillna("UNKNOWN")

    # 4. Enrollment date parsing
    df["enrollment_date"] = pd.to_datetime(df["enrollment_date"], errors="coerce")
    bad_dates = df["enrollment_date"].isna().sum()
    if bad_dates:
        issues.append(f"{bad_dates} enrollment_date values could not be parsed")

    # 5. Duplicate fellow_ids — keep first
    dup_ids = df["fellow_id"].duplicated().sum()
    if dup_ids:
        issues.append(f"{dup_ids} duplicate fellow_ids — kept first occurrence")
    df = df.drop_duplicates(subset="fellow_id", keep="first")

    log_step(log, "fellows_issues", issues)
    log_step(log, "fellows_clean_rows", len(df))
    return df


def clean_alc(df: pd.DataFrame, log: dict) -> pd.DataFrame:
    issues = []

    dup_rows = df.duplicated().sum()
    if dup_rows:
        issues.append(f"{dup_rows} duplicate rows dropped")
    df = df.drop_duplicates()

    # Out-of-range week sentinel
    bad_weeks = (df["week_number"] == 99).sum()
    if bad_weeks:
        issues.append(f"{bad_weeks} rows with week_number=99 removed (out-of-range sentinel)")
    df = df[df["week_number"] != 99]

    missing_alc = df["alc_code"].isna().sum()
    if missing_alc:
        issues.append(f"{missing_alc} missing alc_code filled with UNKNOWN")
    df["alc_code"] = df["alc_code"].fillna("UNKNOWN")

    df["state"] = df["state"].str.strip().str.title()

    log_step(log, "alc_issues", issues)
    log_step(log, "alc_clean_rows", len(df))
    return df


def clean_survey(df: pd.DataFrame, log: dict) -> pd.DataFrame:
    issues = []

    # Duplicate fellow+week responses
    dup_fwk = df.duplicated(subset=["fellow_id", "week"]).sum()
    if dup_fwk:
        issues.append(f"{dup_fwk} duplicate fellow_id+week rows — kept first response")
    df = df.drop_duplicates(subset=["fellow_id", "week"], keep="first")

    # Phone standardisation
    before_count = df["phone_number"].notna().sum()
    df["phone_std"] = df["phone_number"].apply(standardise_phone)
    after_count = df["phone_std"].notna().sum()
    failed = before_count - after_count
    issues.append(
        f"phone: {after_count}/{before_count} standardised to 234XXXXXXXXXX"
        + (f"; {failed} could not be recovered (set to null)" if failed else "")
    )

    log_step(log, "survey_issues", issues)
    log_step(log, "survey_clean_rows", len(df))
    return df


def clean_employer(df: pd.DataFrame, log: dict) -> pd.DataFrame:
    issues = []

    dup_rows = df.duplicated().sum()
    if dup_rows:
        issues.append(f"{dup_rows} duplicate rows dropped")
    df = df.drop_duplicates()

    missing_alc = df["alc_code"].isna().sum()
    if missing_alc:
        issues.append(f"{missing_alc} missing alc_code filled with UNKNOWN")
    df["alc_code"] = df["alc_code"].fillna("UNKNOWN")

    log_step(log, "employer_issues", issues)
    log_step(log, "employer_clean_rows", len(df))
    return df


def deduplicate_fellows(fellows: pd.DataFrame, survey: pd.DataFrame, log: dict) -> pd.DataFrame:
    """
    Deduplicate fellows across three keys:
      1. fellow_id  (already done in clean_fellows)
      2. email      (from survey)
      3. phone_std  (from survey)
    """
    issues = []
    before = len(fellows)

    # Bring email + phone from survey (most recent entry per fellow)
    phone_map = (survey.dropna(subset=["phone_std"])
                 .sort_values("week", ascending=False)
                 .drop_duplicates(subset="fellow_id")
                 [["fellow_id", "phone_std", "email"]])
    fellows = fellows.merge(phone_map, on="fellow_id", how="left")

    # Email dedup
    dup_email = fellows.duplicated(subset="email", keep="first").sum()
    if dup_email:
        issues.append(f"{dup_email} fellows removed as email duplicates")
    fellows = fellows.drop_duplicates(subset="email", keep="first")

    # Phone dedup (only where phone is not null)
    mask = fellows["phone_std"].notna()
    dup_phone = (mask & fellows.duplicated(subset="phone_std", keep="first")).sum()
    if dup_phone:
        issues.append(f"{dup_phone} fellows removed as phone duplicates")
    fellows = fellows[~(mask & fellows.duplicated(subset="phone_std", keep="first"))]

    issues.append(f"fellows: {before} → {len(fellows)} after full dedup")
    log_step(log, "dedup_issues", issues)
    log_step(log, "fellows_final_rows", len(fellows))
    return fellows


# ── Writer ────────────────────────────────────────────────────────────────────

def write_outputs(output_dir: str,
                  fellows: pd.DataFrame,
                  alc: pd.DataFrame,
                  survey: pd.DataFrame,
                  employer: pd.DataFrame):
    """
    Write cleaned CSVs to output_dir.
    Idempotent: overwrites existing files with identical content on repeat runs.
    """
    os.makedirs(output_dir, exist_ok=True)
    fellows.to_csv(os.path.join(output_dir, "fellows_cohort_clean.csv"), index=False)
    alc.to_csv(os.path.join(output_dir, "alc_weekly_log_clean.csv"), index=False)
    survey.to_csv(os.path.join(output_dir, "reflection_survey_clean.csv"), index=False)
    employer.to_csv(os.path.join(output_dir, "employer_engagement_clean.csv"), index=False)


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run_pipeline(fellows_path, alc_path, survey_path, employer_path, output_dir):
    start = time.time()
    run_log = {
        "run_timestamp": datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z",
        "inputs": {
            "fellows": fellows_path,
            "alc": alc_path,
            "survey": survey_path,
            "employer": employer_path,
        },
        "output_dir": output_dir,
        "steps": [],
        "rows_in": {},
        "rows_out": {},
        "issues_summary": [],
        "status": "RUNNING",
    }

    try:
        # ── Load ──────────────────────────────────────────────────────────────
        print("[ 1/6 ] Loading files...")
        fellows_raw  = load_fellows(fellows_path, run_log)
        alc_raw      = load_alc(alc_path, run_log)
        survey_raw   = load_survey(survey_path, run_log)
        employer_raw = load_employer(employer_path, run_log)

        run_log["rows_in"] = {
            "fellows":  len(fellows_raw),
            "alc":      len(alc_raw),
            "survey":   len(survey_raw),
            "employer": len(employer_raw),
        }

        # ── Clean ─────────────────────────────────────────────────────────────
        print("[ 2/6 ] Cleaning fellows_cohort...")
        fellows_clean  = clean_fellows(fellows_raw.copy(), run_log)

        print("[ 3/6 ] Cleaning alc_weekly_log...")
        alc_clean      = clean_alc(alc_raw.copy(), run_log)

        print("[ 4/6 ] Cleaning reflection_survey (phone standardisation)...")
        survey_clean   = clean_survey(survey_raw.copy(), run_log)

        print("[ 5/6 ] Cleaning employer_engagement...")
        employer_clean = clean_employer(employer_raw.copy(), run_log)

        # ── Dedup ─────────────────────────────────────────────────────────────
        fellows_final = deduplicate_fellows(fellows_clean, survey_clean, run_log)

        run_log["rows_out"] = {
            "fellows":  len(fellows_final),
            "alc":      len(alc_clean),
            "survey":   len(survey_clean),
            "employer": len(employer_clean),
        }

        # ── Write ─────────────────────────────────────────────────────────────
        print("[ 6/6 ] Writing cleaned files...")
        write_outputs(output_dir, fellows_final, alc_clean, survey_clean, employer_clean)

        elapsed = round(time.time() - start, 2)
        run_log["processing_time_seconds"] = elapsed
        run_log["status"] = "SUCCESS"

        # Build summary
        run_log["issues_summary"] = [
            item
            for step in run_log["steps"]
            for k, v in step.items()
            if "issues" in k
            for item in (v if isinstance(v, list) else [v])
        ]

        # Write run-log JSON
        log_path = os.path.join(output_dir, "run_log.json")
        with open(log_path, "w") as f:
            json.dump(run_log, f, indent=2, default=str)

        # ── Print summary ─────────────────────────────────────────────────────
        print("\n" + "=" * 55)
        print("  3MTT PIPELINE — RUN COMPLETE")
        print("=" * 55)
        print(f"  Status          : {run_log['status']}")
        print(f"  Run timestamp   : {run_log['run_timestamp']}")
        print(f"  Processing time : {elapsed}s")
        print(f"\n  Rows in  → fellows:{run_log['rows_in']['fellows']}  "
              f"alc:{run_log['rows_in']['alc']}  "
              f"survey:{run_log['rows_in']['survey']}  "
              f"employer:{run_log['rows_in']['employer']}")
        print(f"  Rows out → fellows:{run_log['rows_out']['fellows']}  "
              f"alc:{run_log['rows_out']['alc']}  "
              f"survey:{run_log['rows_out']['survey']}  "
              f"employer:{run_log['rows_out']['employer']}")
        print(f"\n  Issues found    : {len(run_log['issues_summary'])}")
        for issue in run_log["issues_summary"]:
            print(f"    • {issue}")
        print(f"\n  Output dir      : {output_dir}")
        print(f"  Run log         : {log_path}")
        print("=" * 55)

    except (FileNotFoundError, ValueError) as e:
        # Known, recoverable errors — clean message, no traceback
        run_log["status"] = "FAILED"
        run_log["error"] = str(e)
        run_log["processing_time_seconds"] = round(time.time() - start, 2)
        os.makedirs(output_dir, exist_ok=True)
        log_path = os.path.join(output_dir, "run_log.json")
        with open(log_path, "w") as f:
            json.dump(run_log, f, indent=2, default=str)
        print(f"\n[ERROR] Pipeline failed: {e}", file=sys.stderr)
        print(f"[ERROR] Run log written to: {log_path}", file=sys.stderr)
        sys.exit(1)

    except Exception as e:
        # Unexpected error — still clean message
        run_log["status"] = "FAILED"
        run_log["error"] = f"Unexpected error: {type(e).__name__}: {e}"
        run_log["processing_time_seconds"] = round(time.time() - start, 2)
        os.makedirs(output_dir, exist_ok=True)
        log_path = os.path.join(output_dir, "run_log.json")
        with open(log_path, "w") as f:
            json.dump(run_log, f, indent=2, default=str)
        print(f"\n[ERROR] Unexpected pipeline error: {type(e).__name__}: {e}", file=sys.stderr)
        print(f"[ERROR] Run log written to: {log_path}", file=sys.stderr)
        sys.exit(1)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="3MTT Data Pipeline — clean, dedup, and standardise 3MTT datasets."
    )
    parser.add_argument("--fellows",  required=True, help="Path to fellows_cohort.csv")
    parser.add_argument("--alc",      required=True, help="Path to alc_weekly_log.csv")
    parser.add_argument("--survey",   required=True, help="Path to reflection_survey.xlsx")
    parser.add_argument("--employer", required=True, help="Path to employer_engagement.csv")
    parser.add_argument("--output",   default="./cleaned",
                        help="Output directory for cleaned files (default: ./cleaned)")
    args = parser.parse_args()

    run_pipeline(args.fellows, args.alc, args.survey, args.employer, args.output)


if __name__ == "__main__":
    main()
