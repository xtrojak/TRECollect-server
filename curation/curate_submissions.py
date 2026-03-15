from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from APIs.google_spreadsheets import GoogleAPI

from .annotation import curate_value
from .output_rules import (
    apply_output_rules,
    get_output_rules,
    sheets_to_load_for_rules,
)


def fetch_new_rows(google_api: GoogleAPI, source_sheet_id: str, last_timestamp: dt.datetime) -> Dict[str, pd.DataFrame]:
    """
    Read all worksheets from the source spreadsheet and return only rows
    whose `Submission date` is newer than `last_timestamp`.

    The returned mapping is:

        {
            "<worksheet_name>": DataFrame([...]),
            ...
        }
    """
    new_rows: Dict[str, pd.DataFrame] = {}

    if not source_sheet_id:
        return new_rows

    # Normalise comparison timestamp to UTC so we can safely compare against
    # the `Submission date` values, which are in ISO format with a trailing 'Z'
    # (UTC designator), e.g. "2026-03-13T09:07:32.901575Z".
    if last_timestamp.tzinfo is not None:
        last_ts_utc = last_timestamp.astimezone(dt.timezone.utc)
    else:
        last_ts_utc = last_timestamp.replace(tzinfo=dt.timezone.utc)

    worksheet_names = google_api.get_all_worksheets(source_sheet_id)

    for sheet_name in worksheet_names:
        df = google_api.read_table(source_sheet_id, sheet_name)

        if df.empty:
            continue

        if "Submission date" not in df.columns:
            # Not a submissions worksheet; skip it.
            continue

        # Parse submission timestamps as UTC-aware datetimes.
        # pandas.to_datetime understands the trailing 'Z' as UTC.
        submission_times = pd.to_datetime(
            df["Submission date"],
            utc=True,
            errors="coerce",
        )

        # Keep only rows with a valid timestamp strictly newer than last_ts_utc.
        mask = submission_times.notna() & (submission_times > last_ts_utc)
        filtered = df.loc[mask].reset_index(drop=True)

        if not filtered.empty:
            new_rows[sheet_name] = filtered

    return new_rows


def curate_rows_per_sheet(
    raw_rows: Dict[str, pd.DataFrame],
    owncloud_images_token: str,
) -> Dict[str, pd.DataFrame]:
    """
    Apply curation rules to all newly collected rows per worksheet.

    Internally this applies `curate_value` cell-wise to supported sheets
    and returns a new mapping with curated DataFrames.
    """
    curated: Dict[str, pd.DataFrame] = {}
    print(">>> Curating sheets")

    for sheet_name, df in raw_rows.items():
        print(f">>>'{sheet_name}' with {len(df)} rows.")
        # For now we only curate LSI sheets; others are ignored.
        if not sheet_name.startswith("LSI"):
            continue

        if df.empty:
            continue

        # Apply curate_value to every cell
        curated_df = df.map(lambda v: curate_value(v, owncloud_images_token))
        curated[sheet_name] = curated_df

    return curated


def load_existing_sheets(
    google_api: GoogleAPI,
    target_sheet_id: str,
    sheet_names: Set[str],
) -> Dict[str, pd.DataFrame]:
    """Load current content of the given sheets from the target spreadsheet."""
    result: Dict[str, pd.DataFrame] = {}
    if not target_sheet_id:
        return result
    existing_tabs = google_api.get_all_worksheets(target_sheet_id)
    for name in sheet_names:
        if name in existing_tabs:
            result[name] = google_api.read_table(target_sheet_id, name)
        else:
            result[name] = pd.DataFrame()
    return result


def write_curated_rows(
    google_api: GoogleAPI,
    target_sheet_id: str,
    rows_to_write: Dict[str, pd.DataFrame],
    overwrite_sheets: Set[str],
) -> None:
    """
    Write prepared data to the target spreadsheet.

    - Sheets in overwrite_sheets: full overwrite of the tab.
    - All other sheets: append rows to the tab (create if missing).
    """
    if not target_sheet_id:
        return

    print(f">>> Writing sheets")

    for sheet_name, df in rows_to_write.items():
        print(f">>>'{sheet_name}' with {len(df)} rows.")
        if df.empty:
            continue
        if sheet_name in overwrite_sheets:
            google_api.overwrite_table(target_sheet_id, sheet_name, df)
        else:
            row_dicts = df.to_dict(orient="records")
            google_api.add_rows(target_sheet_id, sheet_name, row_dicts)


def _build_full_snapshot(
    rows_to_write: Dict[str, pd.DataFrame],
    overwrite_sheets: Set[str],
    existing_sheets: Dict[str, pd.DataFrame],
) -> Dict[str, pd.DataFrame]:
    """
    Build a local snapshot of the target spreadsheet after write.

    - Overwrite sheets: content is exactly rows_to_write[sheet].
    - Append sheets: content is existing_sheets[sheet] + rows_to_write[sheet].
    """
    full: Dict[str, pd.DataFrame] = {}
    for sheet_name, new_df in rows_to_write.items():
        if sheet_name in overwrite_sheets:
            full[sheet_name] = new_df.copy()
        else:
            existing = existing_sheets.get(sheet_name, pd.DataFrame())
            if existing.empty:
                full[sheet_name] = new_df.copy()
            else:
                full[sheet_name] = pd.concat([existing, new_df], ignore_index=True)
    return full


def run_curation(
    production_data: Dict[str, List[Dict[str, Any]]],
    logsheet_names: Dict[str, str],
    google_api: GoogleAPI,
    target_sheet_id: str,
    owncloud_images_token: str,
) -> Optional[Dict[str, pd.DataFrame]]:
    """
    Curate production data (in-memory) and write to the target spreadsheet.

    production_data: form_id -> list of submission row dicts (same as in process_latest_submissions).
    logsheet_names: form_id -> sheet name (e.g. "LSI 1", "LSI 14-1").

    Returns:
        Full snapshot of the target spreadsheet after write (sheet_name -> DataFrame),
        or None if nothing was written (no data / no target).
    """
    raw_rows: Dict[str, pd.DataFrame] = {}
    for form_id, rows in production_data.items():
        sheet_name = logsheet_names.get(form_id)
        if not sheet_name or not rows:
            continue
        raw_rows[sheet_name] = pd.DataFrame(rows)

    curated = curate_rows_per_sheet(raw_rows, owncloud_images_token)
    rules = get_output_rules()
    sheets_for_rules = sheets_to_load_for_rules(rules)
    # Load existing content for merge targets and for every curated sheet (needed for merge rules and full snapshot).
    all_sheet_names = sheets_for_rules | set(curated.keys())
    existing_sheets = load_existing_sheets(google_api, target_sheet_id, all_sheet_names)

    rows_to_write, overwrite_sheets = apply_output_rules(curated, existing_sheets, rules)
    write_curated_rows(google_api, target_sheet_id, rows_to_write, overwrite_sheets)

    full_snapshot = _build_full_snapshot(rows_to_write, overwrite_sheets, existing_sheets)
    return full_snapshot


if __name__ == "__main__":
    print("Curation is run automatically from process_latest_submissions.py. Run that script instead.")
