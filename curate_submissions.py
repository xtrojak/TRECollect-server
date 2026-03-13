from __future__ import annotations

import argparse
import datetime as dt
import os
from typing import Dict, List

import pandas as pd
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

from APIs.google_spreadsheets import GoogleAPI
from processing.utils import get_last_curation_timestamp, save_last_curation_timestamp


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
    raise NotImplementedError


def curate_rows_per_sheet(
    raw_rows: Dict[str, pd.DataFrame],
) -> Dict[str, pd.DataFrame]:
    """
    Apply curation rules to all newly collected rows per worksheet.

    Internally this will convert each DataFrame to a list of records,
    run them through `curate_submissions`, and convert back to DataFrames.
    """
    raise NotImplementedError


def write_curated_rows(
    google_api: GoogleAPI,
    target_sheet_id: str,
    curated_rows: Dict[str, pd.DataFrame],
) -> None:
    """
    Store curated rows into the target spreadsheet.

    Depending on the final design, this may append to existing tabs,
    overwrite them, or write into dedicated curated tabs.
    """
    raise NotImplementedError


def main(args: argparse.Namespace) -> None:
    """
    Top-level orchestration for LSI curation:

    1. Load configuration and last curation timestamp.
    2. Read new rows (based on `Submission date`) from all tabs.
    3. Curate the collected rows.
    4. Store curated data in the target spreadsheet.
    5. Update the stored timestamp.
    """
    load_dotenv("CONFIG.env")

    source_sheet_id = os.environ.get("RAW_SHEET_ID")
    lsi_target_sheet_id = os.environ.get("LSI_SHEET_LATEST_SUBMISSIONS_ID")

    google_api = GoogleAPI()

    now = dt.datetime.now(ZoneInfo("Europe/Paris"))
    last_timestamp = get_last_curation_timestamp()

    raw_rows = fetch_new_rows(google_api, source_sheet_id, last_timestamp)
    curated = curate_rows_per_sheet(raw_rows)

    write_curated_rows(google_api, lsi_target_sheet_id, curated)
    save_last_curation_timestamp(now)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Curate submissions from Google Sheets.")
    # Outline: we can later add options like --since, --dry-run, etc.
    args = parser.parse_args()
    main(args)
