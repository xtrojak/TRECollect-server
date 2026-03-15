"""
Compute statistics and summary from curated sheet data and save to CSV.
"""

from __future__ import annotations

from typing import Dict

import pandas as pd


def compute_and_save_statistics(full_snapshot: Dict[str, pd.DataFrame]) -> None:
    """
    Compute statistics and summary from the full curated spreadsheet snapshot
    and write results to a CSV file.

    full_snapshot: sheet_name -> DataFrame (complete contents of each sheet after curation).
    output_csv_path: path for the output CSV (e.g. "curation_statistics.csv").
    """
    if not full_snapshot:
        return

    # Placeholder: one row per sheet with basic counts. Extend with real stats later.
    rows = []
    for sheet_name, df in full_snapshot.items():
        rows.append({
            "sheet": sheet_name,
            "row_count": len(df),
            "column_count": len(df.columns) if not df.empty else 0,
        })

    summary_df = pd.DataFrame(rows)
    summary_df.to_csv("statistics.csv", index=False)
    print(f">>> Statistics written to statistics.csv")
