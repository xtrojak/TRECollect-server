"""
Compute statistics and summary from curated sheet data and save to CSV.

For development: full_snapshot can be saved/loaded as pickle to avoid API calls.
"""

from __future__ import annotations
from typing import Dict, Set
import pandas as pd


def _barcode_columns_per_sheet(configs: Dict[str, dict]) -> Dict[str, Set[str]]:
    """From configs (config_name -> version -> config dict), build sheet_name -> set of barcode field labels."""
    name_to_barcodes: Dict[str, Set[str]] = {}
    for versions in configs.values():
        if not isinstance(versions, dict):
            continue
        for config in versions.values():
            if not isinstance(config, dict):
                continue
            name = config.get("name")
            if not name:
                continue
            fields = config.get("fields") or []
            barcodes = {f["label"] for f in fields if isinstance(f, dict) and f.get("type") == "barcode"}
            name_to_barcodes.setdefault(name, set()).update(barcodes)
    # Merged sheet LSI 14: union barcode fields from LSI 14-1, 14-2, 14-3.
    for part in ("LSI 14-1", "LSI 14-2", "LSI 14-3"):
        if part in name_to_barcodes:
            name_to_barcodes.setdefault("LSI 14", set()).update(name_to_barcodes[part])
    return name_to_barcodes


def _is_empty(val) -> bool:
    if pd.isna(val):
        return True
    return str(val).strip() == ""


def compute_and_save_statistics(data: Dict[str, pd.DataFrame], configs: Dict[str, dict]) -> None:
    """
    Compute statistics and summary from the full curated spreadsheet snapshot
    and write results to a CSV file.

    data: sheet_name -> DataFrame (complete contents of each sheet after curation).
    configs: reserved for later use.
    """
    barcode_by_sheet = _barcode_columns_per_sheet(configs) if configs else {}

    # Per site_id: which sheets and how many rows (sheet_name -> row count).
    per_site: Dict[str, Dict[str, int]] = {}

    for sheet_name, df in data.items():
        if df.empty or "Site ID" not in df.columns:
            continue
        site_col = df["Site ID"].dropna().astype(str).str.strip()
        for sid in site_col[site_col != ""].unique():
            count = int((site_col == sid).sum())
            if sid not in per_site:
                per_site[sid] = {}
            per_site[sid][sheet_name] = count

    if not per_site:
        return

    sheet_names = sorted(
        name for name, df in data.items()
        if not df.empty and "Site ID" in df.columns
    )
    columns = ["Site ID"] + sheet_names
    rows = []
    for site_id in sorted(per_site.keys()):
        row = {"Site ID": site_id}
        for sh in sheet_names:
            row[sh] = per_site[site_id].get(sh, 0)
        rows.append(row)

    out_df = pd.DataFrame(rows, columns=columns)
    out_df.to_csv("statistics/statistics.csv", index=False)
    print(f">>> Statistics written to statistics/statistics.csv")

    # Missing barcodes: per site, per sheet, list of barcode column names that are empty (in at least one row).
    missing_per_site: Dict[str, Dict[str, list]] = {}
    for sheet_name, df in data.items():
        if df.empty or "Site ID" not in df.columns:
            continue
        barcode_cols = barcode_by_sheet.get(sheet_name, set())
        if not barcode_cols:
            continue
        # Only consider barcode columns that exist in the DataFrame.
        barcode_cols = [c for c in barcode_cols if c in df.columns]
        if not barcode_cols:
            continue
        site_col = df["Site ID"].dropna().astype(str).str.strip()
        for sid in site_col[site_col != ""].unique():
            mask = (df["Site ID"].astype(str).str.strip() == sid)
            subset = df.loc[mask, barcode_cols]
            missing = [c for c in barcode_cols if subset[c].apply(_is_empty).any()]
            if not missing:
                continue
            if sid not in missing_per_site:
                missing_per_site[sid] = {}
            missing_per_site[sid][sheet_name] = sorted(missing)

    if missing_per_site:
        with open("statistics/missing_barcodes.md", "w", encoding="utf-8") as f:
            f.write("# Missing barcode warnings\n\n")
            for site_id in sorted(missing_per_site.keys()):
                f.write(f"## Site ID: {site_id}\n\n")
                for sheet_name in sorted(missing_per_site[site_id].keys()):
                    missing = missing_per_site[site_id][sheet_name]
                    f.write(f"### {sheet_name}\n\n")
                    f.write(", ".join(f"`{col}`" for col in missing) + "\n\n")
        print(">>> Missing barcode warnings written to statistics/missing_barcodes.md")

    # Duplicated barcodes: per barcode value, list all locations (sheet, column, site IDs).
    duplicates: Dict[str, Dict[str, Dict[str, list]]] = {}
    for sheet_name, df in data.items():
        if df.empty or "Site ID" not in df.columns:
            continue
        barcode_cols = barcode_by_sheet.get(sheet_name, set())
        if not barcode_cols:
            continue
        barcode_cols = [c for c in barcode_cols if c in df.columns]
        if not barcode_cols:
            continue
        site_col = df["Site ID"].astype(str).str.strip()
        for col in barcode_cols:
            values = df[col].astype(str).str.strip()
            # Ignore empty values
            values_nonempty = values[values != ""]
            if values_nonempty.empty:
                continue
            counts = values_nonempty.value_counts()
            dup_values = counts[counts > 1].index
            if not len(dup_values):
                continue
            for barcode_val in dup_values:
                mask = values == barcode_val
                site_ids = site_col[mask].dropna().str.strip()
                site_ids = sorted({sid for sid in site_ids if sid})
                if not site_ids:
                    continue
                # Organize as barcode_val -> sheet_name -> column_name -> [site_ids]
                for sid in site_ids:
                    duplicates.setdefault(barcode_val, {}).setdefault(sheet_name, {}).setdefault(col, []).append(sid)

    if duplicates:
        with open("statistics/duplicated_barcodes.md", "w", encoding="utf-8") as f:
            f.write("# Duplicated barcode errors\n\n")
            for barcode_val in sorted(duplicates.keys()):
                f.write(f"## Barcode `{barcode_val}`\n\n")
                for sheet_name in sorted(duplicates[barcode_val].keys()):
                    for col in sorted(duplicates[barcode_val][sheet_name].keys()):
                        site_ids = sorted({sid for sid in duplicates[barcode_val][sheet_name][col] if sid})
                        if not site_ids:
                            continue
                        sites_str = ", ".join(f"`{sid}`" for sid in site_ids)
                        f.write(f"- Sheet `{sheet_name}`, column `{col}`: Site ID(s) {sites_str}\n")
                f.write("\n")
        print(">>> Duplicated barcode errors written to statistics/duplicated_barcodes.md")
 