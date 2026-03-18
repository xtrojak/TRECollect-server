"""
Compute statistics and summary from curated sheet data and save to CSV.

For development: full_snapshot can be saved/loaded as pickle to avoid API calls.
"""

from __future__ import annotations
from typing import Dict, Set
import json
import pandas as pd


def _barcode_columns_per_sheet(configs: Dict[str, dict]) -> Dict[str, Set[str]]:
    """From configs (config_name -> version -> config dict), build sheet_name -> set of barcode field labels.

    Also supports dynamic fields with subFields that contain barcode fields.
    """
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
            barcodes: Set[str] = set()
            for f in fields:
                if not isinstance(f, dict):
                    continue
                f_type = f.get("type")
                if f_type == "barcode":
                    label = f.get("label")
                    if label:
                        barcodes.add(label)
                elif f_type == "dynamic":
                    # Dynamic fields can have nested subFields; collect any barcode labels there.
                    for sf in f.get("subFields") or []:
                        if not isinstance(sf, dict):
                            continue
                        if sf.get("type") == "barcode":
                            label = sf.get("label")
                            if label:
                                barcodes.add(label)
            if barcodes:
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


def _compute_site_overview(data: Dict[str, pd.DataFrame]) -> None:
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
        with open("statistics/statistics.md", "w", encoding="utf-8") as f:
            pass
        print(">>> Site overview written to statistics/statistics.md (no sites)")
        return

    try:
        with open("curation/expected_numbers.json", encoding="utf-8") as f:
            expected_raw = json.load(f)
    except FileNotFoundError:
        expected_raw = {}

    expected_counts = {k: int(v) for k, v in expected_raw.items()}

    lines: list[str] = []
    sheet_names = sorted(expected_counts.keys())

    for site_id in sorted(per_site.keys()):
        issues: list[str] = []
        for sheet in sheet_names:
            expected = expected_counts.get(sheet, 0)
            actual = per_site[site_id].get(sheet, 0)
            if actual == expected:
                continue
            diff = actual - expected
            if diff < 0:
                issues.append(f"- `{sheet}`: missing {-diff} (expected {expected}, found {actual})")
            elif diff > 0:
                issues.append(f"- `{sheet}`: extra {diff} (expected {expected}, found {actual})")

        if not issues:
            lines.append(f"## {site_id} ✓\n")
            lines.append("All sheets present with expected counts.\n")
        else:
            lines.append(f"## {site_id} ✗\n")
            lines.extend(issues)
            lines.append("")

    with open("statistics/statistics.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")
    print(">>> Site overview written to statistics/statistics.md")


def _compute_missing_barcodes(
    data: Dict[str, pd.DataFrame],
    barcode_by_sheet: Dict[str, Set[str]],
) -> None:
    missing_per_site: Dict[str, Dict[str, list]] = {}
    for sheet_name, df in data.items():
        if df.empty or "Site ID" not in df.columns:
            continue
        patterns = barcode_by_sheet.get(sheet_name, set())
        if not patterns:
            continue
        # Match barcode columns by suffix to support dynamic instances that prefix labels.
        barcode_cols = [
            col
            for col in df.columns
            if any(pat and str(col).endswith(pat) for pat in patterns)
        ]
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

    with open("statistics/missing_barcodes.md", "w", encoding="utf-8") as f:
        if not missing_per_site:
            print(">>> Missing barcode warnings written to statistics/missing_barcodes.md (none)")
            return

        for site_id in sorted(missing_per_site.keys()):
            f.write(f"## Site ID: {site_id}\n\n")
            for sheet_name in sorted(missing_per_site[site_id].keys()):
                missing = missing_per_site[site_id][sheet_name]
                f.write(f"* **{sheet_name}**: ")
                f.write(", ".join(f"`{col}`" for col in missing) + "\n\n")
    print(">>> Missing barcode warnings written to statistics/missing_barcodes.md")


def _compute_duplicated_barcodes(
    data: Dict[str, pd.DataFrame],
    barcode_by_sheet: Dict[str, Set[str]],
) -> None:
    # duplicates: barcode_val -> sheet_name -> column_name -> [site_ids]
    duplicates: Dict[str, Dict[str, Dict[str, list]]] = {}
    for sheet_name, df in data.items():
        if df.empty or "Site ID" not in df.columns:
            continue
        patterns = barcode_by_sheet.get(sheet_name, set())
        if not patterns:
            continue
        barcode_cols = [col for col in df.columns if any(pat and str(col).endswith(pat) for pat in patterns)]
        if not barcode_cols:
            continue
        site_col = df["Site ID"].astype(str).str.strip()
        for col in barcode_cols:
            values = df[col].astype(str).str.strip()
            for idx, barcode_val in enumerate(values):
                barcode_val = barcode_val.strip()
                if not barcode_val:
                    continue
                sid = site_col.iloc[idx].strip()
                if not sid:
                    continue
                duplicates.setdefault(barcode_val, {}).setdefault(sheet_name, {}).setdefault(col, []).append(sid)

    # Keep only barcodes that occur more than once anywhere (across sites and sheets).
    filtered_duplicates: Dict[str, Dict[str, Dict[str, list]]] = {}
    for barcode_val, sheets in duplicates.items():
        total_occurrences = sum(len(site_ids) for cols in sheets.values() for site_ids in cols.values())
        if total_occurrences > 1:
            filtered_duplicates[barcode_val] = sheets

    with open("statistics/duplicated_barcodes.md", "w", encoding="utf-8") as f:
        if not filtered_duplicates:
            print(">>> Duplicated barcode errors written to statistics/duplicated_barcodes.md (none)")
            return

        for barcode_val in sorted(filtered_duplicates.keys()):
            f.write(f"### {barcode_val}\n\n")
            for sheet_name in sorted(filtered_duplicates[barcode_val].keys()):
                for col in sorted(filtered_duplicates[barcode_val][sheet_name].keys()):
                    site_ids = sorted({sid for sid in filtered_duplicates[barcode_val][sheet_name][col] if sid})
                    if not site_ids:
                        continue
                    sites_str = ", ".join(f"`{sid}`" for sid in site_ids)
                    f.write(f"* `{sheet_name}` | `{col}` | {sites_str}\n")
            f.write("\n\n")
    print(">>> Duplicated barcode errors written to statistics/duplicated_barcodes.md")


def _compute_coordinates(data: Dict[str, pd.DataFrame]) -> None:
    rows: list[dict] = []

    def _add_row(site_id: str, label: str, lat, lon) -> None:
        if _is_empty(lat) or _is_empty(lon):
            return
        rows.append(
            {
                "label": label,
                "Site ID": site_id,
                "latitude": lat,
                "longitude": lon,
            }
        )

    lsi3 = data.get("LSI 3")
    if lsi3 is not None and not lsi3.empty and "Site ID" in lsi3.columns:
        for _, row in lsi3.iterrows():
            site_id = str(row.get("Site ID", "")).strip()
            if not site_id:
                continue
            lat = row.get("Soil square GPS coordinates - latitude")
            lon = row.get("Soil square GPS coordinates - longitude")
            transect = str(row.get("Transect number", "")).strip()
            square = str(row.get("Square number", "")).strip()
            label = f"{site_id}: soil transect {transect} square {square}"
            _add_row(site_id, label, lat, lon)

    lsi5 = data.get("LSI 5")
    if lsi5 is not None and not lsi5.empty and "Site ID" in lsi5.columns:
        for _, row in lsi5.iterrows():
            site_id = str(row.get("Site ID", "")).strip()
            if not site_id:
                continue
            lat = row.get("Sediment triangle GPS coordinates - latitude")
            lon = row.get("Sediment triangle GPS coordinates - longitude")
            transect = str(row.get("Transect number", "")).strip()
            triangle = str(row.get("Triangle number", "")).strip()
            label = f"{site_id}: sediment transect {transect} triangle {triangle}"
            _add_row(site_id, label, lat, lon)

    lsi8 = data.get("LSI 8")
    if lsi8 is not None and not lsi8.empty and "Site ID" in lsi8.columns:
        for col in lsi8.columns:
            if not col.startswith("Water collection - Carboy ") or "GPS coordinates - latitude" not in col:
                continue
            try:
                prefix, rest = col.split("Carboy ", 1)
                carboy_num = rest.split(" -", 1)[0].strip()
            except ValueError:
                continue
            lon_col = col.replace("GPS coordinates - latitude", "GPS coordinates - longitude")
            if lon_col not in lsi8.columns:
                continue
            for _, row in lsi8.iterrows():
                site_id = str(row.get("Site ID", "")).strip()
                if not site_id:
                    continue
                lat = row.get(col)
                lon = row.get(lon_col)
                label = f"{site_id}: shoreline transect - Carboy {carboy_num}"
                _add_row(site_id, label, lat, lon)

    if not rows:
        pd.DataFrame(columns=["label", "Site ID", "latitude", "longitude"]).to_csv(
            "statistics/coords.csv", index=False
        )
        print(">>> Coordinates written to statistics/coords.csv (none)")
        return

    df_out = pd.DataFrame(rows)
    df_out.to_csv("statistics/coords.csv", index=False)
    print(">>> Coordinates written to statistics/coords.csv")


def compute_and_save_statistics(data: Dict[str, pd.DataFrame], configs: Dict[str, dict]) -> None:
    """
    Orchestrate computation of statistics and derived summaries.

    data: sheet_name -> DataFrame (complete contents of each sheet after curation).
    configs: configuration dicts used for determining barcode columns.
    """
    barcode_by_sheet = _barcode_columns_per_sheet(configs) if configs else {}

    _compute_site_overview(data)
    _compute_missing_barcodes(data, barcode_by_sheet)
    _compute_duplicated_barcodes(data, barcode_by_sheet)
    _compute_coordinates(data)
 