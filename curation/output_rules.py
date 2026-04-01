"""
Configurable output rules: merge sheets, upsert by key, etc.

Rules are applied after cell-level curation and before writing.
Each rule is a dict; supported types are documented in apply_output_rules.
"""

from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple, Optional
import datetime as dt
from collections import Counter
from zoneinfo import ZoneInfo

import pandas as pd
from APIs.google_spreadsheets import GoogleAPI


# Rule type for merging several source sheets into one target sheet,
# upserting by a key column and never overwriting non-empty with empty.
MERGE_UPSERT = "merge_upsert"
GATHER_WEATHER = "gather_weather"

# Default rules: add more dicts here or load from file later.
OUTPUT_RULES: List[Dict[str, Any]] = [
    {
        "type": MERGE_UPSERT,
        "sources": ["LSI 14-1", "LSI 14-2", "LSI 14-3"],
        "target": "LSI 14",
        "key_column": "Site ID",
        # For LSI 14 we also maintain a \"Total score\" column that is
        # recomputed as the sum of all other columns whose header contains
        # the word \"total\".
        "total_score_column": "Total score",
        "total_from_contains": "total",
    },
    {
        "type": GATHER_WEATHER,
        "sources": {"LSI 3": ["Soil square GPS coordinates - latitude", "Soil square GPS coordinates - longitude"],
                    "LSI 5": ["Sediment triangle GPS coordinates - latitude", "Sediment triangle GPS coordinates - longitude"],
                    "LSI 8": ["Water collection GPS coordinates - latitude", "Water collection GPS coordinates - longitude"]},
        "target": "LSI 1"
    },
]


def get_output_rules() -> List[Dict[str, Any]]:
    """Return the list of output rules (configurable)."""
    return list(OUTPUT_RULES)


def _is_empty(val: Any) -> bool:
    return pd.isna(val) or val == ""


def _is_zero(val: Any) -> bool:
    """
    Return True if val represents numeric zero.

    Treats 0, 0.0, and string forms like "0" or "0.0" as zero.
    """
    if isinstance(val, (int, float)):
        return val == 0
    if isinstance(val, str):
        try:
            return float(val) == 0.0
        except ValueError:
            return False
    return False


def _recompute_total_score(
    df: pd.DataFrame,
    total_column: str,
    keyword: str,
) -> pd.DataFrame:
    """
    Recompute a \"Total score\"-style column as the row-wise sum of all
    other columns whose name contains the given keyword.

    Non-numeric values are ignored in the sum. The total column itself
    is excluded from the inputs.
    """
    if df.empty:
        return df

    keyword_lower = keyword.lower()
    total_lower = total_column.lower()

    candidate_cols = [
        col
        for col in df.columns
        if keyword_lower in str(col).lower() and str(col).lower() != total_lower
    ]

    if not candidate_cols:
        return df

    numeric_part = df[candidate_cols].apply(pd.to_numeric, errors="coerce")
    df[total_column] = numeric_part.sum(axis=1, skipna=True)
    return df


def _upsert_by_key(
    combined_df: pd.DataFrame,
    new_df: pd.DataFrame,
    key_column: str,
) -> pd.DataFrame:
    """Merge new_df into combined_df by key_column; never overwrite non-empty with empty."""
    if key_column not in new_df.columns:
        return combined_df

    if combined_df.empty:
        combined_df = new_df.copy()
    elif key_column not in combined_df.columns:
        combined_df = combined_df.copy()
        combined_df[key_column] = pd.NA

    for _, row in new_df.iterrows():
        key_val = row.get(key_column)
        if _is_empty(key_val):
            continue

        existing_cols = list(combined_df.columns)
        row_dict = row.to_dict()

        for col in existing_cols:
            if col not in row_dict:
                row_dict[col] = pd.NA

        for col in row_dict:
            if col not in combined_df.columns:
                combined_df[col] = pd.NA

        matches = combined_df.index[combined_df[key_column] == key_val].tolist()

        if not matches:
            # Append one row without concat/reindex to avoid issues with non-unique indices.
            combined_df.loc[len(combined_df)] = row_dict
        else:
            idx = matches[0]
            for col, val in row_dict.items():
                if _is_empty(val):
                    # Source empty -> never copy.
                    continue

                existing_val = combined_df.at[idx, col]

                if _is_empty(existing_val):
                    # Target empty -> always take source.
                    combined_df.at[idx, col] = val
                elif _is_zero(existing_val) and not _is_zero(val):
                    # Target is zero, source is a different non-zero value -> replace.
                    combined_df.at[idx, col] = val
                else:
                    # Target has a non-empty, non-zero value -> keep it.
                    continue

    return combined_df


def apply_output_rules(
    curated_rows: Dict[str, pd.DataFrame],
    existing_sheets: Dict[str, pd.DataFrame],
    rules: List[Dict[str, Any]],
) -> Tuple[Dict[str, pd.DataFrame], Set[str]]:
    """
    Apply configured output rules to curated per-sheet data.

    Args:
        curated_rows: sheet_name -> DataFrame of curated new rows.
        existing_sheets: sheet_name -> DataFrame of current content (for merge targets).
        rules: list of rule dicts (e.g. from get_output_rules()).

    Returns:
        (rows_to_write, overwrite_sheet_names)
        - rows_to_write: sheet_name -> DataFrame to write (merged or as-is).
        - overwrite_sheet_names: sheets that must be overwritten (e.g. merge targets);
          all others are written by appending.
    """
    rows_to_write: Dict[str, pd.DataFrame] = {}
    overwrite_sheets: Set[str] = set()
    consumed_sources: Set[str] = set()

    for rule in rules:
        if rule.get("type") == MERGE_UPSERT:
            sources = rule.get("sources") or []
            target = rule.get("target")
            key_column = rule.get("key_column", "Site ID")
            if not target:
                continue

            for s in sources:
                consumed_sources.add(s)

            # 1) Start from current target table content.
            existing = existing_sheets.get(target, pd.DataFrame())
            combined = existing.copy()

            # 2) Upsert all new rows coming from all source sheets into the same output table.
            for src in sources:
                df = curated_rows.get(src, pd.DataFrame())
                if df.empty:
                    continue
                combined = _upsert_by_key(combined, df, key_column)

            # Optional rule extras: recompute a \"Total score\" column after merging.
            total_col = rule.get("total_score_column")
            total_kw = rule.get("total_from_contains")
            if total_col and total_kw:
                combined = _recompute_total_score(combined, total_col, total_kw)

            # 3) MERGE_UPSERT target is always replaced, never appended.
            rows_to_write[target] = combined
            overwrite_sheets.add(target)
        elif rule.get("type") == GATHER_WEATHER:
            target = rule.get("target")
            source_defs = rule.get("sources") or {}
            if not target or target not in curated_rows or not isinstance(source_defs, dict):
                continue
            centroids = _collect_site_centroids(curated_rows, source_defs)
            if not centroids:
                continue
            try:
                google_api = GoogleAPI()
                updated = _fill_weather_columns(
                    curated_rows[target],
                    centroids,
                    google_api,
                )
                curated_rows[target] = updated
            except Exception as e:
                print(f">>> GATHER_WEATHER: rule failed, skipping weather fill: {e}")

    for sheet_name, df in curated_rows.items():
        if sheet_name in consumed_sources:
            continue
        if df.empty:
            continue
        rows_to_write[sheet_name] = df

    return rows_to_write, overwrite_sheets


def sheets_to_load_for_rules(rules: List[Dict[str, Any]]) -> Set[str]:
    """Return set of sheet names that must be read from the target for rules (e.g. merge targets)."""
    out: Set[str] = set()
    for rule in rules:
        if rule.get("type") == MERGE_UPSERT and rule.get("target"):
            out.add(rule["target"])
    return out


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_iso_utc(text: Any) -> Optional[dt.datetime]:
    if text is None:
        return None
    s = str(text).strip()
    if not s:
        return None
    try:
        t = pd.to_datetime(s, utc=True, errors="coerce")
        if pd.isna(t):
            return None
        return t.to_pydatetime()
    except Exception:
        return None


def _parse_sampling_dt_utc(date_val: Any, time_val: Any) -> Optional[dt.datetime]:
    if date_val is None or time_val is None:
        return None
    date_s = str(date_val).strip()
    time_s = str(time_val).strip()
    if not date_s or not time_s:
        return None
    # Sampling date/time values are stored in local CET/CEST time (Europe/Paris),
    # not UTC. Parse as local time first, then convert to UTC.
    t = pd.to_datetime(f"{date_s} {time_s}", errors="coerce")
    if pd.isna(t):
        return None
    py_t = t.to_pydatetime()
    if py_t.tzinfo is None:
        py_t = py_t.replace(tzinfo=ZoneInfo("Europe/Paris"))
    return py_t.astimezone(dt.timezone.utc)


def _avg(values: List[Optional[float]]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def _wind_cardinal_to_acronym(cardinal: Optional[str]) -> Optional[str]:
    if not cardinal:
        return None
    c = str(cardinal).strip().upper()
    mapping = {
        "NORTH": "N",
        "NORTH_NORTHEAST": "NNE",
        "NORTHEAST": "NE",
        "EAST_NORTHEAST": "ENE",
        "EAST": "E",
        "EAST_SOUTHEAST": "ESE",
        "SOUTHEAST": "SE",
        "SOUTH_SOUTHEAST": "SSE",
        "SOUTH": "S",
        "SOUTH_SOUTHWEST": "SSW",
        "SOUTHWEST": "SW",
        "WEST_SOUTHWEST": "WSW",
        "WEST": "W",
        "WEST_NORTHWEST": "WNW",
        "NORTHWEST": "NW",
        "NORTH_NORTHWEST": "NNW",
        "N": "N",
        "NNE": "NNE",
        "NE": "NE",
        "ENE": "ENE",
        "E": "E",
        "ESE": "ESE",
        "SE": "SE",
        "SSE": "SSE",
        "S": "S",
        "SSW": "SSW",
        "SW": "SW",
        "WSW": "WSW",
        "W": "W",
        "WNW": "WNW",
        "NW": "NW",
        "NNW": "NNW",
    }
    return mapping.get(c, c)


def _collect_site_centroids(
    curated_rows: Dict[str, pd.DataFrame],
    source_defs: Dict[str, List[str]],
) -> Dict[str, Tuple[float, float]]:
    sums_by_site: Dict[str, List[float]] = {}  # site -> [sum_lat, sum_lon, count]
    for sheet_name, coord_cols in source_defs.items():
        df = curated_rows.get(sheet_name, pd.DataFrame())
        if df.empty or "Site ID" not in df.columns or len(coord_cols) < 2:
            continue
        lat_pattern, lon_pattern = coord_cols[0], coord_cols[1]

        # dynamic columns can have prefixes; use suffix matching there.
        lat_cols = [c for c in df.columns if str(c) == lat_pattern or str(c).endswith(lat_pattern)]
        lon_cols = [c for c in df.columns if str(c) == lon_pattern or str(c).endswith(lon_pattern)]
        if not lat_cols or not lon_cols:
            continue

        for _, row in df.iterrows():
            site_id = str(row.get("Site ID", "")).strip()
            if not site_id:
                continue
            for lat_col in lat_cols:
                lon_col = lat_col.replace(lat_pattern, lon_pattern)
                if lon_col not in df.columns:
                    continue
                lat = _to_float(row.get(lat_col))
                lon = _to_float(row.get(lon_col))
                if lat is None or lon is None:
                    continue
                rec = sums_by_site.setdefault(site_id, [0.0, 0.0, 0.0])
                rec[0] += lat
                rec[1] += lon
                rec[2] += 1.0

    centroids: Dict[str, Tuple[float, float]] = {}
    for site_id, (sum_lat, sum_lon, count) in sums_by_site.items():
        if count <= 0:
            continue
        centroids[site_id] = (sum_lat / count, sum_lon / count)
    return centroids


def _fill_weather_columns(
    input_df: pd.DataFrame,
    centroids: Dict[str, Tuple[float, float]],
    google_api: GoogleAPI,
    max_sampling_end_age_hours: int = 24
) -> pd.DataFrame:
    if input_df.empty:
        return input_df

    weather_cols = [
        "Cloud coverage",
        "General weather description",
        "Temperature (°C)",
        "Dewpoint temperature (°C)",
        "Pressure (hPa)",
        "Wind speed (km/h)",
        "Wind direction",
        "Humidity (%)",
        "Precipitation rate (mm)",
    ]
    out = input_df.copy()
    for col in weather_cols:
        if col not in out.columns:
            out[col] = pd.NA

    now_utc = dt.datetime.now(dt.timezone.utc)
    for idx, row in out.iterrows():
        # Fill only if at least one weather field is missing.
        missing_cols = [c for c in weather_cols if _is_empty(row.get(c))]
        if not missing_cols:
            continue

        site_id = str(row.get("Site ID", "")).strip()
        if not site_id:
            continue

        sample_start = _parse_sampling_dt_utc(row.get("Sampling date start"), row.get("Sampling time start"))
        sample_end = _parse_sampling_dt_utc(row.get("Sampling date end"), row.get("Sampling time end"))
        if sample_start is None or sample_end is None:
            continue
        if sample_end < sample_start:
            sample_start, sample_end = sample_end, sample_start

        # Weather API provides only recent history; gate on sampling end, not submission time.
        age_since_sampling_end = now_utc - sample_end
        if age_since_sampling_end < dt.timedelta(0) or age_since_sampling_end > dt.timedelta(hours=max_sampling_end_age_hours):
            continue

        centroid = centroids.get(site_id)
        if not centroid:
            continue

        selected_all = []
        lat, lon = centroid
        try:
            payload = google_api.weather_history_hours_lookup(lat, lon)
        except Exception as e:
            print(f">>> GATHER_WEATHER: weather API call failed for Site ID '{site_id}': {e}")
            continue
        history = payload.get("historyHours", [])
        if not isinstance(history, list):
            continue
        for hour in history:
            interval = hour.get("interval", {}) if isinstance(hour, dict) else {}
            start_ts = _parse_iso_utc(interval.get("startTime"))
            end_ts = _parse_iso_utc(interval.get("endTime"))
            if start_ts is None or end_ts is None:
                continue
            if start_ts >= sample_start and end_ts <= sample_end:
                selected_all.append(hour)

        if not selected_all:
            continue

        pressure = _avg([_to_float(h.get("airPressure", {}).get("meanSeaLevelMillibars")) for h in selected_all])
        cloud = _avg([_to_float(h.get("cloudCover")) for h in selected_all])
        dew = _avg([_to_float(h.get("dewPoint", {}).get("degrees")) for h in selected_all])
        precip = _avg([_to_float(h.get("precipitation", {}).get("qpf", {}).get("quantity")) for h in selected_all])
        humidity = _avg([_to_float(h.get("relativeHumidity")) for h in selected_all])
        temperature = _avg([_to_float(h.get("temperature", {}).get("degrees")) for h in selected_all])
        wind_speed = _avg([_to_float(h.get("wind", {}).get("speed", {}).get("value")) for h in selected_all])

        wind_cardinals = [
            str(h.get("wind", {}).get("direction", {}).get("cardinal", "")).strip()
            for h in selected_all
            if str(h.get("wind", {}).get("direction", {}).get("cardinal", "")).strip()
        ]
        weather_descriptions = [
            str(h.get("weatherCondition", {}).get("description", {}).get("text", "")).strip()
            for h in selected_all
            if str(h.get("weatherCondition", {}).get("description", {}).get("text", "")).strip()
        ]
        wind_direction = Counter(wind_cardinals).most_common(1)[0][0] if wind_cardinals else None
        wind_direction = _wind_cardinal_to_acronym(wind_direction)
        weather_description = Counter(weather_descriptions).most_common(1)[0][0] if weather_descriptions else None

        computed = {
            "Cloud coverage": cloud,
            "General weather description": weather_description,
            "Temperature (°C)": temperature,
            "Dewpoint temperature (°C)": dew,
            "Pressure (hPa)": pressure,
            "Wind speed (km/h)": wind_speed,
            "Wind direction": wind_direction,
            "Humidity (%)": humidity,
            "Precipitation rate (mm)": precip,
        }
        for col, val in computed.items():
            if col in missing_cols and not _is_empty(val):
                out.at[idx, col] = val

    return out
