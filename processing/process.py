"""
Curate form XML data into a flat label -> value mapping using config field definitions.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from processing.xml import FormXMLParser

# Types that can be resolved to a single value from the XML (handled directly).
SIMPLE_TYPES = frozenset({
    "text",
    "textarea",
    "number",
    "date",
    "time",
    "select",
    "multiselect",
    "select_image",
    "multiselect_image",
    "barcode",
    "photo",
    "checkbox"
})

# Types that require custom extraction (structure preserved for later implementation).
SPECIAL_TYPES = frozenset({
    "table",
    "dynamic",
    "gps"
})


def _find_field_by_id(fields_list: list[dict[str, Any]], field_id: str) -> dict[str, Any] | None:
    """Find the first field in the fields list whose id matches field_id."""
    for f in fields_list:
        if f.get("id") == field_id:
            return f
    return None


def _normalize_to_list(obj: Any) -> list[dict[str, Any]]:
    """Return a list of dicts from instance/list or single-dict XML output."""
    if obj is None:
        return []
    if isinstance(obj, list):
        return obj
    return [obj]


def _get_field_value(field_data: dict[str, Any], field_type: str) -> Any:
    """Extract the value to store for a field (reused for simple and dynamic subfields)."""
    if field_type == "photo":
        return field_data.get("photoFileName")
    if field_type in ["multiselect", "multiselect_image"]:
        return field_data.get("values")
    return field_data.get("value")


def _extract_simple_value(field_data: dict[str, Any], field_type: str) -> Any:
    """Extract a single value from XML field data for simple types. Returns None if missing."""
    if field_type == "photo":
        return field_data.get("photoFileName")
    if field_type in ["multiselect", "multiselect_image"]:
        return field_data.get("values")
    return field_data.get("value")


def _extract_table_flat(
    field_data: dict[str, Any],
    label: str,
    row_names: list[str],
    column_names: list[str],
) -> dict[str, Any]:
    """
    Parse tableData JSON and return flat dict of "{label} - {row} - {column}" -> value.
    tableData is expected to be JSON: either a 2D array (list of rows, each row list of
    column values in order) or a dict row_name -> { column_name -> value }.
    """
    out: dict[str, Any] = {}
    raw = field_data.get("tableData") or field_data.get("value")
    if raw is None:
        return out
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return out
    else:
        data = raw
    if not row_names or not column_names:
        return out
    if isinstance(data, list):
        # 2D array: data[i][j] = value for row i, column j
        for i, row_name in enumerate(row_names):
            if i >= len(data):
                break
            row = data[i] if isinstance(data[i], list) else []
            for j, col_name in enumerate(column_names):
                if j < len(row):
                    key = f"{label} - {row_name} - {col_name}"
                    out[key] = row[j]
    elif isinstance(data, dict):
        # dict: data[row_name][column_name] = value
        for row_name in row_names:
            row_cells = data.get(row_name)
            if not isinstance(row_cells, dict):
                continue
            for col_name in column_names:
                if col_name in row_cells:
                    key = f"{label} - {row_name} - {col_name}"
                    out[key] = row_cells[col_name]
    return out


def process_site(form_parser: FormXMLParser, config: dict[str, Any]) -> dict[str, Any]:
    """
    Build an output dict mapping config label -> value from the form XML.

    Matching is done by field id (same in config and XML). Config defines type;
    simple types (text, number, date, time, select, barcode, photo, gps, etc.) are
    resolved to a value; special types (table, dynamic, multiselect, ...) are stored
    as raw structures for later handling.
    """
    out: dict[str, Any] = {}
    fields_list = form_parser.fields
    config_fields = config.get("fields") or []

    for field_def in config_fields:
        field_id = field_def.get("id")
        label = field_def.get("label") or field_id
        field_type = field_def.get("type")

        if field_type in SIMPLE_TYPES:
            field_data = _find_field_by_id(fields_list, field_id)
            if field_data is not None:
                value = _extract_simple_value(field_data, field_type)
                out[label] = value
        else:
            field_data = _find_field_by_id(fields_list, field_id)
            if field_data is not None:
                if field_type == "dynamic":
                    raw = field_data.get("dynamicInstances") or {}
                    instances = _normalize_to_list(raw.get("instance"))
                    instance_name = field_def.get("instance_name") or "Instance"
                    subfield_defs = field_def.get("subFields") or []
                    for idx, instance in enumerate(instances):
                        subfield_list = _normalize_to_list(instance.get("subField"))
                        # Use instance's "number" attribute + 1 for display (1-based)
                        try:
                            display_index = int(instance.get("number", idx)) + 1
                        except (TypeError, ValueError):
                            display_index = idx + 1
                        for sub_def in subfield_defs:
                            sub_id = sub_def.get("id")
                            sub_label = sub_def.get("label") or sub_id
                            sub_type = sub_def.get("type") or "text"
                            sub_data = _find_field_by_id(subfield_list, sub_id)
                            key_base = f"{label} - {instance_name} {display_index} - {sub_label}"
                            if sub_type == "gps" and sub_data:
                                out[f"{key_base} - latitude"] = sub_data.get("gpsLatitude")
                                out[f"{key_base} - longitude"] = sub_data.get("gpsLongitude")
                            else:
                                out[key_base] = _get_field_value(sub_data, sub_type) if sub_data else None
                elif field_type == "table":
                    row_names = field_def.get("rows") or []
                    column_names = field_def.get("columns") or []
                    table_flat = _extract_table_flat(field_data, label, row_names, column_names)
                    out.update(table_flat)
                elif field_type == "gps":
                    out[f"{label} - latitude"] = field_data.get("gpsLatitude")
                    out[f"{label} - longitude"] = field_data.get("gpsLongitude")

    return out
