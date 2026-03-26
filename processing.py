"""
Reusable processing functions for Export, Unsold, and Titan Stock CSVs.
"""

import csv
import io
import re
from collections import defaultdict
from datetime import datetime


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _read_semicolon_csv(filepath):
    """Read a semicolon-delimited CSV and return cleaned row dicts."""
    rows = []
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";", quotechar='"')
        reader.fieldnames = [h.strip() for h in reader.fieldnames]
        for row in reader:
            cleaned = {
                k.strip(): v.strip().rstrip(";").strip()
                for k, v in row.items() if k
            }
            rows.append(cleaned)
    return rows


def _read_comma_csv(filepath):
    """Read a comma-delimited CSV and return cleaned row dicts."""
    rows = []
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=",", quotechar='"')
        reader.fieldnames = [h.strip() for h in reader.fieldnames]
        for row in reader:
            cleaned = {
                k.strip(): v.strip() for k, v in row.items() if k
            }
            rows.append(cleaned)
    return rows


def _parse_q(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def _parse_date(date_str):
    date_str = (date_str or "").strip()
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%d/%m/%Y")
        return None if dt.year < 1900 else dt
    except ValueError:
        return None


def _parse_date_multi(date_str):
    """Parse dates in multiple formats found in Titan Stock CSV."""
    date_str = (date_str or "").strip()
    if not date_str:
        return None
    for fmt in (
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%m/%d/%y %H:%M",
        "%m/%d/%y",
    ):
        try:
            dt = datetime.strptime(date_str, fmt)
            return None if dt.year < 1900 else dt
        except ValueError:
            continue
    return None


def _extract_ean13(codes_str):
    """Extract all EAN-13 barcodes (exactly 13 digits) from a string."""
    return re.findall(r'\b\d{13}\b', codes_str or "")


def _format_date(dt):
    return dt.strftime("%d/%m/%Y") if dt else "Never"


def generate_csv_string(rows, fieldnames):
    """Render a list of dicts as a CSV string."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Unified output fields (5 columns)
# ---------------------------------------------------------------------------

OUTPUT_FIELDS = ["Name", "Quantity", "EAN_13 (PipCode)", "EAN_status", "Latest Delivery"]


# ---------------------------------------------------------------------------
# Export.CSV processing
# ---------------------------------------------------------------------------

def process_export(filepath):
    """Process an Export.CSV file. Returns (rows, stats)."""
    raw = _read_semicolon_csv(filepath)

    # Deduplicate and group by name
    groups = defaultdict(int)
    for row in raw:
        key = (
            row.get("name", ""),
            row.get("shortestcode", ""),
            row.get("preferredcode", ""),
        )
        groups[key] += _parse_q(row.get("Q", "0"))

    # Group by name for final output
    name_groups = defaultdict(lambda: {"total_quantity": 0, "ean13": set()})
    for (name, shortest, preferred), total_q in groups.items():
        entry = name_groups[name]
        entry["total_quantity"] += total_q
        entry["ean13"].update(_extract_ean13(shortest))
        entry["ean13"].update(_extract_ean13(preferred))

    rows = []
    for name in sorted(name_groups):
        e = name_groups[name]
        ean_list = sorted(e["ean13"])
        if ean_list:
            for ean in ean_list:
                rows.append({
                    "Name": name,
                    "Quantity": e["total_quantity"],
                    "EAN_13 (PipCode)": ean,
                    "EAN_status": "",
                    "Latest Delivery": "",
                })
        else:
            rows.append({
                "Name": name,
                "Quantity": e["total_quantity"],
                "EAN_13 (PipCode)": "",
                "EAN_status": "MISSING",
                "Latest Delivery": "",
            })

    stats = {
        "raw_rows": len(raw),
        "output_rows": len(rows),
        "unique_items": len(name_groups),
        "duplicates_removed": len(raw) - len(name_groups),
    }
    return rows, stats


# ---------------------------------------------------------------------------
# Unsold products.CSV processing
# ---------------------------------------------------------------------------

def process_unsold(filepath):
    """Process an Unsold_products.CSV file. Returns (rows, stats)."""
    raw = _read_semicolon_csv(filepath)

    # Deduplicate
    groups = {}
    for row in raw:
        key = (
            row.get("name", ""),
            row.get("reference", ""),
            row.get("prod_id", ""),
        )
        q = _parse_q(row.get("Q", "0"))
        dt = _parse_date(row.get("lastdelivered", ""))
        if key not in groups:
            groups[key] = {"Q": 0, "lastdelivered": dt}
        groups[key]["Q"] += q
        existing = groups[key]["lastdelivered"]
        if dt and (existing is None or dt > existing):
            groups[key]["lastdelivered"] = dt

    # Group by name for final output
    name_groups = defaultdict(
        lambda: {"total_quantity": 0, "ean13": set(), "latest_date": None}
    )
    for (name, reference, prod_id), data in groups.items():
        entry = name_groups[name]
        entry["total_quantity"] += data["Q"]
        entry["ean13"].update(_extract_ean13(reference))
        dt = data["lastdelivered"]
        if dt and (entry["latest_date"] is None or dt > entry["latest_date"]):
            entry["latest_date"] = dt

    rows = []
    for name in sorted(name_groups):
        e = name_groups[name]
        latest = _format_date(e["latest_date"])
        ean_list = sorted(e["ean13"])
        if ean_list:
            for ean in ean_list:
                rows.append({
                    "Name": name,
                    "Quantity": e["total_quantity"],
                    "EAN_13 (PipCode)": ean,
                    "EAN_status": "",
                    "Latest Delivery": latest,
                })
        else:
            rows.append({
                "Name": name,
                "Quantity": e["total_quantity"],
                "EAN_13 (PipCode)": "",
                "EAN_status": "MISSING",
                "Latest Delivery": latest,
            })

    stats = {
        "raw_rows": len(raw),
        "output_rows": len(rows),
        "unique_items": len(name_groups),
        "duplicates_removed": len(raw) - len(name_groups),
    }
    return rows, stats


# ---------------------------------------------------------------------------
# Titan Stock CSV processing
# ---------------------------------------------------------------------------

def process_titan_stock(filepath):
    """Process a Titan_Stock.CSV file. Returns (rows, stats)."""
    raw = _read_comma_csv(filepath)

    # Group by Drug Name
    name_groups = defaultdict(
        lambda: {
            "total_stock": 0, "total_doses": 0,
            "latest_input": None, "latest_dispense": None,
        }
    )
    for row in raw:
        drug_name = row.get("Drug Name", "").strip()
        entry = name_groups[drug_name]
        entry["total_stock"] += _parse_q(row.get("Stock Level", "0"))
        entry["total_doses"] += _parse_q(row.get("Number of doses", "0"))
        dt_in = _parse_date_multi(row.get("Last Inputed", ""))
        if dt_in and (entry["latest_input"] is None or dt_in > entry["latest_input"]):
            entry["latest_input"] = dt_in
        dt_disp = _parse_date_multi(row.get("Last Dispense Date", ""))
        if dt_disp and (entry["latest_dispense"] is None or dt_disp > entry["latest_dispense"]):
            entry["latest_dispense"] = dt_disp

    rows = []
    for name in sorted(name_groups):
        e = name_groups[name]
        rows.append({
            "Name": name,
            "Quantity": e["total_stock"],
            "EAN_13 (PipCode)": "",
            "EAN_status": "",
            "Latest Delivery": _format_date(e["latest_dispense"]),
        })

    stats = {
        "raw_rows": len(raw),
        "output_rows": len(rows),
        "unique_items": len(rows),
        "duplicates_removed": len(raw) - len(rows),
    }
    return rows, stats
