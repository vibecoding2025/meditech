"""
Reusable processing functions for Export, Unsold, and Titan Stock CSVs.
Extracted from process_data.py and process_unsold.py.
"""

import csv
import io
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
        "%d/%m/%Y %H:%M:%S",   # 25/03/2026 17:15:56
        "%d/%m/%Y",             # 25/03/2026
        "%m/%d/%y %H:%M",      # 3/25/26 13:31
        "%m/%d/%y",             # 3/25/26
    ):
        try:
            dt = datetime.strptime(date_str, fmt)
            return None if dt.year < 1900 else dt
        except ValueError:
            continue
    return None


def _format_date(dt):
    return dt.strftime("%d/%m/%Y") if dt else "Never"


def generate_csv_string(rows, fieldnames):
    """Render a list of dicts as a CSV string."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Export.CSV processing
# ---------------------------------------------------------------------------

EXPORT_CLEAN_FIELDS = ["name", "shortestcode", "longestcode", "preferredcode", "Q"]
EXPORT_SUMMARY_FIELDS = ["name", "total_quantity", "all_codes"]


def process_export(filepath):
    """Process an Export.CSV file. Returns (clean_rows, summary_rows, stats)."""
    raw = _read_semicolon_csv(filepath)

    # Step 1 – deduplicate
    groups = defaultdict(int)
    for row in raw:
        key = (
            row.get("name", ""),
            row.get("shortestcode", ""),
            row.get("longestcode", ""),
            row.get("preferredcode", ""),
        )
        groups[key] += _parse_q(row.get("Q", "0"))

    clean = []
    for (name, shortest, longest, preferred), total_q in sorted(groups.items()):
        clean.append({
            "name": name,
            "shortestcode": shortest,
            "longestcode": longest,
            "preferredcode": preferred,
            "Q": total_q,
        })

    # Step 2 – summary by name
    name_groups = defaultdict(lambda: {"total_quantity": 0, "codes": set()})
    for row in clean:
        entry = name_groups[row["name"]]
        entry["total_quantity"] += row["Q"]
        for field in ("shortestcode", "longestcode"):
            code = row.get(field, "").strip()
            if code:
                entry["codes"].add(code)
        preferred = row.get("preferredcode", "")
        if preferred:
            for part in preferred.split(","):
                code = part.strip()
                if code:
                    entry["codes"].add(code)

    summary = []
    for name in sorted(name_groups):
        e = name_groups[name]
        summary.append({
            "name": name,
            "total_quantity": e["total_quantity"],
            "all_codes": "; ".join(sorted(e["codes"])),
        })

    stats = {
        "raw_rows": len(raw),
        "clean_rows": len(clean),
        "summary_rows": len(summary),
        "duplicates_removed": len(raw) - len(clean),
    }
    return clean, summary, stats


# ---------------------------------------------------------------------------
# Unsold products.CSV processing
# ---------------------------------------------------------------------------

UNSOLD_CLEAN_FIELDS = ["name", "prod_id", "reference", "lastdelivered", "Q"]
UNSOLD_SUMMARY_FIELDS = ["name", "total_quantity", "latest_delivery", "prod_ids", "all_codes"]


def process_unsold(filepath):
    """Process an Unsold_products.CSV file. Returns (clean_rows, summary_rows, stats)."""
    raw = _read_semicolon_csv(filepath)

    # Step 1 – deduplicate
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

    clean = []
    for (name, reference, prod_id), data in sorted(groups.items()):
        clean.append({
            "name": name,
            "prod_id": prod_id,
            "reference": reference,
            "lastdelivered": _format_date(data["lastdelivered"]),
            "Q": data["Q"],
        })

    # Step 2 – summary by name
    name_groups = defaultdict(
        lambda: {"total_quantity": 0, "codes": set(), "prod_ids": set(), "latest_date": None}
    )
    for row in clean:
        entry = name_groups[row["name"]]
        entry["total_quantity"] += row["Q"]
        ref = row.get("reference", "")
        if ref:
            for part in ref.split(","):
                code = part.strip()
                if code:
                    entry["codes"].add(code)
        pid = row.get("prod_id", "")
        if pid:
            entry["prod_ids"].add(pid)
        dt = _parse_date(row.get("lastdelivered", ""))
        if dt and (entry["latest_date"] is None or dt > entry["latest_date"]):
            entry["latest_date"] = dt

    summary = []
    for name in sorted(name_groups):
        e = name_groups[name]
        summary.append({
            "name": name,
            "total_quantity": e["total_quantity"],
            "latest_delivery": _format_date(e["latest_date"]),
            "prod_ids": "; ".join(sorted(e["prod_ids"])),
            "all_codes": "; ".join(sorted(e["codes"])),
        })

    stats = {
        "raw_rows": len(raw),
        "clean_rows": len(clean),
        "summary_rows": len(summary),
        "duplicates_removed": len(raw) - len(clean),
    }
    return clean, summary, stats


# ---------------------------------------------------------------------------
# Titan Stock CSV processing
# ---------------------------------------------------------------------------

TITAN_CLEAN_FIELDS = [
    "Drug Name", "Stock Level", "Last Inputed", "Last Dispense Date",
    "Pack Size", "Number of doses", "Is Split Pack",
]
TITAN_SUMMARY_FIELDS = [
    "Drug Name", "Total Stock", "Total Doses", "Pack Sizes",
    "Last Inputed", "Last Dispense Date",
]


def process_titan_stock(filepath):
    """Process a Titan_Stock.CSV file. Returns (clean_rows, summary_rows, stats)."""
    raw = _read_comma_csv(filepath)

    # Step 1 – deduplicate by (Drug Name, Pack Size, Last Inputed)
    groups = {}
    for row in raw:
        key = (
            row.get("Drug Name", ""),
            row.get("Pack Size", ""),
            row.get("Last Inputed", ""),
        )
        stock = _parse_q(row.get("Stock Level", "0"))
        doses = _parse_q(row.get("Number of doses", "0"))
        if key not in groups:
            groups[key] = {
                "Stock Level": 0,
                "Number of doses": 0,
                "Last Dispense Date": row.get("Last Dispense Date", ""),
                "Is Split Pack": row.get("Is Split Pack", "FALSE"),
            }
        groups[key]["Stock Level"] += stock
        groups[key]["Number of doses"] += doses
        # Keep the latest dispense date
        existing_dt = _parse_date_multi(groups[key]["Last Dispense Date"])
        new_dt = _parse_date_multi(row.get("Last Dispense Date", ""))
        if new_dt and (existing_dt is None or new_dt > existing_dt):
            groups[key]["Last Dispense Date"] = row.get("Last Dispense Date", "")

    clean = []
    for (drug_name, pack_size, last_inputed), data in sorted(groups.items()):
        clean.append({
            "Drug Name": drug_name,
            "Stock Level": data["Stock Level"],
            "Last Inputed": last_inputed,
            "Last Dispense Date": data["Last Dispense Date"],
            "Pack Size": pack_size,
            "Number of doses": data["Number of doses"],
            "Is Split Pack": data["Is Split Pack"],
        })

    # Step 2 – summary by Drug Name
    name_groups = defaultdict(
        lambda: {
            "total_stock": 0, "total_doses": 0, "pack_sizes": set(),
            "latest_input": None, "latest_dispense": None,
        }
    )
    for row in clean:
        entry = name_groups[row["Drug Name"]]
        entry["total_stock"] += row["Stock Level"]
        entry["total_doses"] += row["Number of doses"]
        ps = row.get("Pack Size", "").strip()
        if ps:
            entry["pack_sizes"].add(ps)
        dt_in = _parse_date_multi(row.get("Last Inputed", ""))
        if dt_in and (entry["latest_input"] is None or dt_in > entry["latest_input"]):
            entry["latest_input"] = dt_in
        dt_disp = _parse_date_multi(row.get("Last Dispense Date", ""))
        if dt_disp and (entry["latest_dispense"] is None or dt_disp > entry["latest_dispense"]):
            entry["latest_dispense"] = dt_disp

    summary = []
    for name in sorted(name_groups):
        e = name_groups[name]
        summary.append({
            "Drug Name": name,
            "Total Stock": e["total_stock"],
            "Total Doses": e["total_doses"],
            "Pack Sizes": "; ".join(sorted(e["pack_sizes"])),
            "Last Inputed": _format_date(e["latest_input"]),
            "Last Dispense Date": _format_date(e["latest_dispense"]),
        })

    stats = {
        "raw_rows": len(raw),
        "clean_rows": len(clean),
        "summary_rows": len(summary),
        "duplicates_removed": len(raw) - len(clean),
    }
    return clean, summary, stats
