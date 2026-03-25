"""
Reusable processing functions for Export and Unsold CSVs.
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
