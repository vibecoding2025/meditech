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


# ---------------------------------------------------------------------------
# Drug Tariff pricing
# ---------------------------------------------------------------------------

_tariff_cache = None
_brand_map_cache = None
_partix_cache = None

def _load_tariff():
    """Load Drug Tariff Part VIIIA CSV. Returns dict of {lowercase name: price in pence}."""
    global _tariff_cache
    if _tariff_cache is not None:
        return _tariff_cache

    import os
    tariff_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "drug_tariff.csv")
    tariff = {}
    if not os.path.exists(tariff_path):
        _tariff_cache = tariff
        return tariff

    with open(tariff_path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i < 3:  # skip header rows
                continue
            if len(row) >= 7 and row[0].strip():
                name = row[0].strip().lower()
                try:
                    price_pence = int(row[6].strip())
                except (ValueError, IndexError):
                    continue
                tariff[name] = price_pence

    _tariff_cache = tariff
    return tariff


def _load_brand_map():
    """Load brand-to-generic mapping CSV. Returns dict of {branded name lower: generic name lower}."""
    global _brand_map_cache
    if _brand_map_cache is not None:
        return _brand_map_cache

    import os
    map_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "brand_to_generic.csv")
    mapping = {}
    if not os.path.exists(map_path):
        _brand_map_cache = mapping
        return mapping

    with open(map_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            brand = (row.get("brand_name") or "").strip().lower()
            generic = (row.get("generic_name") or "").strip().lower()
            if brand and generic:
                mapping[brand] = generic

    _brand_map_cache = mapping
    return mapping


def _load_partix():
    """Load Drug Tariff Part IX CSV. Returns dict with amp_prices, vmp_prices, gtin_prices."""
    global _partix_cache
    if _partix_cache is not None:
        return _partix_cache

    import os, glob
    base_dir = os.path.dirname(os.path.abspath(__file__))
    # Find Part IX CSV file
    files = glob.glob(os.path.join(base_dir, "Drug_Tariff_Part_IX*.csv"))
    if not files:
        _partix_cache = {"amp": {}, "vmp": {}, "gtin": {}}
        return _partix_cache

    amp_prices = {}   # amp name lower -> price pence
    vmp_prices = {}   # vmp name lower -> price pence
    gtin_prices = {}  # gtin 13-digit -> price pence

    with open(files[0], "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            amp = (row.get("AMP Name") or "").strip().lower()
            vmp = (row.get("VMP Name") or "").strip().lower()
            gtin = (row.get("GTIN") or "").strip()
            try:
                price = int(row.get("Price", "0").strip())
            except (ValueError, TypeError):
                continue
            if price <= 0:
                continue
            if amp:
                amp_prices[amp] = price
            if vmp:
                vmp_prices[vmp] = price
            if gtin:
                gtin_norm = gtin.lstrip("0")
                if len(gtin_norm) == 13 and gtin_norm.isdigit():
                    gtin_prices[gtin_norm] = price

    _partix_cache = {"amp": amp_prices, "vmp": vmp_prices, "gtin": gtin_prices}
    return _partix_cache


def _lookup_tariff_price(drug_name, ean_code=""):
    """Look up Drug Tariff price for a drug name. Returns price in pence or None."""
    tariff = _load_tariff()
    brand_map = _load_brand_map()
    partix = _load_partix()
    name = drug_name.strip().lower()

    # 1. Part VIIIA exact match
    if name in tariff:
        return tariff[name]

    # 2. Brand-to-generic mapping -> Part VIIIA
    if name in brand_map:
        generic = brand_map[name]
        if generic in tariff:
            return tariff[generic]

    # 3. Part IX - GTIN match
    if ean_code and ean_code in partix["gtin"]:
        return partix["gtin"][ean_code]

    # 4. Part IX - AMP name match
    if name in partix["amp"]:
        return partix["amp"][name]

    # 5. Part IX - VMP name match
    if name in partix["vmp"]:
        return partix["vmp"][name]

    return None


def _enrich_with_pricing(rows):
    """Add Drug Tariff Price and Total Value columns to rows. Returns grand total in pence."""
    grand_total = 0
    seen_names = set()
    for row in rows:
        price = _lookup_tariff_price(row["Name"], row.get("EAN_13 (PipCode)", ""))
        if price is not None:
            price_pounds = price / 100.0
            row["Drug Tariff Price"] = f"£{price_pounds:.2f}"
            # Only count total value once per drug name (avoid duplicates from multiple EANs)
            name = row["Name"].strip()
            if name not in seen_names:
                qty = int(row["Quantity"]) if row["Quantity"] else 0
                total = price_pounds * qty
                row["Total Value"] = f"£{total:.2f}"
                grand_total += total * 100
                seen_names.add(name)
            else:
                row["Total Value"] = ""
        else:
            row["Drug Tariff Price"] = ""
            row["Total Value"] = ""
    return int(grand_total)


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

OUTPUT_FIELDS = ["Name", "Quantity", "EAN_13 (PipCode)", "EAN_status", "Latest Delivery", "Drug Tariff Price", "Total Value"]


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
