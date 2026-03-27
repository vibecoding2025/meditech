"""Meditech CSV Processor — Flask web app."""

import csv
import io
import json
import os
import uuid
import webbrowser
from datetime import datetime
from threading import Timer

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, Response, session,
)

import processing as proc

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "meditech-local-dev-key")

UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# In-memory store: each user session gets their own results
_results_store = {}

USAGE_PATH = os.path.join(BASE_DIR, "usage.json")


def _load_usage():
    if os.path.exists(USAGE_PATH):
        with open(USAGE_PATH, "r") as f:
            return json.load(f)
    return {"total_visits": 0, "total_uploads": 0, "by_type": {"export": 0, "unsold": 0, "titan_stock": 0}, "last_upload": ""}


def _save_usage(data):
    with open(USAGE_PATH, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    usage = _load_usage()
    usage["total_visits"] += 1
    _save_usage(usage)
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    file = request.files.get("csvfile")
    file_type = request.form.get("file_type", "")

    if not file or not file.filename:
        flash("Please select a file to upload.", "error")
        return redirect(url_for("index"))

    if file_type not in ("export", "unsold", "titan_stock"):
        flash("Please select a file type.", "error")
        return redirect(url_for("index"))

    # Clear ODS code so pricing is locked on each new upload
    session.pop("ods_code", None)

    # Save uploaded file temporarily
    filename = file.filename
    filepath = os.path.join(UPLOAD_DIR, f"{uuid.uuid4()}_{filename}")
    file.save(filepath)

    try:
        if file_type == "export":
            rows, stats = proc.process_export(filepath)
        elif file_type == "unsold":
            rows, stats = proc.process_unsold(filepath)
        else:  # titan_stock
            rows, stats = proc.process_titan_stock(filepath)

        grand_total_pence = proc._enrich_with_pricing(rows)
        stats["grand_total"] = f"£{grand_total_pence / 100:.2f}"
        priced_names = set(r["Name"] for r in rows if r.get("Drug Tariff Price") and r["Drug Tariff Price"] != "NOT FOUND")
        all_names = set(r["Name"] for r in rows)
        stats["priced_items"] = len(priced_names)
        stats["missing_price"] = len(all_names) - len(priced_names)
        missing_ean_names = set(r["Name"] for r in rows if r.get("EAN_status") == "MISSING")
        stats["missing_ean"] = len(missing_ean_names)

        # Count expensive items (>£50 Drug Tariff Price)
        expensive_names = set()
        for r in rows:
            price_str = r.get("Drug Tariff Price", "").replace("£", "")
            if price_str:
                try:
                    if float(price_str) > 50:
                        expensive_names.add(r["Name"])
                except ValueError:
                    pass
        stats["expensive_items"] = len(expensive_names)

        output_csv = proc.generate_csv_string(rows, proc.OUTPUT_FIELDS)

        # Build missing EAN CSV
        missing_ean_rows = []
        seen = set()
        for r in rows:
            if r.get("EAN_status") == "MISSING" and r["Name"] not in seen:
                missing_ean_rows.append(r)
                seen.add(r["Name"])
        missing_ean_csv = proc.generate_csv_string(missing_ean_rows, proc.OUTPUT_FIELDS)

        # Build expensive items CSV
        expensive_rows = []
        seen_exp = set()
        for r in rows:
            price_str = r.get("Drug Tariff Price", "").replace("£", "")
            if price_str:
                try:
                    if float(price_str) > 50 and r["Name"] not in seen_exp:
                        expensive_rows.append(r)
                        seen_exp.add(r["Name"])
                except ValueError:
                    pass
        expensive_csv = proc.generate_csv_string(expensive_rows, proc.OUTPUT_FIELDS)

        # Track usage
        usage = _load_usage()
        usage["total_uploads"] += 1
        usage["by_type"][file_type] = usage["by_type"].get(file_type, 0) + 1
        usage["last_upload"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _save_usage(usage)

        # Store results in memory with a unique token
        token = str(uuid.uuid4())
        _results_store[token] = {
            "stats": stats,
            "output_csv": output_csv,
            "missing_ean_csv": missing_ean_csv,
            "expensive_csv": expensive_csv,
            "file_type": file_type,
            "filename": filename,
        }
        session["result_token"] = token

        flash(f"Processed {filename}: {stats['raw_rows']} rows read, "
              f"{stats['duplicates_removed']} duplicates removed, "
              f"{stats['unique_items']} unique items.", "success")
        return redirect(url_for("results"))

    except Exception as e:
        flash(f"Error processing file: {e}", "error")
        return redirect(url_for("index"))
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)


@app.route("/results")
def results():
    token = session.get("result_token")
    if not token or token not in _results_store:
        flash("No results found. Please upload a file first.", "error")
        return redirect(url_for("index"))

    data = _results_store[token]
    stats = data["stats"]

    rows = list(csv.DictReader(io.StringIO(data["output_csv"])))
    headers = proc.OUTPUT_FIELDS

    return render_template(
        "results.html",
        file_type=data["file_type"],
        filename=data["filename"],
        stats=stats,
        headers=headers,
        rows=rows,
        ods_unlocked=bool(session.get("ods_code")),
        ods_code=session.get("ods_code", ""),
    )


@app.route("/unlock-pricing", methods=["POST"])
def unlock_pricing():
    ods_code = request.form.get("ods_code", "").strip().upper()
    if not ods_code:
        return "ODS code required", 400
    session["ods_code"] = ods_code

    # Get details from current results
    grand_total = ""
    file_type = ""
    filename = ""
    token = session.get("result_token")
    if token and token in _results_store:
        data = _results_store[token]
        grand_total = data["stats"].get("grand_total", "")
        file_type = data.get("file_type", "")
        filename = data.get("filename", "")

    # Track ODS code - each unlock is a separate log entry
    usage = _load_usage()
    if "ods_codes" not in usage:
        usage["ods_codes"] = {}
    usage["ods_codes"][ods_code] = usage["ods_codes"].get(ods_code, 0) + 1
    if "upload_log" not in usage:
        usage["upload_log"] = []
    usage["upload_log"].append({
        "ods_code": ods_code,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "file_type": file_type,
        "filename": filename,
        "drug_tariff_total": grand_total,
    })
    _save_usage(usage)
    return "OK", 200


@app.route("/download")
def download():
    token = session.get("result_token")
    if not token or token not in _results_store:
        flash("No results found. Please upload a file first.", "error")
        return redirect(url_for("index"))

    data = _results_store[token]
    filename = f"{data['file_type']}_processed.csv"
    return Response(
        data["output_csv"],
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/download/expensive")
def download_expensive():
    token = session.get("result_token")
    if not token or token not in _results_store:
        flash("No results found. Please upload a file first.", "error")
        return redirect(url_for("index"))

    data = _results_store[token]
    filename = f"{data['file_type']}_expensive_items.csv"
    return Response(
        data["expensive_csv"],
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/download/missing-ean")
def download_missing_ean():
    token = session.get("result_token")
    if not token or token not in _results_store:
        flash("No results found. Please upload a file first.", "error")
        return redirect(url_for("index"))

    data = _results_store[token]
    filename = f"{data['file_type']}_missing_ean.csv"
    return Response(
        data["missing_ean_csv"],
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/admin/stats")
def admin_stats():
    usage = _load_usage()
    ods_codes = usage.get("ods_codes", {})
    upload_log = usage.get("upload_log", [])
    by_type = usage.get("by_type", {})
    export_count = by_type.get("export", 0)
    unsold_count = by_type.get("unsold", 0)
    titan_count = by_type.get("titan_stock", 0)

    # ODS summary rows
    ods_summary_rows = "".join(
        f'<tr><td style="padding:0.4rem 0.8rem;border-bottom:1px solid #e2e8f0"><strong>{code}</strong></td>'
        f'<td style="padding:0.4rem 0.8rem;border-bottom:1px solid #e2e8f0;text-align:right">{count}</td></tr>'
        for code, count in sorted(ods_codes.items(), key=lambda x: -x[1])
    )

    # File type labels
    type_labels = {"export": "📦 Export", "unsold": "📋 Unsold", "titan_stock": "💊 Titan Stock"}

    # Upload log rows (newest first)
    log_rows = "".join(
        f'<tr><td style="padding:0.4rem 0.8rem;border-bottom:1px solid #e2e8f0"><strong>{entry.get("ods_code", "—")}</strong></td>'
        f'<td style="padding:0.4rem 0.8rem;border-bottom:1px solid #e2e8f0">{entry.get("date", "—")}</td>'
        f'<td style="padding:0.4rem 0.8rem;border-bottom:1px solid #e2e8f0">{type_labels.get(entry.get("file_type", ""), entry.get("file_type", "—"))}</td>'
        f'<td style="padding:0.4rem 0.8rem;border-bottom:1px solid #e2e8f0;font-size:0.8rem">{entry.get("filename", "—")}</td>'
        f'<td style="padding:0.4rem 0.8rem;border-bottom:1px solid #e2e8f0;text-align:right;color:#16a34a;font-weight:600">{entry.get("drug_tariff_total", "—")}</td></tr>'
        for entry in reversed(upload_log)
    )

    return f"""<!DOCTYPE html>
<html><head><title>Usage Stats</title>
<style>
body {{ font-family: -apple-system, sans-serif; max-width: 900px; margin: 4rem auto; padding: 1rem; }}
h1 {{ color: #2563eb; }} h2 {{ color: #1e293b; margin-top: 2rem; }}
.stat-grid {{ display: flex; flex-wrap: wrap; gap: 0.5rem; }}
.stat {{ padding: 1rem; background: #f8fafc; border-radius: 8px; border: 1px solid #e2e8f0; flex: 1; min-width: 120px; }}
.stat strong {{ font-size: 1.5rem; color: #1e293b; display: block; }} .label {{ color: #64748b; font-size: 0.8rem; }}
table {{ width: 100%; border-collapse: collapse; margin-top: 0.5rem; }}
th {{ background: #eef2ff; color: #1e40af; padding: 0.5rem 0.8rem; text-align: left; border-bottom: 2px solid #818cf8; font-size: 0.85rem; }}
</style></head><body>
<h1>📊 Meditech Stock Bot — Usage Stats</h1>

<div class="stat-grid">
<div class="stat"><strong>{usage.get('total_visits', 0)}</strong><span class="label">👁️ Page Visits</span></div>
<div class="stat"><strong>{usage.get('total_uploads', 0)}</strong><span class="label">📤 Files Uploaded</span></div>
<div class="stat"><strong>{export_count}</strong><span class="label">📦 Export</span></div>
<div class="stat"><strong>{unsold_count}</strong><span class="label">📋 Unsold</span></div>
<div class="stat"><strong>{titan_count}</strong><span class="label">💊 Titan Stock</span></div>
<div class="stat"><strong>{len(ods_codes)}</strong><span class="label">🏥 Pharmacies</span></div>
</div>

<h2>🏥 ODS Code Summary</h2>
<table>
<thead><tr><th>ODS Code</th><th style="text-align:right">Total Uploads</th></tr></thead>
<tbody>{ods_summary_rows if ods_summary_rows else '<tr><td colspan="2" style="padding:0.8rem;color:#64748b">No uploads yet</td></tr>'}</tbody>
</table>

<h2>📋 Upload Log (newest first)</h2>
<table>
<thead><tr><th>ODS Code</th><th>Date</th><th>File Type</th><th>Filename</th><th style="text-align:right">Drug Tariff Total</th></tr></thead>
<tbody>{log_rows if log_rows else '<tr><td colspan="5" style="padding:0.8rem;color:#64748b">No uploads yet</td></tr>'}</tbody>
</table>
</body></html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def open_browser():
    webbrowser.open("http://127.0.0.1:5000")


if __name__ == "__main__":
    Timer(1.5, open_browser).start()
    print("\n  Meditech CSV Processor running at http://127.0.0.1:5000\n")
    app.run(debug=False, port=5000)
