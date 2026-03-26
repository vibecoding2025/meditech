"""Meditech CSV Processor — Flask web app."""

import csv
import io
import os
import uuid
import webbrowser
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


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
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
        priced_names = set(r["Name"] for r in rows if r.get("Drug Tariff Price"))
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
    )


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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def open_browser():
    webbrowser.open("http://127.0.0.1:5000")


if __name__ == "__main__":
    Timer(1.5, open_browser).start()
    print("\n  Meditech CSV Processor running at http://127.0.0.1:5000\n")
    app.run(debug=False, port=5000)
