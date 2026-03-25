"""Meditech CSV Processor — Flask web app."""

import csv
import io
import json
import os
import webbrowser
from threading import Timer

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, send_file, Response,
)

import database as db
import processing as proc

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "meditech-local-dev-key")

UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    recent = db.get_runs()[:5]
    return render_template("index.html", recent=recent)


@app.route("/upload", methods=["POST"])
def upload():
    file = request.files.get("csvfile")
    file_type = request.form.get("file_type", "")

    if not file or not file.filename:
        flash("Please select a file to upload.", "error")
        return redirect(url_for("index"))

    if file_type not in ("export", "unsold"):
        flash("Please select a file type.", "error")
        return redirect(url_for("index"))

    # Save uploaded file
    filename = file.filename
    filepath = os.path.join(UPLOAD_DIR, filename)
    file.save(filepath)

    try:
        if file_type == "export":
            clean, summary, stats = proc.process_export(filepath)
            clean_csv = proc.generate_csv_string(clean, proc.EXPORT_CLEAN_FIELDS)
            summary_csv = proc.generate_csv_string(summary, proc.EXPORT_SUMMARY_FIELDS)
        else:
            clean, summary, stats = proc.process_unsold(filepath)
            clean_csv = proc.generate_csv_string(clean, proc.UNSOLD_CLEAN_FIELDS)
            summary_csv = proc.generate_csv_string(summary, proc.UNSOLD_SUMMARY_FIELDS)

        run_id = db.save_run(file_type, filename, stats, clean_csv, summary_csv)
        flash(f"Processed {filename}: {stats['raw_rows']} rows read, "
              f"{stats['duplicates_removed']} duplicates removed, "
              f"{stats['summary_rows']} unique items.", "success")
        return redirect(url_for("results", run_id=run_id))

    except Exception as e:
        flash(f"Error processing file: {e}", "error")
        return redirect(url_for("index"))
    finally:
        # Clean up uploaded file
        if os.path.exists(filepath):
            os.remove(filepath)


@app.route("/results/<int:run_id>")
def results(run_id):
    run = db.get_run(run_id)
    if not run:
        flash("Run not found.", "error")
        return redirect(url_for("index"))

    stats = json.loads(run["stats_json"]) if run["stats_json"] else {}

    # Parse CSVs back into lists for the template
    clean_csv = db.get_run_data(run_id, "clean") or ""
    summary_csv = db.get_run_data(run_id, "summary") or ""

    clean_rows = list(csv.DictReader(io.StringIO(clean_csv)))
    summary_rows = list(csv.DictReader(io.StringIO(summary_csv)))

    clean_headers = list(clean_rows[0].keys()) if clean_rows else []
    summary_headers = list(summary_rows[0].keys()) if summary_rows else []

    return render_template(
        "results.html",
        run=run,
        stats=stats,
        clean_headers=clean_headers,
        clean_rows=clean_rows,
        summary_headers=summary_headers,
        summary_rows=summary_rows,
    )


@app.route("/download/<int:run_id>/<data_type>")
def download(run_id, data_type):
    if data_type not in ("clean", "summary"):
        flash("Invalid download type.", "error")
        return redirect(url_for("index"))

    run = db.get_run(run_id)
    if not run:
        flash("Run not found.", "error")
        return redirect(url_for("index"))

    content = db.get_run_data(run_id, data_type)
    if not content:
        flash("Data not found.", "error")
        return redirect(url_for("results", run_id=run_id))

    filename = f"{run['file_type']}_{data_type}_{run['run_date'][:10]}.csv"
    return Response(
        content,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/history")
def history():
    runs = db.get_runs()
    return render_template("history.html", runs=runs)


@app.route("/delete/<int:run_id>", methods=["POST"])
def delete(run_id):
    db.delete_run(run_id)
    flash("Run deleted.", "success")
    return redirect(url_for("history"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def open_browser():
    webbrowser.open("http://127.0.0.1:5000")


if __name__ == "__main__":
    db.init_db()
    Timer(1.5, open_browser).start()
    print("\n  Meditech CSV Processor running at http://127.0.0.1:5000\n")
    app.run(debug=False, port=5000)
