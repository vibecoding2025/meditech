"""Meditech CSV Processor — Flask web app."""

import csv
import io
import json
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

# In-memory store: each user session gets their own results, auto-cleared
# Key = session token, Value = {stats, clean_csv, summary_csv, file_type, filename, clean_fields, summary_fields}
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
            clean, summary, stats = proc.process_export(filepath)
            clean_csv = proc.generate_csv_string(clean, proc.EXPORT_CLEAN_FIELDS)
            summary_csv = proc.generate_csv_string(summary, proc.EXPORT_SUMMARY_FIELDS)
        elif file_type == "unsold":
            clean, summary, stats = proc.process_unsold(filepath)
            clean_csv = proc.generate_csv_string(clean, proc.UNSOLD_CLEAN_FIELDS)
            summary_csv = proc.generate_csv_string(summary, proc.UNSOLD_SUMMARY_FIELDS)
        else:  # titan_stock
            clean, summary, stats = proc.process_titan_stock(filepath)
            clean_csv = proc.generate_csv_string(clean, proc.TITAN_CLEAN_FIELDS)
            summary_csv = proc.generate_csv_string(summary, proc.TITAN_SUMMARY_FIELDS)

        # Store results in memory with a unique token
        token = str(uuid.uuid4())
        _results_store[token] = {
            "stats": stats,
            "clean_csv": clean_csv,
            "summary_csv": summary_csv,
            "file_type": file_type,
            "filename": filename,
        }
        session["result_token"] = token

        flash(f"Processed {filename}: {stats['raw_rows']} rows read, "
              f"{stats['duplicates_removed']} duplicates removed, "
              f"{stats['summary_rows']} unique items.", "success")
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

    clean_rows = list(csv.DictReader(io.StringIO(data["clean_csv"])))
    summary_rows = list(csv.DictReader(io.StringIO(data["summary_csv"])))

    clean_headers = list(clean_rows[0].keys()) if clean_rows else []
    summary_headers = list(summary_rows[0].keys()) if summary_rows else []

    return render_template(
        "results.html",
        file_type=data["file_type"],
        filename=data["filename"],
        stats=stats,
        clean_headers=clean_headers,
        clean_rows=clean_rows,
        summary_headers=summary_headers,
        summary_rows=summary_rows,
    )


@app.route("/download/<data_type>")
def download(data_type):
    if data_type not in ("clean", "summary"):
        flash("Invalid download type.", "error")
        return redirect(url_for("index"))

    token = session.get("result_token")
    if not token or token not in _results_store:
        flash("No results found. Please upload a file first.", "error")
        return redirect(url_for("index"))

    data = _results_store[token]
    content = data["clean_csv"] if data_type == "clean" else data["summary_csv"]

    filename = f"{data['file_type']}_{data_type}.csv"
    return Response(
        content,
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
