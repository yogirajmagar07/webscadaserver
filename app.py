import os
from datetime import datetime
import pytz
from flask import Flask, request, jsonify, send_file, render_template, session, redirect
from azure.data.tables import TableServiceClient
import pandas as pd
from io import BytesIO
from reportlab.platypus import SimpleDocTemplate, Table
from reportlab.lib.pagesizes import letter

app = Flask(__name__)
app.secret_key = "super-secret-key"

# =========================
# Azure Table Config
# =========================
TABLES_CONN = os.environ.get("TABLES_CONNECTION_STRING")
TABLE_NAME = os.environ.get("TABLE_NAME", "DeviceData")

service = TableServiceClient.from_connection_string(TABLES_CONN)
table_client = service.get_table_client(TABLE_NAME)

try:
    table_client.create_table()
except:
    pass

# =========================
# In-Memory Cache (Device Wise)
# =========================
latest_cache = {}

# =========================
# LOGIN CHECK DECORATOR
# =========================
def login_required(func):
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect("/login")
        return func(*args, **kwargs)
    wrapper.__name__ = func.__name__
    return wrapper


# =========================
# LOGIN PAGE (Dummy)
# =========================
@app.route("/login")
def login():
    session["logged_in"] = True
    return redirect("/")


# =========================
# LOGOUT
# =========================
@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


# =========================
# DASHBOARD PAGE
# =========================
@app.route("/")
@login_required
def dashboard():
    return render_template("index.html")


# =========================
# REPORT PAGE
# =========================
@app.route("/reports")
@login_required
def reports():
    return render_template("reports.html")


# =========================
# BUILD ENTITY
# =========================
def build_entity(data):

    deviceid = str(data.get("deviceid", "susanad"))

    ist = pytz.timezone("Asia/Kolkata")
    ist_time = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(ist)

    # ✅ FIX 1 — Unique RowKey
    rowkey = ist_time.strftime("%Y%m%d%H%M%S%f")

    ts = ist_time.strftime("%Y-%m-%d %H:%M:%S")

    entity = {
        "PartitionKey": deviceid,
        "RowKey": rowkey,
        "TimestampIST": ts
    }

    for k, v in data.items():
        if k != "deviceid":
            entity[k] = str(v)

    return entity


# =========================
# INGEST API (SCADA)
# =========================
@app.route("/ingest", methods=["POST"])
def ingest():

    if not request.is_json:
        return jsonify({"error": "JSON required"}), 400

    data = request.get_json()

    try:
        entity = build_entity(data)

        table_client.upsert_entity(entity=entity, mode="replace")

        # ✅ FIX 2 — Multi Device Cache
        latest_cache[entity["PartitionKey"]] = entity

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"status": "ok"})


# =========================
# Latest Dashboard API
# =========================
@app.route("/api/latest")
@login_required
def latest():
    return jsonify(list(latest_cache.values()))


# =========================
# Fetch Records For Reports
# =========================
def fetch_records(prefix, start, end):

    partition = "susanad"
    query = f"PartitionKey eq '{partition}'"

    entities = table_client.query_entities(query)

    records = []

    fmt = "%Y-%m-%d %H:%M:%S"

    start_dt = datetime.strptime(start, fmt)
    end_dt = datetime.strptime(end, fmt)

    for e in entities:

        ts = e.get("TimestampIST")
        if not ts:
            continue

        # ✅ FIX 3 — Proper Datetime Comparison
        ts_dt = datetime.strptime(ts, fmt)

        if start_dt <= ts_dt <= end_dt:

            records.append({
                "Timestamp": ts,
                "MassFlow": e.get(prefix + "MassFlow"),
                "Masstotal": e.get(prefix + "Masstotal"),
                "VolumeFlow": e.get(prefix + "VolumeFlow"),
                "Volumetotal": e.get(prefix + "Volumetotal"),
                "Density": e.get(prefix + "Density"),
                "Temp": e.get(prefix + "Temp")
            })

    return records


# =========================
# CSV DOWNLOAD
# =========================
@app.route("/download_csv")
@login_required
def download_csv():

    prefix = request.args.get("type")
    start = request.args.get("start").replace("T", " ")
    end = request.args.get("end").replace("T", " ")

    data = fetch_records(prefix, start, end)

    df = pd.DataFrame(data)

    output = BytesIO()
    df.to_csv(output, index=False)
    output.seek(0)

    return send_file(
        output,
        mimetype="text/csv",
        download_name="report.csv",
        as_attachment=True
    )


# =========================
# PDF DOWNLOAD
# =========================
@app.route("/download_pdf")
@login_required
def download_pdf():

    prefix = request.args.get("type")
    start = request.args.get("start").replace("T", " ")
    end = request.args.get("end").replace("T", " ")

    data = fetch_records(prefix, start, end)

    buffer = BytesIO()
    pdf = SimpleDocTemplate(buffer, pagesize=letter)

    if not data:
        table_data = [["No Data Found"]]
    else:
        table_data = [list(data[0].keys())]
        for row in data:
            table_data.append(list(row.values()))

    table = Table(table_data)
    pdf.build([table])

    buffer.seek(0)

    return send_file(
        buffer,
        mimetype="application/pdf",
        download_name="report.pdf",
        as_attachment=True
    )


# =========================
# RUN APP
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
