import os
from datetime import datetime, timedelta
import pytz
from flask import Flask, request, jsonify, send_file, render_template, session, redirect, Response
from azure.data.tables import TableServiceClient
import pandas as pd
from io import BytesIO
from reportlab.platypus import SimpleDocTemplate, Table
from reportlab.lib.pagesizes import letter
import json

app = Flask(__name__)
app.secret_key = "super-secret-key"
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

# =========================
# Azure Table Config
# =========================
TABLES_CONN = os.environ.get("TABLES_CONNECTION_STRING")
TABLE_NAME = os.environ.get("TABLE_NAME", "DeviceData")
TABLE_NAME_2 = os.environ.get("TABLE_NAME_2", "ScadaData")

service = TableServiceClient.from_connection_string(TABLES_CONN)
table_client = service.get_table_client(TABLE_NAME)
table_client_2 = service.get_table_client(TABLE_NAME_2)

try:
    table_client.create_table()
    table_client_2.create_table()
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

@app.route("/load", methods=["POST"])
def load_data():

    if not request.is_json:
        return jsonify({"error": "JSON required"}), 400

    data = request.get_json()

    try:
        entity = build_entity(data)

        table_client_2.upsert_entity(entity=entity, mode="replace")

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
# Fuel Consumption API
# =========================
@app.route("/API")
def api_readings():

    from_time = request.args.get("fromTime")
    to_time = request.args.get("toTime")
    deviceid = "susanmpa"

    if not from_time or not to_time:
        return jsonify({"error": "fromTime and toTime required"}), 400

    try:
        start_dt = datetime.strptime(from_time, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(to_time, "%Y-%m-%d %H:%M:%S")
    except:
        return jsonify({"error": "Invalid datetime format"}), 400

    readings = []

    try:

        # Azure optimized query
        query = (
            f"PartitionKey eq '{deviceid}' "
            f"and TimestampIST ge '{from_time}' "
            f"and TimestampIST le '{to_time}'"
        )

        entities = list(table_client_2.query_entities(query))

        current_start = start_dt

        while current_start < end_dt:

            current_end = min(current_start + timedelta(hours=1), end_dt)

            # Initialize totals for all meters
            total_ft1 = 0  # PME INLET - Main Engine 1 Inlet
            total_ft2 = 0  # PME OUTLET - Main Engine 1 Outlet
            total_ft3 = 0  # SME INLET - Main Engine 2 Inlet
            total_ft4 = 0  # SME OUTLET - Main Engine 2 Outlet
            total_ft5 = 0  # PAE INLET - Generator 1 Inlet
            total_ft6 = 0  # PAE OUTLET - Generator 1 Outlet
            total_ft7 = 0  # SAE INLET - Generator 2 Inlet
            total_ft8 = 0  # SAE OUTLET - Generator 2 Outlet
            total_ft9 = 0  # BUNKER FLOW (if needed)

            for e in entities:

                ts = e.get("TimestampIST")
                if not ts:
                    continue

                try:
                    ts_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                except:
                    continue

                if current_start <= ts_dt < current_end:

                    # Accumulate values for each meter
                    total_ft1 += float(e.get("FT1MassFlow") or 0)
                    total_ft2 += float(e.get("FT2MassFlow") or 0)
                    total_ft3 += float(e.get("FT3MassFlow") or 0)
                    total_ft4 += float(e.get("FT4MassFlow") or 0)
                    total_ft5 += float(e.get("FT5MassFlow") or 0)
                    total_ft6 += float(e.get("FT6MassFlow") or 0)
                    total_ft7 += float(e.get("FT7MassFlow") or 0)
                    total_ft8 += float(e.get("FT8MassFlow") or 0)
                    total_ft9 += float(e.get("FT9MassFlow") or 0)

            # Calculate engine and generator totals
            # Main Engine 1: FT1 (Inlet) + FT2 (Outlet)
            main_engine_1_total = total_ft1 - total_ft2
            
            # Main Engine 2: FT3 (Inlet) + FT4 (Outlet)
            main_engine_2_total = total_ft3 - total_ft4
            
            # Generator 1: FT5 (Inlet) + FT6 (Outlet)
            generator_1_total = total_ft5 - total_ft6
            
            # Generator 2: FT7 (Inlet) + FT8 (Outlet)
            generator_2_total = total_ft7 - total_ft8
            
            # Total all engines and generators
            total_main_engines = main_engine_1_total + main_engine_2_total
            total_generators = generator_1_total + generator_2_total

            readings.append({
                "measurementStartTime": current_start.strftime("%Y-%m-%d %H:%M:%S"),
                "measurementEndTime": (current_end - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S"),
                "kind": "VESSEL",
                "mmsi": "419001409",
                "imo": "933632",
                "consumption": {
                    "mainEnginesTotal": round(total_main_engines, 8),
                    "generatorsTotal": round(total_generators, 8),
                    "mainEngines": [
                        {
                            "name": "Main Engine 1 ",
                            "value": round(main_engine_1_total, 8)
                        },
                        {
                            "name": "Main Engine 2 ",
                            "value": round(main_engine_2_total, 8)
                        }
                    ],
                    "generators": [
                        {
                            "name": "Generator 1 ",
                            "value": round(generator_1_total, 8)
                        },
                        {
                            "name": "Generator 2 ",
                            "value": round(generator_2_total, 8)
                        }
                    ]
                }
            })

            current_start = current_end

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return Response(json.dumps({"readings": readings}, indent=4), mimetype="application/json")


# =========================
# Fetch Records For Reports
# =========================
# def parse_dt(value):
#     try:
#         return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
#     except ValueError:
#         return datetime.strptime(value, "%Y-%m-%d %H:%M")


# def fetch_records(prefix, start, end):

#     deviceid = "susanmpa"

#     start_dt = parse_dt(start)
#     end_dt = parse_dt(end)

#     records = []

#     query = f"PartitionKey eq '{deviceid}'"
#     entities = table_client.query_entities(query)

#     for e in entities:

#         ts = e.get("TimestampIST")
#         if not ts:
#             continue

#         ts_dt = parse_dt(ts)

#         if start_dt <= ts_dt <= end_dt:
#             records.append({
#                 "Timestamp": ts,
#                 "MassFlow": e.get(prefix + "MassFlow"),
#                 "Masstotal": e.get(prefix + "Masstotal"),
#                 "VolumeFlow": e.get(prefix + "VolumeFlow"),
#                 "Volumetotal": e.get(prefix + "Volumetotal"),
#                 "Density": e.get(prefix + "Density"),
#                 "Temp": e.get(prefix + "Temp")
#             })

#     return records


# =========================
# # Fetch Records API for Preview
# # =========================
# @app.route("/api/fetch_records")
# @login_required
# def api_fetch_records():
#     try:
#         prefix = request.args.get("type")
#         start = request.args.get("start")
#         end = request.args.get("end")
        
#         if not prefix or not start or not end:
#             return jsonify({"error": "Missing parameters"}), 400
        
#         # Use your existing fetch_records function
#         data = fetch_records(prefix, start, end)
        
#         # Round values to 5 decimal places
#         for record in data:
#             for key in ['MassFlow', 'Masstotal', 'VolumeFlow', 'Volumetotal', 'Density', 'Temp']:
#                 if key in record and record[key] is not None:
#                     try:
#                         record[key] = round(float(record[key]), 5)
#                     except (ValueError, TypeError):
#                         pass
        
#         return jsonify(data)
        
#     except Exception as e:
#         print(f"API fetch records error: {e}")
#         return jsonify({"error": str(e)}), 500



def parse_dt(value):
    try:
        # Try parsing with seconds
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            # Try parsing without seconds
            return datetime.strptime(value, "%Y-%m-%d %H:%M")
        except ValueError:
            # Try parsing with T
            return datetime.strptime(value.replace('T', ' '), "%Y-%m-%d %H:%M")

def fetch_records(prefix, start, end):
    deviceid = "susanad"
    
    print(f"Fetching records for {prefix} from {start} to {end}")  # Debug print
    
    try:
        start_dt = parse_dt(start)
        end_dt = parse_dt(end)
        
        print(f"Parsed dates: start={start_dt}, end={end_dt}")  # Debug print
    except Exception as e:
        print(f"Date parsing error: {e}")
        return []

    records = []
    
    # Optimize query with time filter
    query = f"PartitionKey eq '{deviceid}'"
    
    try:
        entities = list(table_client.query_entities(query))
        print(f"Total entities found: {len(entities)}")  # Debug print
        
        for e in entities:
            ts = e.get("TimestampIST")
            if not ts:
                continue
            
            try:
                ts_dt = parse_dt(ts)
                
                if start_dt <= ts_dt <= end_dt:
                    record = {
                        "Timestamp": ts,
                        "MassFlow": e.get(prefix + "MassFlow", 0),
                        "Masstotal": e.get(prefix + "Masstotal", 0),
                        "VolumeFlow": e.get(prefix + "VolumeFlow", 0),
                        "Volumetotal": e.get(prefix + "Volumetotal", 0),
                        "Density": e.get(prefix + "Density", 0),
                        "Temp": e.get(prefix + "Temp", 0)
                    }
                    records.append(record)
            except Exception as e:
                print(f"Error processing record: {e}")
                continue
        
        print(f"Records found for {prefix}: {len(records)}")  # Debug print
        
    except Exception as e:
        print(f"Query error: {e}")
        return []
    
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
# @app.route("/download_pdf")
# @login_required
# def download_pdf():

#     prefix = request.args.get("type")
#     start = request.args.get("start").replace("T", " ")
#     end = request.args.get("end").replace("T", " ")

#     data = fetch_records(prefix, start, end)

#     buffer = BytesIO()
#     pdf = SimpleDocTemplate(buffer, pagesize=letter)

#     if not data:
#         table_data = [["No Data Found"]]
#     else:
#         table_data = [list(data[0].keys())]
#         for row in data:
#             table_data.append(list(row.values()))

#     table = Table(table_data)
#     pdf.build([table])

#     buffer.seek(0)

#     return send_file(
#         buffer,
#         mimetype="application/pdf",
#         download_name="report.pdf",
#         as_attachment=True
#     )

@app.route("/download_pdf")
@login_required
def download_pdf():
    try:
        prefix = request.args.get("type")
        start = request.args.get("start").replace("T", " ")
        end = request.args.get("end").replace("T", " ")
        
        print(f"PDF Request - Prefix: {prefix}, Start: {start}, End: {end}")  # Debug
        
        # Fetch data
        data = fetch_records(prefix, start, end)
        
        print(f"Data retrieved: {len(data)} records")  # Debug
        
        buffer = BytesIO()
        
        if not data or len(data) == 0:
            # Create PDF with no data message
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import letter
            
            c = canvas.Canvas(buffer, pagesize=letter)
            c.setFont("Helvetica-Bold", 16)
            c.drawString(100, 750, f"No Data Found for {prefix}")
            
            c.setFont("Helvetica", 12)
            c.drawString(100, 700, f"From: {start}")
            c.drawString(100, 680, f"To: {end}")
            c.drawString(100, 660, f"Device: susanad")
            c.drawString(100, 640, f"Time Range: {(parse_dt(end) - parse_dt(start)).total_seconds()/3600:.1f} hours")
            
            c.setFont("Helvetica", 10)
            c.drawString(100, 600, "Possible issues:")
            c.drawString(120, 580, "1. No data exists for this time range")
            c.drawString(120, 560, "2. The meter prefix might be incorrect")
            c.drawString(120, 540, "3. Check if the device ID 'susanad' has data")
            
            c.save()
        else:
            # Create PDF with data
            from reportlab.lib.pagesizes import landscape, letter
            from reportlab.pdfgen import canvas
            
            c = canvas.Canvas(buffer, pagesize=landscape(letter))
            
            # Header
            c.setFont("Helvetica-Bold", 14)
            c.drawString(50, 550, f"{prefix} Flow Meter Report")
            
            c.setFont("Helvetica", 10)
            c.drawString(50, 530, f"From: {start}")
            c.drawString(350, 530, f"To: {end}")
            c.drawString(50, 515, f"Total Records: {len(data)}")
            
            # Table headers
            y = 480
            c.setFont("Helvetica-Bold", 8)
            c.drawString(50, y, "Timestamp")
            c.drawString(200, y, "Mass Flow")
            c.drawString(270, y, "Mass Total")
            c.drawString(340, y, "Volume Flow")
            c.drawString(410, y, "Volume Total")
            c.drawString(480, y, "Density")
            c.drawString(550, y, "Temp")
            
            y -= 15
            c.setFont("Helvetica", 7)
            
            # Show first 30 records
            for i, row in enumerate(data[:30]):
                if y < 50:  # New page
                    c.showPage()
                    y = 550
                    c.setFont("Helvetica-Bold", 8)
                    c.drawString(50, y, "Timestamp (continued)")
                    y -= 15
                    c.setFont("Helvetica", 7)
                
                try:
                    c.drawString(50, y, str(row.get("Timestamp", ""))[:16])
                    c.drawString(200, y, f"{float(row.get('MassFlow', 0) or 0):.5f}")
                    c.drawString(270, y, f"{float(row.get('Masstotal', 0) or 0):.5f}")
                    c.drawString(340, y, f"{float(row.get('VolumeFlow', 0) or 0):.5f}")
                    c.drawString(410, y, f"{float(row.get('Volumetotal', 0) or 0):.5f}")
                    c.drawString(480, y, f"{float(row.get('Density', 0) or 0):.5f}")
                    c.drawString(550, y, f"{float(row.get('Temp', 0) or 0):.5f}")
                except Exception as e:
                    print(f"Error writing row {i}: {e}")
                
                y -= 12
            
            # Add summary
            if len(data) > 30:
                c.setFont("Helvetica-Oblique", 8)
                c.drawString(50, y-10, f"... and {len(data) - 30} more records")
            
            c.save()
        
        buffer.seek(0)
        
        filename = f"{prefix}_report_{start.replace(' ', '_')}_to_{end.replace(' ', '_')}.pdf"
        
        return send_file(
            buffer,
            mimetype="application/pdf",
            download_name=filename,
            as_attachment=True
        )
        
    except Exception as e:
        print(f"PDF download error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# =========================
# RUN APP
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)













