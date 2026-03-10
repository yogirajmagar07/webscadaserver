import os
from datetime import datetime, timedelta
import pytz
from flask import Flask, request, jsonify, send_file, render_template, session, redirect, Response ,url_for
from azure.data.tables import TableServiceClient
import pandas as pd
from io import BytesIO
from reportlab.platypus import SimpleDocTemplate, Table
from reportlab.lib.pagesizes import letter
import json
from functools import wraps

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

# Login decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# Login route
@app.route('/')
def login():
    return render_template('login.html')

# Login API endpoint (for AJAX login)
@app.route('/api/login', methods=['POST'])
def api_login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    
    # Simple authentication (use proper auth in production)
    if username == 'admin' and password == 'admin123':
        session['user_id'] = username
        session['username'] = username
        return jsonify({'success': True})
    else:
        return jsonify({'success': False, 'error': 'Invalid credentials'}), 401

# =========================
# LOGOUT
# =========================
@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")


# =========================
# DASHBOARD PAGE
# =========================
# Dashboard route (protected)
@app.route('/dashboard')
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

#  Fetch and parse Records

# def parse_dt(value):
#     try:
#         # Try parsing with seconds
#         return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
#     except ValueError:
#         try:
#             # Try parsing without seconds
#             return datetime.strptime(value, "%Y-%m-%d %H:%M")
#         except ValueError:
#             # Try parsing with T
#             return datetime.strptime(value.replace('T', ' '), "%Y-%m-%d %H:%M")

# def fetch_records(prefix, start, end):
#     deviceid = "susanmpa"
    
#     print(f"Fetching records for {prefix} from {start} to {end}")  # Debug print
    
#     try:
#         start_dt = parse_dt(start)
#         end_dt = parse_dt(end)
        
#         print(f"Parsed dates: start={start_dt}, end={end_dt}")  # Debug print
#     except Exception as e:
#         print(f"Date parsing error: {e}")
#         return []

#     records = []
    
#     # Optimize query with time filter
#     query = f"PartitionKey eq '{deviceid}'"
    
#     try:
#         entities = list(table_client_2.query_entities(query))
#         print(f"Total entities found: {len(entities)}")  # Debug print
        
#         for e in entities:
#             ts = e.get("TimestampIST")
#             if not ts:
#                 continue
            
#             try:
#                 ts_dt = parse_dt(ts)
                
#                 if start_dt <= ts_dt <= end_dt:
#                     record = {
#                         "Timestamp": ts,
#                         "MassFlow": e.get(prefix + "MassFlow", 0),
#                         "Masstotal": e.get(prefix + "Masstotal", 0),
#                         "VolumeFlow": e.get(prefix + "VolumeFlow", 0),
#                         "Volumetotal": e.get(prefix + "Volumetotal", 0),
#                         "Density": e.get(prefix + "Density", 0),
#                         "Temp": e.get(prefix + "Temp", 0)
#                     }
#                     records.append(record)
#             except Exception as e:
#                 print(f"Error processing record: {e}")
#                 continue
        
#         print(f"Records found for {prefix}: {len(records)}")  # Debug print
        
#     except Exception as e:
#         print(f"Query error: {e}")
#         return []
    
#     return records
# # =========================
# # CSV DOWNLOAD
# # =========================
# @app.route("/download_csv")
# @login_required
# def download_csv():

#     prefix = request.args.get("type")
#     start = request.args.get("start").replace("T", " ")
#     end = request.args.get("end").replace("T", " ")

#     data = fetch_records(prefix, start, end)

#     df = pd.DataFrame(data)

#     output = BytesIO()
#     df.to_csv(output, index=False)
#     output.seek(0)

#     return send_file(
#         output,
#         mimetype="text/csv",
#         download_name="report.csv",
#         as_attachment=True
#     )



# @app.route("/download_pdf")
# @login_required
# def download_pdf():
#     try:
#         prefix = request.args.get("type")
#         start = request.args.get("start").replace("T", " ")
#         end = request.args.get("end").replace("T", " ")
        
#         # Fetch data
#         data = fetch_records(prefix, start, end)
        
#         buffer = BytesIO()
        
#         if not data or len(data) == 0:
#             from reportlab.pdfgen import canvas
#             from reportlab.lib.pagesizes import letter
            
#             c = canvas.Canvas(buffer, pagesize=letter)
#             c.setFont("Helvetica-Bold", 16)
#             c.drawString(100, 750, f"No Data Found for {prefix}")
#             c.setFont("Helvetica", 12)
#             c.drawString(100, 700, f"From: {start}")
#             c.drawString(100, 680, f"To: {end}")
#             c.save()
#         else:
#             from reportlab.lib.pagesizes import landscape, letter
#             from reportlab.pdfgen import canvas
#             from math import ceil
            
#             c = canvas.Canvas(buffer, pagesize=landscape(letter))
            
#             # Calculate pages needed (approximately 45 records per page)
#             records_per_page = 45
#             total_pages = ceil(len(data) / records_per_page)
#             current_page = 1
            
#             for page_start in range(0, len(data), records_per_page):
#                 page_end = min(page_start + records_per_page, len(data))
                
#                 if current_page > 1:
#                     c.showPage()
                
#                 # Header
#                 c.setFont("Helvetica-Bold", 12)
#                 c.drawString(50, 550, f"{prefix} Flow Meter Report - Page {current_page}/{total_pages}")
#                 c.setFont("Helvetica", 10)
#                 c.drawString(50, 535, f"From: {start}  To: {end}")
#                 c.drawString(50, 520, f"Records: {page_start+1} to {page_end} of {len(data)}")
                
#                 # Headers
#                 y = 500
#                 c.setFont("Helvetica-Bold", 8)
#                 c.drawString(50, y, "Timestamp")
#                 c.drawString(200, y, "Mass Flow")
#                 c.drawString(270, y, "Mass Total")
#                 c.drawString(340, y, "Volume Flow")
#                 c.drawString(410, y, "Volume Total")
#                 c.drawString(480, y, "Density")
#                 c.drawString(550, y, "Temp")
                
#                 y -= 15
#                 c.setFont("Helvetica", 7)
                
#                 # Write ALL records for this page
#                 for i, row in enumerate(data[page_start:page_end]):
#                     if y < 50:
#                         break
                    
#                     c.drawString(50, y, str(row.get("Timestamp", ""))[:16])
#                     c.drawString(200, y, f"{float(row.get('MassFlow',0)):.5f}")
#                     c.drawString(270, y, f"{float(row.get('Masstotal',0)):.5f}")
#                     c.drawString(340, y, f"{float(row.get('VolumeFlow',0)):.5f}")
#                     c.drawString(410, y, f"{float(row.get('Volumetotal',0)):.5f}")
#                     c.drawString(480, y, f"{float(row.get('Density',0)):.5f}")
#                     c.drawString(550, y, f"{float(row.get('Temp',0)):.5f}")
#                     y -= 12
                
#                 current_page += 1
            
#             c.save()
        
#         buffer.seek(0)
        
#         filename = f"{prefix}_report_{start.replace(' ', '_')}_to_{end.replace(' ', '_')}.pdf"
        
#         return send_file(
#             buffer,
#             mimetype="application/pdf",
#             download_name=filename,
#             as_attachment=True
#         )
        
#     except Exception as e:
#         print(f"PDF download error: {e}")
#         return jsonify({"error": str(e)}), 500

# New logic

# Add these imports at the top if not already present
from io import BytesIO
import pandas as pd
from datetime import datetime
from functools import wraps

def parse_dt(value):
    """Parse datetime string to datetime object"""
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M")
        except ValueError:
            return datetime.strptime(value.replace('T', ' '), "%Y-%m-%d %H:%M")

def fetch_engine_consumption(engine_type, start, end, interval='hour'):
    """Fetch engine consumption data based on engine type"""
    deviceid = "susanmpa"
    
    # Define meter pairs for each engine type
    engine_config = {
        'PME': {
            'name': 'PME Main Engine (P)',
            'inlet': 'FT1',
            'outlet': 'FT2',
            'formula': 'FT1VolumeTotal - FT2VolumeTotal'
        },
        'SME': {
            'name': 'SME Main Engine (S)',
            'inlet': 'FT3',
            'outlet': 'FT4',
            'formula': 'FT3VolumeTotal - FT4VolumeTotal'
        },
        'PAE': {
            'name': 'PAE Auxiliary Engine (P)',
            'inlet': 'FT5',
            'outlet': 'FT6',
            'formula': 'FT5VolumeTotal - FT6VolumeTotal'
        },
        'SAE': {
            'name': 'SAE Auxiliary Engine (S)',
            'inlet': 'FT7',
            'outlet': 'FT8',
            'formula': 'FT7VolumeTotal - FT8VolumeTotal'
        },
        'consumpution': {
            'name': 'Total Consumption',
            'meter': 'FT9',
            'formula': 'FT9VolumeTotal'
        }
    }
    
    if engine_type not in engine_config:
        return None
    
    config = engine_config[engine_type]
    
    try:
        start_dt = parse_dt(start)
        end_dt = parse_dt(end)
    except Exception as e:
        print(f"Date parsing error: {e}")
        return None
    
    # Query all entities
    query = f"PartitionKey eq '{deviceid}'"
    entities = list(table_client_2.query_entities(query))
    print(f"Total entities found: {len(entities)}")
    
    # Filter entities by time range
    filtered_entities = []
    for e in entities:
        ts = e.get("TimestampIST")
        if not ts:
            continue
        try:
            ts_dt = parse_dt(ts)
            if start_dt <= ts_dt <= end_dt:
                filtered_entities.append(e)
        except:
            continue
    
    print(f"Filtered entities: {len(filtered_entities)}")
    
    if not filtered_entities:
        return {
            'engine_type': engine_type,
            'name': config['name'],
            'records': [],
            'total_consumption': 0,
            'avg_consumption': 0,
            'record_count': 0,
            'interval': interval
        }
    
    # Sort entities by timestamp
    filtered_entities.sort(key=lambda x: x.get('TimestampIST', ''))
    
    # Process records based on engine type
    records = []
    consumption_by_interval = {}
    
    for e in filtered_entities:
        ts = e.get("TimestampIST")
        ts_dt = parse_dt(ts)
        
        # Debug: Print first record to see available fields
        if len(records) == 0:
            print(f"Sample record keys: {list(e.keys())}")
        
        if engine_type == 'consumpution':
            # Total Consumption - FT9 only
            current_value = float(e.get("FT9Volumetotal", 0) or 0)
            inlet_value = current_value
            outlet_value = 0
            consumption = current_value
            
            record_data = {
                "FT9_VolumeTotal": current_value
            }
            
            # Get other FT9 fields
            ft9_massflow = float(e.get("FT9MassFlow", 0) or 0)
            ft9_temp = float(e.get("FT9Temp", 0) or 0)
            ft9_density = float(e.get("FT9Density", 0) or 0)
        else:
            # Engine consumption (Inlet - Outlet)
            inlet_vol = float(e.get(config['inlet'] + "Volumetotal", 0) or 0)
            outlet_vol = float(e.get(config['outlet'] + "Volumetotal", 0) or 0)
            consumption = inlet_vol - outlet_vol
            
            # Get other fields
            inlet_massflow = float(e.get(config['inlet'] + "MassFlow", 0) or 0)
            outlet_massflow = float(e.get(config['outlet'] + "MassFlow", 0) or 0)
            inlet_temp = float(e.get(config['inlet'] + "Temp", 0) or 0)
            outlet_temp = float(e.get(config['outlet'] + "Temp", 0) or 0)
            inlet_density = float(e.get(config['inlet'] + "Density", 0) or 0)
            outlet_density = float(e.get(config['outlet'] + "Density", 0) or 0)
            
            record_data = {
                f"{config['inlet']}_VolumeTotal": inlet_vol,
                f"{config['outlet']}_VolumeTotal": outlet_vol
            }
        
        # Determine interval key
        if interval == 'minute':
            interval_key = ts_dt.strftime("%Y-%m-%d %H:%M")
        elif interval == 'hour':
            interval_key = ts_dt.strftime("%Y-%m-%d %H:00")
        elif interval == 'daily':
            interval_key = ts_dt.strftime("%Y-%m-%d")
        elif interval == 'monthly':
            interval_key = ts_dt.strftime("%Y-%m")
        elif interval == 'yearly':
            interval_key = ts_dt.strftime("%Y")
        else:
            interval_key = ts
        
        record = {
            "Timestamp": ts,
            "Interval": interval_key,
            "EngineType": engine_type,
            "EngineName": config['name'],
            "Consumption": round(consumption, 5),
            **record_data
        }
        
        # Add additional fields based on engine type
        if engine_type != 'consumpution':
            record.update({
                f"{config['inlet']}_MassFlow": round(inlet_massflow, 5),
                f"{config['outlet']}_MassFlow": round(outlet_massflow, 5),
                f"{config['inlet']}_Temp": round(inlet_temp, 2),
                f"{config['outlet']}_Temp": round(outlet_temp, 2),
                f"{config['inlet']}_Density": round(inlet_density, 2),
                f"{config['outlet']}_Density": round(outlet_density, 2),
                "InletValue": round(inlet_vol, 5),
                "OutletValue": round(outlet_vol, 5)
            })
        else:
            record.update({
                "FT9_MassFlow": round(ft9_massflow, 5),
                "FT9_Temp": round(ft9_temp, 2),
                "FT9_Density": round(ft9_density, 2),
                "InletValue": round(current_value, 5),
                "OutletValue": 0
            })
        
        if interval != 'raw':
            # Aggregate by interval
            if interval_key not in consumption_by_interval:
                consumption_by_interval[interval_key] = {
                    'count': 0,
                    'total_inlet': 0,
                    'total_outlet': 0,
                    'total_consumption': 0,
                    'total_inlet_mass': 0,
                    'total_outlet_mass': 0,
                    'total_inlet_temp': 0,
                    'total_outlet_temp': 0,
                    'total_inlet_density': 0,
                    'total_outlet_density': 0,
                    'first_timestamp': ts
                }
            
            agg = consumption_by_interval[interval_key]
            agg['count'] += 1
            agg['total_inlet'] += (inlet_vol if engine_type != 'consumpution' else current_value)
            agg['total_outlet'] += (outlet_vol if engine_type != 'consumpution' else 0)
            agg['total_consumption'] += consumption
            
            if engine_type != 'consumpution':
                agg['total_inlet_mass'] += inlet_massflow
                agg['total_outlet_mass'] += outlet_massflow
                agg['total_inlet_temp'] += inlet_temp
                agg['total_outlet_temp'] += outlet_temp
                agg['total_inlet_density'] += inlet_density
                agg['total_outlet_density'] += outlet_density
            else:
                agg['total_inlet_mass'] += ft9_massflow
                agg['total_inlet_temp'] += ft9_temp
                agg['total_inlet_density'] += ft9_density
        else:
            records.append(record)
    
    # Create aggregated records for intervals
    if interval != 'raw':
        for interval_key, agg in consumption_by_interval.items():
            count = agg['count']
            agg_record = {
                "Timestamp": agg['first_timestamp'],
                "Interval": interval_key,
                "EngineType": engine_type,
                "EngineName": config['name'],
                "Consumption": round(agg['total_consumption'], 5),
                "RecordCount": count,
                "InletValue": round(agg['total_inlet'], 5),
                "OutletValue": round(agg['total_outlet'], 5)
            }
            
            if engine_type != 'consumpution':
                agg_record.update({
                    f"{config['inlet']}_VolumeTotal": round(agg['total_inlet'], 5),
                    f"{config['outlet']}_VolumeTotal": round(agg['total_outlet'], 5),
                    f"{config['inlet']}_MassFlow": round(agg['total_inlet_mass'] / count, 5),
                    f"{config['outlet']}_MassFlow": round(agg['total_outlet_mass'] / count, 5),
                    f"{config['inlet']}_Temp": round(agg['total_inlet_temp'] / count, 2),
                    f"{config['outlet']}_Temp": round(agg['total_outlet_temp'] / count, 2),
                    f"{config['inlet']}_Density": round(agg['total_inlet_density'] / count, 2),
                    f"{config['outlet']}_Density": round(agg['total_outlet_density'] / count, 2)
                })
            else:
                agg_record.update({
                    "FT9_VolumeTotal": round(agg['total_inlet'], 5),
                    "FT9_MassFlow": round(agg['total_inlet_mass'] / count, 5),
                    "FT9_Temp": round(agg['total_inlet_temp'] / count, 2),
                    "FT9_Density": round(agg['total_inlet_density'] / count, 2)
                })
            
            records.append(agg_record)
        
        # Sort by timestamp
        records.sort(key=lambda x: x['Timestamp'])
    
    total_consumption = sum(r['Consumption'] for r in records)
    avg_consumption = total_consumption / len(records) if records else 0
    
    return {
        'engine_type': engine_type,
        'name': config['name'],
        'formula': config['formula'],
        'records': records,
        'total_consumption': round(total_consumption, 5),
        'avg_consumption': round(avg_consumption, 5),
        'record_count': len(records),
        'interval': interval
    }

@app.route("/download_csv")
@login_required
def download_csv():
    """Download CSV report for selected engine"""
    try:
        # Get parameters
        engine_type = request.args.get("type", "PME")
        start = request.args.get("start", "").replace("T", " ")
        end = request.args.get("end", "").replace("T", " ")
        interval = request.args.get("interval", "hour")
        
        if not start or not end:
            return jsonify({"error": "Start and end time required"}), 400
        
        print(f"CSV Request - Engine: {engine_type}, Interval: {interval}, Start: {start}, End: {end}")
        
        # Fetch data based on engine type
        result = fetch_engine_consumption(engine_type, start, end, interval)
        
        if not result:
            return jsonify({"error": "Invalid engine type"}), 400
        
        # Create DataFrame from records
        if result['records']:
            df = pd.DataFrame(result['records'])
            
            # Reorder columns based on engine type and interval
            base_columns = ['Timestamp', 'Interval', 'EngineType', 'EngineName']
            
            if engine_type == 'consumpution':
                value_columns = ['FT9_VolumeTotal', 'Consumption', 'FT9_MassFlow', 'FT9_Temp', 'FT9_Density']
            else:
                config = {
                    'PME': {'inlet': 'FT1', 'outlet': 'FT2'},
                    'SME': {'inlet': 'FT3', 'outlet': 'FT4'},
                    'PAE': {'inlet': 'FT5', 'outlet': 'FT6'},
                    'SAE': {'inlet': 'FT7', 'outlet': 'FT8'}
                }
                cfg = config[engine_type]
                value_columns = [
                    f"{cfg['inlet']}_VolumeTotal", f"{cfg['outlet']}_VolumeTotal", 'Consumption',
                    f"{cfg['inlet']}_MassFlow", f"{cfg['outlet']}_MassFlow",
                    f"{cfg['inlet']}_Temp", f"{cfg['outlet']}_Temp",
                    f"{cfg['inlet']}_Density", f"{cfg['outlet']}_Density"
                ]
            
            if interval != 'raw':
                base_columns.append('RecordCount')
            
            column_order = base_columns + value_columns
            column_order = [col for col in column_order if col in df.columns]
            df = df[column_order]
            
            # Add summary row
            summary = {
                'EngineType': 'SUMMARY',
                'EngineName': result['name'],
                'Consumption': result['total_consumption'],
                'RecordCount': result['record_count']
            }
            
            # Fill missing columns in summary
            for col in column_order:
                if col not in summary and col not in ['Timestamp', 'Interval']:
                    summary[col] = ''
            
            df = pd.concat([df, pd.DataFrame([summary])], ignore_index=True)
        else:
            df = pd.DataFrame([{
                'Timestamp': 'No data found',
                'EngineType': engine_type,
                'EngineName': result['name'] if result else engine_type,
                'Message': f'No records found for {engine_type} from {start} to {end}'
            }])
        
        # Create CSV
        output = BytesIO()
        df.to_csv(output, index=False)
        output.seek(0)
        
        # Generate filename
        filename = f"{engine_type}_{interval}_{start.replace(' ', '_')}_to_{end.replace(' ', '_')}.csv"
        
        return send_file(
            output,
            mimetype="text/csv",
            download_name=filename,
            as_attachment=True
        )
        
    except Exception as e:
        print(f"CSV download error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/download_pdf")
@login_required
def download_pdf():
    """Download PDF report for selected engine"""
    try:
        from reportlab.lib.pagesizes import letter, landscape
        from reportlab.pdfgen import canvas
        from reportlab.lib.utils import simpleSplit
        from math import ceil
        
        # Get parameters
        engine_type = request.args.get("type", "PME")
        start = request.args.get("start", "").replace("T", " ")
        end = request.args.get("end", "").replace("T", " ")
        interval = request.args.get("interval", "hour")
        
        if not start or not end:
            return jsonify({"error": "Start and end time required"}), 400
        
        # Fetch data
        result = fetch_engine_consumption(engine_type, start, end, interval)
        
        if not result:
            return jsonify({"error": "Invalid engine type"}), 400
        
        # Create PDF
        buffer = BytesIO()
        c = canvas.Canvas(buffer, pagesize=landscape(letter))
        
        # Title Page
        c.setFont("Helvetica-Bold", 20)
        c.drawString(50, 550, f"{result['name']} Report")
        
        c.setFont("Helvetica", 12)
        c.drawString(50, 520, f"From: {start}")
        c.drawString(350, 520, f"To: {end}")
        c.drawString(50, 500, f"Interval: {interval.upper()}")
        c.drawString(350, 500, f"Formula: {result['formula']}")
        
        # Summary Statistics
        c.setFont("Helvetica-Bold", 14)
        c.drawString(50, 450, "Summary Statistics")
        
        c.setFont("Helvetica", 12)
        c.drawString(50, 420, f"Total Records: {result['record_count']}")
        c.drawString(50, 400, f"Total Consumption: {result['total_consumption']} L")
        c.drawString(50, 380, f"Average Consumption: {result['avg_consumption']} L")
        
        # Add line
        c.line(50, 350, 750, 350)
        
        # New page for detailed data
        c.showPage()
        
        # Detailed Data Page
        c.setFont("Helvetica-Bold", 16)
        c.drawString(50, 550, f"{result['name']} - Detailed Readings")
        c.setFont("Helvetica", 10)
        c.drawString(50, 530, f"From: {start}  To: {end}")
        c.drawString(350, 530, f"Interval: {interval.upper()}")
        
        if not result['records']:
            c.setFont("Helvetica", 12)
            c.drawString(50, 450, "No data found for selected date range")
        else:
            # Table headers
            y = 500
            c.setFont("Helvetica-Bold", 8)
            
            if engine_type == 'consumpution':
                c.drawString(50, y, "Timestamp")
                c.drawString(180, y, "FT9 Volume")
                c.drawString(260, y, "Consumption")
                c.drawString(340, y, "Mass Flow")
                c.drawString(420, y, "Temp")
                c.drawString(500, y, "Density")
            else:
                c.drawString(50, y, "Timestamp")
                c.drawString(170, y, "Inlet Vol")
                c.drawString(240, y, "Outlet Vol")
                c.drawString(310, y, "Consumption")
                c.drawString(380, y, "Inlet Mass")
                c.drawString(450, y, "Outlet Mass")
                c.drawString(520, y, "Inlet Temp")
                c.drawString(590, y, "Outlet Temp")
            
            y -= 15
            c.setFont("Helvetica", 7)
            
            # Calculate pages needed
            records_per_page = 30
            total_pages = ceil(len(result['records']) / records_per_page)
            
            for page in range(total_pages):
                if page > 0:
                    c.showPage()
                    y = 550
                    c.setFont("Helvetica-Bold", 8)
                    c.drawString(50, y, f"{result['name']} - Page {page+1}/{total_pages}")
                    y -= 20
                    c.setFont("Helvetica-Bold", 8)
                    
                    if engine_type == 'consumpution':
                        c.drawString(50, y, "Timestamp")
                        c.drawString(180, y, "FT9 Volume")
                        c.drawString(260, y, "Consumption")
                        c.drawString(340, y, "Mass Flow")
                        c.drawString(420, y, "Temp")
                        c.drawString(500, y, "Density")
                    else:
                        c.drawString(50, y, "Timestamp")
                        c.drawString(170, y, "Inlet Vol")
                        c.drawString(240, y, "Outlet Vol")
                        c.drawString(310, y, "Consumption")
                        c.drawString(380, y, "Inlet Mass")
                        c.drawString(450, y, "Outlet Mass")
                        c.drawString(520, y, "Inlet Temp")
                        c.drawString(590, y, "Outlet Temp")
                    
                    y -= 15
                    c.setFont("Helvetica", 7)
                
                page_records = result['records'][page * records_per_page:(page + 1) * records_per_page]
                
                for record in page_records:
                    if y < 50:
                        break
                    
                    if engine_type == 'consumpution':
                        c.drawString(50, y, str(record.get('Timestamp', ''))[:16])
                        c.drawString(180, y, f"{record.get('FT9_VolumeTotal', 0):.2f}")
                        c.drawString(260, y, f"{record.get('Consumption', 0):.2f}")
                        c.drawString(340, y, f"{record.get('FT9_MassFlow', 0):.2f}")
                        c.drawString(420, y, f"{record.get('FT9_Temp', 0):.1f}")
                        c.drawString(500, y, f"{record.get('FT9_Density', 0):.2f}")
                    else:
                        c.drawString(50, y, str(record.get('Timestamp', ''))[:16])
                        
                        if engine_type == 'PME':
                            c.drawString(170, y, f"{record.get('FT1_VolumeTotal', 0):.2f}")
                            c.drawString(240, y, f"{record.get('FT2_VolumeTotal', 0):.2f}")
                            c.drawString(310, y, f"{record.get('Consumption', 0):.2f}")
                            c.drawString(380, y, f"{record.get('FT1_MassFlow', 0):.2f}")
                            c.drawString(450, y, f"{record.get('FT2_MassFlow', 0):.2f}")
                            c.drawString(520, y, f"{record.get('FT1_Temp', 0):.1f}")
                            c.drawString(590, y, f"{record.get('FT2_Temp', 0):.1f}")
                        elif engine_type == 'SME':
                            c.drawString(170, y, f"{record.get('FT3_VolumeTotal', 0):.2f}")
                            c.drawString(240, y, f"{record.get('FT4_VolumeTotal', 0):.2f}")
                            c.drawString(310, y, f"{record.get('Consumption', 0):.2f}")
                            c.drawString(380, y, f"{record.get('FT3_MassFlow', 0):.2f}")
                            c.drawString(450, y, f"{record.get('FT4_MassFlow', 0):.2f}")
                            c.drawString(520, y, f"{record.get('FT3_Temp', 0):.1f}")
                            c.drawString(590, y, f"{record.get('FT4_Temp', 0):.1f}")
                        elif engine_type == 'PAE':
                            c.drawString(170, y, f"{record.get('FT5_VolumeTotal', 0):.2f}")
                            c.drawString(240, y, f"{record.get('FT6_VolumeTotal', 0):.2f}")
                            c.drawString(310, y, f"{record.get('Consumption', 0):.2f}")
                            c.drawString(380, y, f"{record.get('FT5_MassFlow', 0):.2f}")
                            c.drawString(450, y, f"{record.get('FT6_MassFlow', 0):.2f}")
                            c.drawString(520, y, f"{record.get('FT5_Temp', 0):.1f}")
                            c.drawString(590, y, f"{record.get('FT6_Temp', 0):.1f}")
                        elif engine_type == 'SAE':
                            c.drawString(170, y, f"{record.get('FT7_VolumeTotal', 0):.2f}")
                            c.drawString(240, y, f"{record.get('FT8_VolumeTotal', 0):.2f}")
                            c.drawString(310, y, f"{record.get('Consumption', 0):.2f}")
                            c.drawString(380, y, f"{record.get('FT7_MassFlow', 0):.2f}")
                            c.drawString(450, y, f"{record.get('FT8_MassFlow', 0):.2f}")
                            c.drawString(520, y, f"{record.get('FT7_Temp', 0):.1f}")
                            c.drawString(590, y, f"{record.get('FT8_Temp', 0):.1f}")
                    
                    y -= 12
        
        c.save()
        buffer.seek(0)
        
        # Generate filename
        filename = f"{engine_type}_{interval}_{start.replace(' ', '_')}_to_{end.replace(' ', '_')}.pdf"
        
        return send_file(
            buffer,
            mimetype="application/pdf",
            download_name=filename,
            as_attachment=True
        )
        
    except Exception as e:
        print(f"PDF download error: {e}")
        return jsonify({"error": str(e)}), 500


# =========================
# RUN APP
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)




















