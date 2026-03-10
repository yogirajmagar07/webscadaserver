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
    deviceid = "susanmpa"
    
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
        entities = list(table_client_2.query_entities(query))
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
# NEW: Group Report Functions
# =========================

def fetch_group_data(group, start, end, interval='raw'):
    """Fetch and calculate engine consumption data for a group with interval aggregation"""
    
    # Define meter pairs for each group
    group_config = {
        'PME': {
            'name': 'Main Engine 1 (PME)',
            'inlet': 'FT1',
            'outlet': 'FT2',
            'inlet_label': 'PME INLET',
            'outlet_label': 'PME OUTLET'
        },
        'SME': {
            'name': 'Main Engine 2 (SME)',
            'inlet': 'FT3',
            'outlet': 'FT4',
            'inlet_label': 'SME INLET',
            'outlet_label': 'SME OUTLET'
        },
        'PAE': {
            'name': 'Generator 1 (PAE)',
            'inlet': 'FT5',
            'outlet': 'FT6',
            'inlet_label': 'PAE INLET',
            'outlet_label': 'PAE OUTLET'
        },
        'SAE': {
            'name': 'Generator 2 (SAE)',
            'inlet': 'FT7',
            'outlet': 'FT8',
            'inlet_label': 'SAE INLET',
            'outlet_label': 'SAE OUTLET'
        }
    }
    
    if group not in group_config:
        return None
    
    config = group_config[group]
    deviceid = "susanmpa"
    
    try:
        start_dt = parse_dt(start)
        end_dt = parse_dt(end)
    except Exception as e:
        print(f"Date parsing error: {e}")
        return None
    
    # Fetch all entities
    query = f"PartitionKey eq '{deviceid}'"
    entities = list(table_client_2.query_entities(query))
    
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
    
    if not filtered_entities:
        return {
            'group': group,
            'name': config['name'],
            'inlet_meter': config['inlet'],
            'outlet_meter': config['outlet'],
            'inlet_label': config['inlet_label'],
            'outlet_label': config['outlet_label'],
            'records': [],
            'total_consumption': 0,
            'avg_consumption': 0,
            'record_count': 0,
            'interval': interval
        }
    
    # Sort entities by timestamp
    filtered_entities.sort(key=lambda x: x.get('TimestampIST', ''))
    
    # Process records and calculate consumption based on interval
    records = []
    consumption_by_interval = {}
    
    for e in filtered_entities:
        ts = e.get("TimestampIST")
        ts_dt = parse_dt(ts)
        
        # Get inlet and outlet volume total values
        inlet_vol_total = float(e.get(config['inlet'] + "Volumetotal", 0) or 0)
        outlet_vol_total = float(e.get(config['outlet'] + "Volumetotal", 0) or 0)
        
        # Calculate consumption (Inlet - Outlet)
        consumption = inlet_vol_total - outlet_vol_total
        
        # Determine interval key based on selected interval
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
        else:  # raw data
            interval_key = ts
        
        record = {
            "Timestamp": ts,
            "Interval": interval_key,
            "InletMeter": config['inlet'],
            "OutletMeter": config['outlet'],
            "InletVolumeTotal": round(inlet_vol_total, 5),
            "OutletVolumeTotal": round(outlet_vol_total, 5),
            "Consumption": round(consumption, 5),
            "InletMassFlow": round(float(e.get(config['inlet'] + "MassFlow", 0) or 0), 5),
            "OutletMassFlow": round(float(e.get(config['outlet'] + "MassFlow", 0) or 0), 5),
            "InletTemp": round(float(e.get(config['inlet'] + "Temp", 0) or 0), 2),
            "OutletTemp": round(float(e.get(config['outlet'] + "Temp", 0) or 0), 2),
            "InletDensity": round(float(e.get(config['inlet'] + "Density", 0) or 0), 2),
            "OutletDensity": round(float(e.get(config['outlet'] + "Density", 0) or 0), 2)
        }
        
        if interval != 'raw':
            # Aggregate by interval
            if interval_key not in consumption_by_interval:
                consumption_by_interval[interval_key] = {
                    'count': 0,
                    'total_inlet_vol': 0,
                    'total_outlet_vol': 0,
                    'total_consumption': 0,
                    'avg_inlet_mass': 0,
                    'avg_outlet_mass': 0,
                    'avg_inlet_temp': 0,
                    'avg_outlet_temp': 0,
                    'avg_inlet_density': 0,
                    'avg_outlet_density': 0,
                    'first_timestamp': ts
                }
            
            agg = consumption_by_interval[interval_key]
            agg['count'] += 1
            agg['total_inlet_vol'] += inlet_vol_total
            agg['total_outlet_vol'] += outlet_vol_total
            agg['total_consumption'] += consumption
            agg['avg_inlet_mass'] += float(e.get(config['inlet'] + "MassFlow", 0) or 0)
            agg['avg_outlet_mass'] += float(e.get(config['outlet'] + "MassFlow", 0) or 0)
            agg['avg_inlet_temp'] += float(e.get(config['inlet'] + "Temp", 0) or 0)
            agg['avg_outlet_temp'] += float(e.get(config['outlet'] + "Temp", 0) or 0)
            agg['avg_inlet_density'] += float(e.get(config['inlet'] + "Density", 0) or 0)
            agg['avg_outlet_density'] += float(e.get(config['outlet'] + "Density", 0) or 0)
        else:
            records.append(record)
    
    # Create aggregated records for intervals
    if interval != 'raw':
        for interval_key, agg in consumption_by_interval.items():
            count = agg['count']
            records.append({
                "Timestamp": agg['first_timestamp'],
                "Interval": interval_key,
                "InletMeter": config['inlet'],
                "OutletMeter": config['outlet'],
                "InletVolumeTotal": round(agg['total_inlet_vol'], 5),
                "OutletVolumeTotal": round(agg['total_outlet_vol'], 5),
                "Consumption": round(agg['total_consumption'], 5),
                "InletMassFlow": round(agg['avg_inlet_mass'] / count, 5),
                "OutletMassFlow": round(agg['avg_outlet_mass'] / count, 5),
                "InletTemp": round(agg['avg_inlet_temp'] / count, 2),
                "OutletTemp": round(agg['avg_outlet_temp'] / count, 2),
                "InletDensity": round(agg['avg_inlet_density'] / count, 2),
                "OutletDensity": round(agg['avg_outlet_density'] / count, 2),
                "RecordCount": count
            })
        
        # Sort by timestamp
        records.sort(key=lambda x: x['Timestamp'])
    
    total_consumption = sum(r['Consumption'] for r in records)
    avg_consumption = total_consumption / len(records) if records else 0
    
    return {
        'group': group,
        'name': config['name'],
        'inlet_meter': config['inlet'],
        'outlet_meter': config['outlet'],
        'inlet_label': config['inlet_label'],
        'outlet_label': config['outlet_label'],
        'records': records,
        'total_consumption': round(total_consumption, 5),
        'avg_consumption': round(avg_consumption, 5),
        'record_count': len(records),
        'interval': interval
    }


@app.route("/download_group_csv")
@login_required
def download_group_csv():
    try:
        group = request.args.get("group")
        start = request.args.get("start").replace("T", " ")
        end = request.args.get("end").replace("T", " ")
        interval = request.args.get("interval", "raw")
        
        print(f"Group CSV Request - Group: {group}, Interval: {interval}, Start: {start}, End: {end}")
        
        result = fetch_group_data(group, start, end, interval)
        
        if not result:
            return jsonify({"error": "Invalid group"}), 400
        
        # Create DataFrame from records
        if result['records']:
            df = pd.DataFrame(result['records'])
            
            # Reorder columns based on interval
            if interval == 'raw':
                column_order = [
                    'Timestamp', 'InletMeter', 'OutletMeter', 
                    'InletVolumeTotal', 'OutletVolumeTotal', 'Consumption',
                    'InletMassFlow', 'OutletMassFlow', 
                    'InletTemp', 'OutletTemp',
                    'InletDensity', 'OutletDensity'
                ]
            else:
                column_order = [
                    'Interval', 'RecordCount', 'InletMeter', 'OutletMeter',
                    'InletVolumeTotal', 'OutletVolumeTotal', 'Consumption',
                    'InletMassFlow', 'OutletMassFlow', 
                    'InletTemp', 'OutletTemp',
                    'InletDensity', 'OutletDensity'
                ]
            
            # Filter only available columns
            column_order = [col for col in column_order if col in df.columns]
            df = df[column_order]
            
            # Add summary row
            summary = pd.DataFrame([{
                'Interval': 'SUMMARY' if interval != 'raw' else 'SUMMARY',
                'RecordCount': result['record_count'],
                'InletVolumeTotal': '',
                'OutletVolumeTotal': '',
                'Consumption': result['total_consumption'],
                'InletMassFlow': '',
                'OutletMassFlow': '',
                'InletTemp': '',
                'OutletTemp': '',
                'InletDensity': '',
                'OutletDensity': ''
            }])
            
            # Fill missing columns in summary
            for col in column_order:
                if col not in summary.columns:
                    summary[col] = ''
            
            df = pd.concat([df, summary], ignore_index=True)
        else:
            df = pd.DataFrame([{
                'Timestamp': 'No data found',
                'Interval': 'No data',
                'InletMeter': group,
                'OutletMeter': '',
                'InletVolumeTotal': 0,
                'OutletVolumeTotal': 0,
                'Consumption': 0,
                'Message': f'No records found for {group} from {start} to {end}'
            }])
        
        output = BytesIO()
        df.to_csv(output, index=False)
        output.seek(0)
        
        filename = f"{group}_{interval}_consumption_{start.replace(' ', '_')}_to_{end.replace(' ', '_')}.csv"
        
        return send_file(
            output,
            mimetype="text/csv",
            download_name=filename,
            as_attachment=True
        )
        
    except Exception as e:
        print(f"Group CSV download error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/download_group_pdf")
@login_required
def download_group_pdf():
    try:
        group = request.args.get("group")
        start = request.args.get("start").replace("T", " ")
        end = request.args.get("end").replace("T", " ")
        interval = request.args.get("interval", "raw")
        
        print(f"Group PDF Request - Group: {group}, Interval: {interval}, Start: {start}, End: {end}")
        
        result = fetch_group_data(group, start, end, interval)
        
        if not result:
            return jsonify({"error": "Invalid group"}), 400
        
        buffer = BytesIO()
        
        if not result['records']:
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import letter
            
            c = canvas.Canvas(buffer, pagesize=letter)
            c.setFont("Helvetica-Bold", 16)
            c.drawString(100, 750, f"No Data Found for {result['name']}")
            c.setFont("Helvetica", 12)
            c.drawString(100, 720, f"Group: {group}")
            c.drawString(100, 700, f"From: {start}")
            c.drawString(100, 680, f"To: {end}")
            c.drawString(100, 660, f"Interval: {interval}")
            c.drawString(100, 640, f"Meters: {result['inlet_label']} - {result['outlet_label']}")
            c.save()
        else:
            from reportlab.lib.pagesizes import landscape, letter
            from reportlab.pdfgen import canvas
            from math import ceil
            
            c = canvas.Canvas(buffer, pagesize=landscape(letter))
            
            # Summary Page
            c.setFont("Helvetica-Bold", 16)
            c.drawString(50, 550, f"{result['name']} Consumption Report")
            
            c.setFont("Helvetica", 12)
            c.drawString(50, 520, f"From: {start}")
            c.drawString(350, 520, f"To: {end}")
            c.drawString(50, 500, f"Interval: {interval.upper()}")
            c.drawString(350, 500, f"Meters: {result['inlet_label']} - {result['outlet_label']}")
            
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
            c.setFont("Helvetica-Bold", 14)
            c.drawString(50, 550, f"{result['name']} - {interval.upper()} Readings")
            c.setFont("Helvetica", 10)
            c.drawString(50, 530, f"From: {start}  To: {end}")
            
            # Table headers based on interval
            y = 500
            c.setFont("Helvetica-Bold", 8)
            
            if interval == 'raw':
                c.drawString(50, y, "Timestamp")
                c.drawString(200, y, "Inlet Vol")
                c.drawString(270, y, "Outlet Vol")
                c.drawString(340, y, "Consumption")
                c.drawString(410, y, "Inlet Mass")
                c.drawString(480, y, "Outlet Mass")
                c.drawString(550, y, "Inlet Temp")
                c.drawString(620, y, "Outlet Temp")
            else:
                c.drawString(50, y, "Interval")
                c.drawString(150, y, "Count")
                c.drawString(200, y, "Inlet Vol")
                c.drawString(270, y, "Outlet Vol")
                c.drawString(340, y, "Consumption")
                c.drawString(410, y, "Inlet Mass")
                c.drawString(480, y, "Outlet Mass")
                c.drawString(550, y, "Inlet Temp")
                c.drawString(620, y, "Outlet Temp")
            
            y -= 15
            c.setFont("Helvetica", 7)
            
            # Calculate pages needed
            records_per_page = 30
            total_pages = ceil(len(result['records']) / records_per_page)
            current_record = 0
            
            for page in range(total_pages):
                if page > 0:
                    c.showPage()
                    y = 550
                    c.setFont("Helvetica-Bold", 8)
                    c.drawString(50, y, f"{result['name']} - {interval.upper()} - Page {page+1}/{total_pages}")
                    y -= 20
                    c.setFont("Helvetica-Bold", 8)
                    
                    if interval == 'raw':
                        c.drawString(50, y, "Timestamp")
                        c.drawString(200, y, "Inlet Vol")
                        c.drawString(270, y, "Outlet Vol")
                        c.drawString(340, y, "Consumption")
                        c.drawString(410, y, "Inlet Mass")
                        c.drawString(480, y, "Outlet Mass")
                        c.drawString(550, y, "Inlet Temp")
                        c.drawString(620, y, "Outlet Temp")
                    else:
                        c.drawString(50, y, "Interval")
                        c.drawString(150, y, "Count")
                        c.drawString(200, y, "Inlet Vol")
                        c.drawString(270, y, "Outlet Vol")
                        c.drawString(340, y, "Consumption")
                        c.drawString(410, y, "Inlet Mass")
                        c.drawString(480, y, "Outlet Mass")
                        c.drawString(550, y, "Inlet Temp")
                        c.drawString(620, y, "Outlet Temp")
                    
                    y -= 15
                    c.setFont("Helvetica", 7)
                
                page_records = result['records'][page * records_per_page:(page + 1) * records_per_page]
                
                for record in page_records:
                    if y < 50:
                        break
                    
                    if interval == 'raw':
                        c.drawString(50, y, str(record['Timestamp'])[:16])
                        c.drawString(200, y, f"{record['InletVolumeTotal']:.2f}")
                        c.drawString(270, y, f"{record['OutletVolumeTotal']:.2f}")
                        c.drawString(340, y, f"{record['Consumption']:.2f}")
                        c.drawString(410, y, f"{record['InletMassFlow']:.2f}")
                        c.drawString(480, y, f"{record['OutletMassFlow']:.2f}")
                        c.drawString(550, y, f"{record['InletTemp']:.1f}")
                        c.drawString(620, y, f"{record['OutletTemp']:.1f}")
                    else:
                        c.drawString(50, y, str(record['Interval']))
                        c.drawString(150, y, str(record.get('RecordCount', 1)))
                        c.drawString(200, y, f"{record['InletVolumeTotal']:.2f}")
                        c.drawString(270, y, f"{record['OutletVolumeTotal']:.2f}")
                        c.drawString(340, y, f"{record['Consumption']:.2f}")
                        c.drawString(410, y, f"{record['InletMassFlow']:.2f}")
                        c.drawString(480, y, f"{record['OutletMassFlow']:.2f}")
                        c.drawString(550, y, f"{record['InletTemp']:.1f}")
                        c.drawString(620, y, f"{record['OutletTemp']:.1f}")
                    
                    y -= 12
                    current_record += 1
            
            c.save()
        
        buffer.seek(0)
        
        filename = f"{group}_{interval}_consumption_{start.replace(' ', '_')}_to_{end.replace(' ', '_')}.pdf"
        
        return send_file(
            buffer,
            mimetype="application/pdf",
            download_name=filename,
            as_attachment=True
        )
        
    except Exception as e:
        print(f"Group PDF download error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# =========================
# CSV DOWNLOAD (Original - Kept for backward compatibility)
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

# @app.route("/download_pdf")
# @login_required
# def download_pdf():
#     try:
#         prefix = request.args.get("type")
#         start = request.args.get("start").replace("T", " ")
#         end = request.args.get("end").replace("T", " ")
        
#         print(f"PDF Request - Prefix: {prefix}, Start: {start}, End: {end}")  # Debug
        
#         # Fetch data
#         data = fetch_records(prefix, start, end)
        
#         print(f"Data retrieved: {len(data)} records")  # Debug
        
#         buffer = BytesIO()
        
#         if not data or len(data) == 0:
#             # Create PDF with no data message
#             from reportlab.pdfgen import canvas
#             from reportlab.lib.pagesizes import letter
            
#             c = canvas.Canvas(buffer, pagesize=letter)
#             c.setFont("Helvetica-Bold", 16)
#             c.drawString(100, 750, f"No Data Found for {prefix}")
            
#             c.setFont("Helvetica", 12)
#             c.drawString(100, 700, f"From: {start}")
#             c.drawString(100, 680, f"To: {end}")
#             c.drawString(100, 660, f"Device: susanad")
#             c.drawString(100, 640, f"Time Range: {(parse_dt(end) - parse_dt(start)).total_seconds()/3600:.1f} hours")
            
#             c.setFont("Helvetica", 10)
#             c.drawString(100, 600, "Possible issues:")
#             c.drawString(120, 580, "1. No data exists for this time range")
#             c.drawString(120, 560, "2. The meter prefix might be incorrect")
#             c.drawString(120, 540, "3. Check if the device ID 'susanad' has data")
            
#             c.save()
#         else:
#             # Create PDF with data
#             from reportlab.lib.pagesizes import landscape, letter
#             from reportlab.pdfgen import canvas
            
#             c = canvas.Canvas(buffer, pagesize=landscape(letter))
            
#             # Header
#             c.setFont("Helvetica-Bold", 14)
#             c.drawString(50, 550, f"{prefix} Flow Meter Report")
            
#             c.setFont("Helvetica", 10)
#             c.drawString(50, 530, f"From: {start}")
#             c.drawString(350, 530, f"To: {end}")
#             c.drawString(50, 515, f"Total Records: {len(data)}")
            
#             # Table headers
#             y = 480
#             c.setFont("Helvetica-Bold", 8)
#             c.drawString(50, y, "Timestamp")
#             c.drawString(200, y, "Mass Flow")
#             c.drawString(270, y, "Mass Total")
#             c.drawString(340, y, "Volume Flow")
#             c.drawString(410, y, "Volume Total")
#             c.drawString(480, y, "Density")
#             c.drawString(550, y, "Temp")
            
#             y -= 15
#             c.setFont("Helvetica", 7)
            
#             # Show first 30 records
#             for i, row in enumerate(data[:30]):
#                 if y < 50:  # New page
#                     c.showPage()
#                     y = 550
#                     c.setFont("Helvetica-Bold", 8)
#                     c.drawString(50, y, "Timestamp (continued)")
#                     y -= 15
#                     c.setFont("Helvetica", 7)
                
#                 try:
#                     c.drawString(50, y, str(row.get("Timestamp", ""))[:16])
#                     c.drawString(200, y, f"{float(row.get('MassFlow', 0) or 0):.5f}")
#                     c.drawString(270, y, f"{float(row.get('Masstotal', 0) or 0):.5f}")
#                     c.drawString(340, y, f"{float(row.get('VolumeFlow', 0) or 0):.5f}")
#                     c.drawString(410, y, f"{float(row.get('Volumetotal', 0) or 0):.5f}")
#                     c.drawString(480, y, f"{float(row.get('Density', 0) or 0):.5f}")
#                     c.drawString(550, y, f"{float(row.get('Temp', 0) or 0):.5f}")
#                 except Exception as e:
#                     print(f"Error writing row {i}: {e}")
                
#                 y -= 12
            
#             # Add summary
#             if len(data) > 30:
#                 c.setFont("Helvetica-Oblique", 8)
#                 c.drawString(50, y-10, f"... and {len(data) - 30} more records")
            
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
#         import traceback
#         traceback.print_exc()
#         return jsonify({"error": str(e)}), 500

@app.route("/download_pdf")
@login_required
def download_pdf():
    try:
        prefix = request.args.get("type")
        start = request.args.get("start").replace("T", " ")
        end = request.args.get("end").replace("T", " ")
        
        # Fetch data
        data = fetch_records(prefix, start, end)
        
        buffer = BytesIO()
        
        if not data or len(data) == 0:
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import letter
            
            c = canvas.Canvas(buffer, pagesize=letter)
            c.setFont("Helvetica-Bold", 16)
            c.drawString(100, 750, f"No Data Found for {prefix}")
            c.setFont("Helvetica", 12)
            c.drawString(100, 700, f"From: {start}")
            c.drawString(100, 680, f"To: {end}")
            c.save()
        else:
            from reportlab.lib.pagesizes import landscape, letter
            from reportlab.pdfgen import canvas
            from math import ceil
            
            c = canvas.Canvas(buffer, pagesize=landscape(letter))
            
            # Calculate pages needed (approximately 45 records per page)
            records_per_page = 45
            total_pages = ceil(len(data) / records_per_page)
            current_page = 1
            
            for page_start in range(0, len(data), records_per_page):
                page_end = min(page_start + records_per_page, len(data))
                
                if current_page > 1:
                    c.showPage()
                
                # Header
                c.setFont("Helvetica-Bold", 12)
                c.drawString(50, 550, f"{prefix} Flow Meter Report - Page {current_page}/{total_pages}")
                c.setFont("Helvetica", 10)
                c.drawString(50, 535, f"From: {start}  To: {end}")
                c.drawString(50, 520, f"Records: {page_start+1} to {page_end} of {len(data)}")
                
                # Headers
                y = 500
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
                
                # Write ALL records for this page
                for i, row in enumerate(data[page_start:page_end]):
                    if y < 50:
                        break
                    
                    c.drawString(50, y, str(row.get("Timestamp", ""))[:16])
                    c.drawString(200, y, f"{float(row.get('MassFlow',0)):.5f}")
                    c.drawString(270, y, f"{float(row.get('Masstotal',0)):.5f}")
                    c.drawString(340, y, f"{float(row.get('VolumeFlow',0)):.5f}")
                    c.drawString(410, y, f"{float(row.get('Volumetotal',0)):.5f}")
                    c.drawString(480, y, f"{float(row.get('Density',0)):.5f}")
                    c.drawString(550, y, f"{float(row.get('Temp',0)):.5f}")
                    y -= 12
                
                current_page += 1
            
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
        return jsonify({"error": str(e)}), 500

# =========================
# RUN APP
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

















