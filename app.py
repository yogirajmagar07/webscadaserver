import os
from datetime import datetime
import pytz
from flask import Flask, request, jsonify
from azure.data.tables import TableServiceClient

app = Flask(__name__)

# ===== Environment Variables =====
TABLES_CONN = os.environ.get("TABLES_CONNECTION_STRING")
TABLE_NAME = os.environ.get("TABLE_NAME", "DeviceData")

if not TABLES_CONN:
    raise RuntimeError("TABLES_CONNECTION_STRING environment variable is not set")

# ===== Azure Table Client =====
service = TableServiceClient.from_connection_string(TABLES_CONN)
table_client = service.get_table_client(TABLE_NAME)

# Create table if not exists
try:
    table_client.create_table()
except Exception:
    pass


def build_entity(data: dict):
    """
    Azure Table design:
    PartitionKey = deviceid
    RowKey       = YYYYMMDDHHMM  (1 record per minute)
    """

    # Device ID (default if missing)
    deviceid = str(data.get("deviceid", "susanad"))

    # IST timestamp
    ist_tz = pytz.timezone("Asia/Kolkata")
    ist_time = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(ist_tz)

    rowkey = ist_time.strftime("%Y%m%d%H%M")
    timestamp_ist = ist_time.strftime("%Y-%m-%d %H:%M:%S")

    entity = {
        "PartitionKey": deviceid,
        "RowKey": rowkey,
        "TimestampIST": timestamp_ist
    }

    # Add SCADA payload fields
    for key, value in data.items():
        if key == "deviceid":
            continue

        if isinstance(value, (dict, list)):
            value = str(value)

        entity[key] = value

    return entity


@app.route("/ingest", methods=["POST"])
def ingest():
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 400

    data = request.get_json()

    try:
        entity = build_entity(data)

        # Retry-safe (replaces same-minute data if resent)
        table_client.upsert_entity(entity=entity, mode="replace")

    except Exception as e:
        return jsonify({"error": "insert failed", "details": str(e)}), 500

    return jsonify({"status": "ok"}), 200


@app.route("/health", methods=["GET"])
def health():
    return "ok", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
