import os
import uuid
from datetime import datetime
import pytz
from flask import Flask, request, jsonify
from azure.data.tables import TableServiceClient

app = Flask(__name__)

# Environment variables
TABLES_CONN = os.environ.get("TABLES_CONNECTION_STRING")
TABLE_NAME = os.environ.get("TABLE_NAME", "DeviceData")

if not TABLES_CONN:
    raise RuntimeError("TABLES_CONNECTION_STRING environment variable is not set")

# Create table client
service = TableServiceClient.from_connection_string(TABLES_CONN)
table_client = service.get_table_client(TABLE_NAME)

# Create table if not exists
try:
    table_client.create_table()
except Exception:
    pass


def build_entity(data: dict):
    """
    Build Azure Table entity with:
    - PartitionKey = deviceid (payload or default)
    - RowKey = IST timestamp unique
    """

    # DEFAULT DEVICE ID IF MISSING
    deviceid = str(data.get("deviceid", "susanad"))

    # Convert timestamp to IST
    utc_now = datetime.utcnow()
    ist_timezone = pytz.timezone("Asia/Kolkata")
    ist_time = utc_now.replace(tzinfo=pytz.utc).astimezone(ist_timezone)
    ist_timestamp = ist_time.strftime("%Y-%m-%d %H:%M:%S")

    # Safe RowKey (no :, space, /)
    safe_ts = ist_timestamp.replace(":", "-").replace(" ", "_").replace("/", "-")
    rowkey = f"{safe_ts}_{uuid.uuid4().hex}"

    entity = {
        "PartitionKey": deviceid,
        "RowKey": rowkey,
        "TimestampIST": ist_timestamp
    }

    # Add all payload fields
    for key, value in data.items():

        # Avoid overwriting PartitionKey again
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
        table_client.create_entity(entity=entity)
    except Exception as e:
        return jsonify({"error": "insert failed", "details": str(e)}), 500

    return jsonify({"status": "ok"}), 200


@app.route("/health", methods=["GET"])
def health():
    return "ok", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)