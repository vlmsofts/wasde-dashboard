"""
wasde_server.py  —  VLM Commodities WASDE Dashboard
Serves cotton_wasde_dashboard.html as a static site on Railway.
"""

from flask import Flask, send_file, jsonify
import os, json

app = Flask(__name__)

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
HTML_FILE = os.path.join(BASE_DIR, "cotton_wasde_dashboard.html")
DATA_FILE = os.path.join(BASE_DIR, "wasde_full_data.json")

@app.route("/")
def index():
    return send_file(HTML_FILE)

@app.route("/api/data")
def api_data():
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        resp = jsonify(data)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp
    except Exception as e:
        resp = jsonify({"error": str(e)})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp, 500

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
