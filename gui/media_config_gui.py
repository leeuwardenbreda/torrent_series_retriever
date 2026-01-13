from flask import Flask, send_from_directory, jsonify, request
import os
import json

app = Flask(__name__)

CONFIG_FILE = os.getenv("CONFIG_FILE", "/app/media_config.json")
GUI_PORT = int(os.getenv("GUI_PORT", 8083))

# Serve GUI
@app.route("/")
def gui():
    return send_from_directory(os.path.dirname(__file__), "gui.html")

# API: GET config
@app.route("/config", methods=["GET"])
def get_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return jsonify(data)

# API: Add series
@app.route("/config/series", methods=["POST"])
def add_series():
    data = request.json
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg.setdefault("series", []).append(data)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    return jsonify({"status": "ok"})

# API: Delete series
@app.route("/config/series/<int:index>", methods=["DELETE"])
def delete_series(index):
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg["series"].pop(index)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    return jsonify({"status": "ok"})

# API: Add film
@app.route("/config/films", methods=["POST"])
def add_film():
    data = request.json
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg.setdefault("films", []).append(data)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    return jsonify({"status": "ok"})

# API: Delete film
@app.route("/config/films/<int:index>", methods=["DELETE"])
def delete_film(index):
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg["films"].pop(index)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=GUI_PORT, debug=True)
