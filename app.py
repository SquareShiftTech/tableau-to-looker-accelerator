from flask import Flask, jsonify
from flask_cors import CORS
import os
import json
from pathlib import Path
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# Import all analysis functions
from utils.Recomendation import recommendation
from utils.Complexity_Analysis import complexity_analysis
from utils.Inventory_summary import inventory_summary
from utils.DataSource_mapping import datasource_mapping
from utils.Appendix import appendix
from utils.Executive_Summary import executive_summary
from utils.workbook_complexity_breakdown import workbook_complexity_breakdown

app = Flask(__name__)
CORS(app)

@app.route("/api/recommendation_analysis", methods=["GET"])
def recommendation_analysis():
    try:
        df = recommendation()
        return jsonify(df.to_dict(orient="records")), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/complexity_analysis", methods=["GET"])
def run_complexity_analysis():
    try:
        result = complexity_analysis()
        # result is now a dict grouped by Feature Area, return it directly
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/inventory_summary", methods=["GET"])
def run_inventory_summary():
    try:
        df = inventory_summary()
        return jsonify(df.to_dict(orient="records")), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/datasource_mapping", methods=["GET"])
def run_datasource_mapping():
    try:
        df = datasource_mapping()
        return jsonify(df.to_dict(orient="records")), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/appendix", methods=["GET"])
def run_appendix():
    try:
        df = appendix()
        return jsonify(df.to_dict(orient="records")), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/executive_summary", methods=["GET"])
def run_executive_summary():
    try:
        metadata, summary_df = executive_summary()
        result = {
            **metadata,
            "summary": summary_df.to_dict(orient="records")
        }
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/workbook_complexity_breakdown", methods=["GET"])
def run_workbook_complexity_breakdown():
    try:
        result = workbook_complexity_breakdown()
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/metrics", methods=["GET"])
def get_metrics():
    try:
        metrics_dir = Path("output") / "metrics"
        
        # Find any JSON file in the output/metrics folder
        metrics_files = list(metrics_dir.glob("*.json"))
        
        if not metrics_files:
            return jsonify({"message": "no metrics data available"}), 200
        
        # Get the first JSON file found
        metrics_file = metrics_files[0]
        
        # Read and return the JSON content as-is
        with open(metrics_file, 'r', encoding='utf-8') as f:
            metrics_data = json.load(f)
        
        return jsonify(metrics_data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("FLASK_PORT", 5000))
    host = os.getenv("FLASK_HOST", "127.0.0.1")
    app.run(debug=True, host=host, port=port)
