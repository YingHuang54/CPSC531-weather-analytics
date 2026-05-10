from flask import request, Blueprint, jsonify, json
from analytics import create_map
from analytics import create_graph 
from flask_app.data_loader import spark, view_dict
from config import LOCATION_MAP_PATH

api_bp = Blueprint('api', __name__)

# API for location map
@api_bp.route('/location', methods = ["GET"])
def location_map_data():
    """Location Map Json, for weather station information"""
    with open(LOCATION_MAP_PATH, "r") as f:
        return jsonify(json.load(f))

# API for animated map
@api_bp.route('/map', methods = ["POST"])
def scatter_map_data():
    """Visualize the data using plotly scatter map.
    Strictly require request to include a JSON of the parameters:
    level: {"hourly", "daily", "monthly", "yearly"},
    start_date: valid date in ISO format yyyy-mm-dd HH:mm:ss
    end_date: 
    agg:
    metric: 
    """
    print('receive map request')

    # check request containing a json
    try:
        params = request.get_json()
        print("REQUEST HEADER", request.headers)
        print("REQUEST JSON", params)
    except Exception as e:
        print("JSON ERROR", e)
        return jsonify({"error" : "invalid JSON"}), 400

    if not params:
        return jsonify({"error" : "no JSON received (api/map)"}), 400

    # call the corresponding function to draw map for different level 
    # using current Spark Session, Spark SQL View, and received parameters
    result = None
    if params["level"] == 'hourly':
        result = create_map.hourly(spark, 
                                   view_dict[params["level"]], 
                                   view_dict["location"],
                                   params)
    elif params["level"] == 'daily':
        result = create_map.daily(spark, 
                                   view_dict[params["level"]], 
                                   view_dict["location"],
                                   params)
    elif params["level"] == 'monthly':
        result = create_map.monthly(spark, 
                                   view_dict[params["level"]], 
                                   view_dict["location"],
                                   params)
    elif params["level"] == 'yearly':
        result = create_map.yearly(spark, 
                                   view_dict[params["level"]], 
                                   view_dict["location"],
                                   params)
    else: # invalid level
        result = {"error": "level",
                  "message": "Invalid Level, can only be hourly, daily, monthly, or yearly."}
    return result

# API for bar graph
@api_bp.route('/graph', methods = ["POST"])
def graph_data():
    print('receive graph request')
    print("")
    params = request.get_json()
    try:
        params = request.get_json()
        print("REQUEST HEADER", request.headers)
        print("REQUEST JSON", params)
    except Exception as e:
        print("JSON ERROR", e)
        return jsonify({"error" : "invalid JSON"}), 400
    
    if not params:
        return jsonify({"error" : "no JSON received (api/graph)"}), 400

    # call the corresponding function to draw bar graph for different level 
    # using current Spark Session, Spark SQL View, and received parameters.
    if params["level"] == 'hourly':
        result = create_graph.hourly(spark, 
                                   view_dict[params["level"]], 
                                   params)
    elif params["level"] == 'daily':
        result = create_graph.daily(spark, 
                                   view_dict[params["level"]], 
                                   params)
    elif params["level"] == 'monthly':
        result = create_graph.monthly(spark, 
                                   view_dict[params["level"]], 
                                   params)
    elif params["level"] == 'yearly':
        result = create_graph.yearly(spark, 
                                   view_dict[params["level"]], 
                                   params)
    else:
        result = {"error": "level",
                  "message": "Invalid Level, can only be hourly, daily, monthly, or yearly."}
    return result