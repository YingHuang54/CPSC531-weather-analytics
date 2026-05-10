from flask import Flask
from flask_cors import CORS
from flask_app.routes import api_bp
from flask_app.data_loader import load_data
import config

# backend main app for register the api
def create_app():
    app = Flask(__name__)
    CORS(app, resources= {r"/*": {"origins":config.FRONTEND_URL}})

    load_data()
    
    app.register_blueprint(api_bp, url_prefix = '/api')

    return app

app = create_app()
if __name__ == "__main__":
    app.run(port=config.PORT, debug=True)